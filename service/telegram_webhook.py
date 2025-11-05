import asyncio
import logging
from typing import Callable, Dict
from collections import defaultdict, deque
import json
import ssl
from aiohttp import web, ClientSession
from aiohttp.web import Request, Response

from telegram import Update, Bot
from telegram.ext import Application, CallbackContext, CallbackQueryHandler, CommandHandler, MessageHandler, filters
from telegram.request import HTTPXRequest
from telegram.error import NetworkError, TimedOut, TelegramError

import config
# from utils.telegram_callbacks import BotCallbacks
# from utils.telegram_commands import BotCommands
from utils.telegram_to_qq import process_telegram_update

# 屏蔽指定日志输出
logging.getLogger('telegram.ext.updater').disabled = True
logging.getLogger('httpx').disabled = True
logging.getLogger('httpcore').disabled = True
logging.getLogger('aiohttp.access').disabled = True

logger = logging.getLogger(__name__)

class ChatUpdateQueue:
    """单个聊天的更新队列管理器"""
    
    def __init__(self, chat_id: int, process_function: Callable):
        self.chat_id = chat_id
        self.process_function = process_function
        self.queue = deque()
        self.is_processing = False
        self.worker_task = None
        
    async def add_update(self, update: Update):
        """添加更新到队列"""
        self.queue.append(update)
        
        # 如果没有在处理，启动处理任务
        if not self.is_processing:
            self.worker_task = asyncio.create_task(self._process_queue())
    
    async def _process_queue(self):
        """处理队列中的更新"""
        self.is_processing = True
        
        try:
            while self.queue:
                update = self.queue.popleft()
                try:
                    await self.process_function(update)
                except Exception as e:
                    logger.error(f"❌ 处理 chat_id {self.chat_id} 的 update 时发生错误: {e}")
                
                # 短暂延迟，避免过于频繁的处理
                await asyncio.sleep(0.01)
                
        finally:
            self.is_processing = False
            self.worker_task = None
    
    async def stop(self):
        """停止队列处理"""
        if self.worker_task and not self.worker_task.done():
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass

class UpdateQueueManager:
    """更新队列管理器 - 按 chat_id 分组处理"""
    
    def __init__(self, process_function: Callable):
        self.process_function = process_function
        self.chat_queues: Dict[int, ChatUpdateQueue] = {}
        self.cleanup_task = None
        
    async def add_update(self, update: Update):
        """添加更新到对应的聊天队列"""
        chat_id = self._get_chat_id(update)
        
        if chat_id not in self.chat_queues:
            self.chat_queues[chat_id] = ChatUpdateQueue(chat_id, self.process_function)
        
        await self.chat_queues[chat_id].add_update(update)
    
    def _get_chat_id(self, update: Update) -> int:
        """从 update 中提取 chat_id"""
        if update.message:
            return update.message.chat_id
        elif update.callback_query:
            return update.callback_query.message.chat_id
        elif update.edited_message:
            return update.edited_message.chat_id
        elif update.channel_post:
            return update.channel_post.chat_id
        elif update.edited_channel_post:
            return update.edited_channel_post.chat_id
        else:
            # 如果无法确定 chat_id，使用一个默认值
            return 0
    
    async def start_cleanup_task(self):
        """启动清理任务，定期清理空闲的队列"""
        self.cleanup_task = asyncio.create_task(self._cleanup_idle_queues())
    
    async def _cleanup_idle_queues(self):
        """清理空闲的队列（每5分钟检查一次）"""
        while True:
            try:
                await asyncio.sleep(300)  # 5分钟
                
                idle_chat_ids = []
                for chat_id, queue in self.chat_queues.items():
                    if not queue.is_processing and len(queue.queue) == 0:
                        idle_chat_ids.append(chat_id)
                
                # 清理空闲队列
                for chat_id in idle_chat_ids:
                    await self.chat_queues[chat_id].stop()
                    del self.chat_queues[chat_id]
                
                if idle_chat_ids:
                    logger.debug(f"🧹 清理了 {len(idle_chat_ids)} 个空闲聊天队列")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ 清理队列时发生错误: {e}")
    
    async def stop_all(self):
        """停止所有队列"""
        # 停止清理任务
        if self.cleanup_task and not self.cleanup_task.done():
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        
        # 停止所有聊天队列
        stop_tasks = []
        for queue in self.chat_queues.values():
            stop_tasks.append(queue.stop())
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        self.chat_queues.clear()
        logger.info("🔴 所有更新队列已停止")

class TelegramWebhookService:
    """Telegram Webhook 服务类"""
    
    def __init__(self, bot_token: str, process_function: Callable,
                webhook_url: str, webhook_port: int = 8443, webhook_path: str = "/qq/webhook",
                commands: list = None, command_handlers: dict = None, callback_handlers: dict = None,
                ssl_cert: str = None, ssl_key: str = None):
        """
        初始化 Webhook 服务
        
        Args:
            bot_token (str): Telegram Bot Token
            process_function (Callable): 处理 update 的外部函数
            webhook_url (str): Webhook URL (例如: https://home.yourdomain.com:8443/qq/webhook)
            webhook_port (int): Webhook 监听端口
            webhook_path (str): Webhook 路径
            commands (list): Bot命令列表
            command_handlers (dict): 命令处理器字典
            callback_handlers (dict): 回调处理器字典
            ssl_cert (str): SSL证书文件路径 (可选，用于HTTPS)
            ssl_key (str): SSL私钥文件路径 (可选，用于HTTPS)
        """
        self.bot_token = bot_token
        self.process_function = process_function
        self.webhook_url = webhook_url
        self.webhook_port = webhook_port
        self.webhook_path = webhook_path
        self.commands = commands or []
        self.command_handlers = command_handlers or {}
        self.callback_handlers = callback_handlers or {}
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        
        self.application = None
        self.web_app = None
        self.runner = None
        self.site = None
        self.is_running = False
        self.queue_manager = UpdateQueueManager(process_function)
        
        # 创建处理器实例用于手动调用
        self._command_handlers = {}
        self._callback_handlers = {}
        
    def create_application(self):
        """创建Application实例"""
        request = HTTPXRequest(
            connection_pool_size=10,
            read_timeout=30,
            write_timeout=30,
            connect_timeout=30,
            pool_timeout=30
        )
        
        return Application.builder().token(self.bot_token).request(request).build()
    
    def setup_handlers(self):
        """设置处理器实例（用于手动调用）"""
        # 创建命令处理器实例
        for command, handler_func in self.command_handlers.items():
            self._command_handlers[command] = CommandHandler(command, handler_func)
        
        # 创建回调查询处理器实例
        for pattern, handler_func in self.callback_handlers.items():
            self._callback_handlers[pattern] = CallbackQueryHandler(handler_func, pattern=pattern)
    
    async def webhook_handler(self, request: Request) -> Response:
        """处理 Webhook 请求"""
        try:
            # 验证请求方法
            if request.method != 'POST':
                return Response(status=405, text="Method Not Allowed")
            
            # 读取请求体
            body = await request.read()
            if not body:
                return Response(status=400, text="Empty request body")
            
            # 解析 JSON
            try:
                update_dict = json.loads(body.decode('utf-8'))
            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON 解析错误: {e}")
                return Response(status=400, text="Invalid JSON")
            
            # 创建 Update 对象
            try:
                update = Update.de_json(update_dict, self.application.bot)
                if not update:
                    return Response(status=400, text="Invalid update")
            except Exception as e:
                logger.error(f"❌ Update 对象创建失败: {e}")
                return Response(status=400, text="Invalid update format")
            
            # 处理 Update
            await self.handle_update(update)
            
            return Response(status=200, text="OK")
            
        except Exception as e:
            logger.error(f"❌ Webhook 处理错误: {e}")
            return Response(status=500, text="Internal Server Error")
    
    async def health_check_handler(self, request: Request) -> Response:
        """健康检查端点"""
        return Response(status=200, text="OK", content_type="text/plain")
    
    async def handle_update(self, update: Update):
        """处理接收到的 update - 使用 Handler 模块处理"""
        try:
            context = CallbackContext(self.application)
            
            # 处理命令 - 让 CommandHandler 自己判断（支持 /command@bot_name 格式）
            if update.message and update.message.text and update.message.text.startswith('/'):
                # 遍历所有命令处理器，让它们自己判断是否匹配
                for command, command_handler in self._command_handlers.items():
                    check_result = command_handler.check_update(update)
                    if check_result:
                        await command_handler.handle_update(update, self.application, check_result, context)
                        return
            
            # 处理回调查询 - 使用 CallbackQueryHandler 检查
            if update.callback_query:
                for pattern, callback_handler in self._callback_handlers.items():
                    # 检查回调查询是否匹配
                    check_result = callback_handler.check_update(update)
                    if check_result:
                        await callback_handler.handle_update(update, self.application, check_result, context)
                        return
            
            # 其他消息通过队列处理
            await self.queue_manager.add_update(update)
            
        except Exception as e:
            logger.error(f"❌ 处理 update 时发生错误: {e}")
    
    def create_web_app(self):
        """创建 aiohttp web 应用"""
        app = web.Application()
        
        # 添加路由
        app.router.add_post(self.webhook_path, self.webhook_handler)
        app.router.add_get('/health', self.health_check_handler)
        
        return app
    
    async def set_webhook(self):
        """设置 Telegram Webhook"""
        try:
            # 如果有SSL证书，可以上传给Telegram
            certificate = None
            if self.ssl_cert:
                with open(self.ssl_cert, 'rb') as cert_file:
                    certificate = cert_file.read()
            
            success = await self.application.bot.set_webhook(
                url=self.webhook_url,
                certificate=certificate,
                drop_pending_updates=False,
                max_connections=100,
                allowed_updates=None  # 接收所有类型的更新
            )
            
            if success:
                logger.info(f"✅ Webhook 设置成功: {self.webhook_url}")
            else:
                logger.error("❌ Webhook 设置失败")
                return False
                
        except Exception as e:
            logger.error(f"❌ 设置 Webhook 时发生错误: {e}")
            return False
        
        return True
    
    async def delete_webhook(self):
        """删除 Telegram Webhook"""
        try:
            await self.application.bot.delete_webhook(drop_pending_updates=False)
            logger.info("🗑️ Webhook 已删除")
        except Exception as e:
            logger.error(f"❌ 删除 Webhook 时发生错误: {e}")
    
    async def setup_commands(self):
        """设置机器人命令菜单"""
        if not self.commands:
            return
            
        try:
            await self.application.bot.set_my_commands(self.commands)
            logger.info(f"🤖 设置了 {len(self.commands)} 个机器人命令")
        except Exception as e:
            logger.warning(f"❌ 设置机器人命令失败: {e}")
    
    async def start_webhook(self):
        """启动 Webhook 服务"""
        try:
            self.is_running = True
            
            # 创建并初始化应用
            self.application = self.create_application()
            await self.application.initialize()
            await self.application.start()
            
            # 设置处理器
            self.setup_handlers()
            
            # 设置机器人命令
            await self.setup_commands()
            
            # 启动队列管理器的清理任务
            await self.queue_manager.start_cleanup_task()
            
            # 创建 web 应用
            self.web_app = self.create_web_app()
            
            # 创建并启动 web 服务器
            self.runner = web.AppRunner(self.web_app)
            await self.runner.setup()
            
            # 配置 SSL (如果提供了证书)
            ssl_context = None
            if self.ssl_cert and self.ssl_key:
                ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                ssl_context.load_cert_chain(self.ssl_cert, self.ssl_key)
                logger.info("🔒 启用 HTTPS")
            
            self.site = web.TCPSite(
                self.runner, 
                host='0.0.0.0', 
                port=self.webhook_port,
                ssl_context=ssl_context
            )
            await self.site.start()
            
            logger.info(f"🌐 Web 服务器已启动，监听端口: {self.webhook_port}")
            
            # 设置 Webhook
            if not await self.set_webhook():
                raise Exception("Webhook 设置失败")
            
            logger.info("✅ Telegram Webhook 服务已启动")
            
            # 保持运行状态
            while self.is_running:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("⚠️ Webhook 服务被取消")
            raise
        except Exception as e:
            logger.error(f"❌ Webhook 服务异常: {e}")
            raise
        finally:
            await self.stop_webhook()
    
    async def stop_webhook(self):
        """停止 Webhook 服务"""
        logger.info("⚠️ 正在停止 Webhook 服务...")
        self.is_running = False
        
        try:
            # 删除 Webhook
            if self.application:
                await self.delete_webhook()
            
            # 停止队列管理器
            await self.queue_manager.stop_all()
            
            # 停止 web 服务器
            if self.site:
                await self.site.stop()
                self.site = None
            
            if self.runner:
                await self.runner.cleanup()
                self.runner = None
            
            # 停止并清理应用
            if self.application:
                await self.application.stop()
                await self.application.shutdown()
                self.application = None
            
        except Exception as e:
            logger.error(f"❌ 停止 Webhook 服务时发生错误: {e}")
        finally:
            logger.info("🔴 Webhook 服务已停止")

# 全局服务实例
webhook_service = None

async def main():
    """异步主函数"""
    global webhook_service
    
    try:
        # 配置 Webhook URL - 443端口不需要显示端口号
        if not config.WEBHOOK_DOMAIN:
            logger.warning('⚠️ 请先在环境变量中配置Webhook域名')

        if config.WEBHOOK_PORT == 443:
            webhook_url = f"https://{config.WEBHOOK_DOMAIN}/qq/webhook"
        else:
            webhook_url = f"https://{config.WEBHOOK_DOMAIN}:{config.WEBHOOK_PORT}/qq/webhook"
        
        cert_name = config.SSL_CERT_NAME
        key_name = config.SSL_KEY_NAME
        ssl_cert_path = f"/app/ssl/{cert_name}"
        ssl_key_path = f"/app/ssl/{key_name}"

        webhook_service = TelegramWebhookService(
            bot_token=config.BOT_TOKEN,
            process_function=process_telegram_update,
            webhook_url=webhook_url,
            webhook_port=8443,
            webhook_path="/qq/webhook",
            # commands=BotCommands.get_command_config(),
            # command_handlers=BotCommands.get_command_handlers(),
            # callback_handlers=BotCallbacks.get_callback_handlers(),
            ssl_cert=ssl_cert_path,
            ssl_key=ssl_key_path 
        )
        
        await webhook_service.start_webhook()
        
    except KeyboardInterrupt:
        logger.info("接收到中断信号")
    except Exception as e:
        logger.error(f"主函数发生错误: {e}")
        raise

async def shutdown():
    """关闭服务"""
    global webhook_service
    if webhook_service:
        await webhook_service.stop_webhook()

if __name__ == "__main__":   
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
