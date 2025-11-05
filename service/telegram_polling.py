import asyncio
import logging
from typing import Callable, Dict
from collections import defaultdict, deque

from telegram import BotCommand, Update
from telegram.ext import Application, CallbackContext, CallbackQueryHandler, CommandHandler, MessageHandler, filters
from telegram.request import HTTPXRequest
from telegram.error import NetworkError, TimedOut, TelegramError

import config
from utils.telegram_callbacks import BotCallbacks
from utils.telegram_commands import BotCommands
from utils.telegram_to_qq import process_telegram_update

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
                    logger.exception(f"❌ 处理 chat_id {self.chat_id} 的 update 时发生错误: {e}")
                
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

class TelegramPollingService:
    """Telegram 轮询服务类"""
    
    def __init__(self, bot_token: str, process_function: Callable, 
                 commands: list = None, command_handlers: dict = None, callback_handlers: dict = None):
        """
        初始化轮询服务
        
        Args:
            bot_token (str): Telegram Bot Token
            process_function (Callable): 处理 update 的外部函数
            commands (list): Bot命令列表
            command_handlers (dict): 命令处理器字典
            callback_handlers (dict): 回调处理器字典
        """
        self.bot_token = bot_token
        self.process_function = process_function
        self.commands = commands or []
        self.command_handlers = command_handlers or {}
        self.callback_handlers = callback_handlers or {}
        self.application = None
        self.is_running = False
        self.queue_manager = UpdateQueueManager(process_function)
        
        # 创建处理器实例用于手动调用（与webhook模块保持一致）
        self._command_handlers = {}
        self._callback_handlers = {}
        
    def create_application(self):
        """创建Application实例，优化网络配置"""
        # 配置HTTP请求参数，主要是增加连接池大小和超时时间
        request = HTTPXRequest(
            connection_pool_size=10,  # 增加连接池大小
            read_timeout=30,          # 读取超时
            write_timeout=30,         # 写入超时
            connect_timeout=30,       # 连接超时
            pool_timeout=30           # 连接池超时
        )
        
        return Application.builder().token(self.bot_token).request(request).build()
    
    def setup_handlers(self):
        """设置处理器实例（用于手动调用，与webhook模块保持一致）"""
        # 创建命令处理器实例
        for command, handler_func in self.command_handlers.items():
            self._command_handlers[command] = CommandHandler(command, handler_func)
        
        # 创建回调查询处理器实例
        for pattern, handler_func in self.callback_handlers.items():
            self._callback_handlers[pattern] = CallbackQueryHandler(handler_func, pattern=pattern)
        
        # 注册到Application（轮询模式需要这样做）
        for command_handler in self._command_handlers.values():
            self.application.add_handler(command_handler)
        
        for callback_handler in self._callback_handlers.values():
            self.application.add_handler(callback_handler)

        # 处理所有其他非命令消息
        self.application.add_handler(
            MessageHandler(filters.ALL & ~filters.COMMAND, self.handle_update)
        )
        
        # 添加错误处理器
        self.application.add_error_handler(self.error_handler)
        
    async def handle_update(self, update: Update, context: CallbackContext):
        """处理接收到的 update - 现在通过队列管理器处理"""
        try:
            # 将 update 添加到对应的聊天队列中
            await self.queue_manager.add_update(update)
        except Exception as e:
            logger.error(f"❌ 添加 update 到队列时发生错误: {e}")
    
    async def error_handler(self, update: Update, context: CallbackContext):
        """错误处理器 - 只记录日志，让轮询机制自然处理"""
        error = context.error
        
        if isinstance(error, (NetworkError, TimedOut)):
            # 网络错误很常见，降低日志级别
            logger.debug(f"❌ 网络错误: {error}")
        elif isinstance(error, TelegramError):
            logger.error(f"❌ Telegram API 错误: {error}")
        else:
            logger.error(f"❌ 未知错误: {error}")
    
    async def setup_commands(self):
        """设置机器人命令菜单"""
        if not self.commands:
            return
            
        try:
            # 将命令列表转换为 BotCommand 对象列表
            bot_commands = []
            for command_info in self.commands:
                if isinstance(command_info, list) and len(command_info) >= 2:
                    # 如果是 [command, description] 格式
                    command = command_info[0]
                    description = command_info[1]
                    # 确保描述不为空且长度合适
                    if description and len(description.strip()) > 0:
                        # Telegram 命令描述最大长度是 256 字符
                        if len(description) > 256:
                            description = description[:253] + "..."
                        bot_commands.append(BotCommand(command, description))
                    else:
                        # 如果描述为空，提供默认描述
                        bot_commands.append(BotCommand(command, f"Execute {command} command"))
                elif isinstance(command_info, BotCommand):
                    # 如果已经是 BotCommand 对象
                    bot_commands.append(command_info)
                else:
                    logger.warning(f"⚠️ 跳过无效的命令配置: {command_info}")
            
            if bot_commands:
                await self.application.bot.set_my_commands(bot_commands)
                logger.info(f"🤖 设置了 {len(bot_commands)} 个机器人命令")
            else:
                logger.warning("⚠️ 没有有效的命令可以设置")
                
        except Exception as e:
            # 设置命令失败不影响主要功能
            logger.warning(f"❌ 设置机器人命令失败: {e}")
            # 打印详细的命令信息用于调试
            logger.debug(f"原始命令配置: {self.commands}")

    async def start_polling(self):
        """启动轮询服务"""
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
            
            # 启动轮询 - 让 python-telegram-bot 自己处理重试
            await self.application.updater.start_polling(
                poll_interval=1.0,  # 轮询间隔
                timeout=20, # 长轮询超时
                bootstrap_retries=-1,   # 启动重试（-1=无限重试）
                drop_pending_updates=False # 保留待处理的消息
            )
            
            logger.info("✅ Telegram 轮询服务已启动")
            
            # 保持运行状态
            while self.is_running:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("⚠️ 轮询服务被取消")
            raise
        except Exception as e:
            logger.error(f"❌ 轮询服务异常: {e}")
            raise
        finally:
            await self.stop_polling()
    
    async def stop_polling(self):
        """停止轮询服务"""
        if not self.application:
            return
            
        logger.info("⚠️ 正在停止轮询服务...")
        self.is_running = False
        
        try:
            # 停止队列管理器
            await self.queue_manager.stop_all()
            
            # 停止轮询器
            if hasattr(self.application, 'updater') and self.application.updater.running:
                await self.application.updater.stop()
            
            # 停止并清理应用
            await self.application.stop()
            await self.application.shutdown()
            
        except Exception as e:
            logger.error(f"❌ 停止轮询服务时发生错误: {e}")
        finally:
            self.application = None
            logger.info("🔴 轮询服务已停止")

# 全局服务实例
polling_service = None

async def main():
    """异步主函数"""
    global polling_service
    
    try:
        polling_service = TelegramPollingService(
            bot_token=config.BOT_TOKEN,
            process_function=process_telegram_update,
            commands=BotCommands.get_command_config(),
            command_handlers=BotCommands.get_command_handlers(),
            callback_handlers=BotCallbacks.get_callback_handlers()
        )
        
        await polling_service.start_polling()
        
    except KeyboardInterrupt:
        logger.info("接收到中断信号")
    except Exception as e:
        logger.error(f"主函数发生错误: {e}")
        raise

async def shutdown():
    """关闭服务"""
    global polling_service
    if polling_service:
        await polling_service.stop_polling()

if __name__ == "__main__":   
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序异常退出: {e}")