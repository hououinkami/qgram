import asyncio
import json
import logging
import time
from typing import Any, Dict, Set

from aiohttp import web

import config
from config import locale
from api.qq_api import qq_api
from api.telegram_sender import telegram_sender
from service.telethon_client import get_user_id
from utils.qq_to_telegram import process_callback_message

logger = logging.getLogger(__name__)

NAPCAT_CALLBACK_PATH = config.NAPCAT_CALLBACK_PATH
NAPCAT_CALLBACK_PORT = config.NAPCAT_CALLBACK_PORT

class MessageDeduplicator:
    """消息去重器"""
    
    def __init__(self, cache_size: int = 1000, ttl: int = 3600):
        """
        初始化去重器
        
        Args:
            cache_size: 内存缓存大小
            ttl: 消息ID过期时间（秒）
        """
        self.processed_messages: Dict[str, float] = {}  # msg_id -> timestamp
        self.cache_size = cache_size
        self.ttl = ttl
        self.last_cleanup = time.time()
    
    def is_duplicate(self, msg_id: str) -> bool:
        """
        检查是否重复消息
        
        Args:
            msg_id: 消息ID
            
        Returns:
            bool: 是否重复
        """
        if not msg_id:
            return False
        
        current_time = time.time()
        
        # 定期清理过期消息
        if current_time - self.last_cleanup > 300:  # 每5分钟清理一次
            self._cleanup_expired(current_time)
            self.last_cleanup = current_time
        
        # 检查是否已处理
        if msg_id in self.processed_messages:
            # 检查是否过期
            if current_time - self.processed_messages[msg_id] < self.ttl:
                return True
            else:
                # 过期了，移除
                del self.processed_messages[msg_id]
        
        return False
    
    def mark_processed(self, msg_id: str):
        """
        标记消息已处理
        
        Args:
            msg_id: 消息ID
        """
        if not msg_id:
            return
        
        current_time = time.time()
        self.processed_messages[msg_id] = current_time
        
        # 如果缓存过大，清理最老的消息
        if len(self.processed_messages) > self.cache_size:
            self._cleanup_oldest()
    
    def _cleanup_expired(self, current_time: float):
        """清理过期消息"""
        expired_keys = [
            msg_id for msg_id, timestamp in self.processed_messages.items()
            if current_time - timestamp >= self.ttl
        ]
        
        for key in expired_keys:
            del self.processed_messages[key]
        
        if expired_keys:
            logger.debug(f"🧹 清理过期消息ID: {len(expired_keys)}个")
    
    def _cleanup_oldest(self):
        """清理最老的消息（当缓存过大时）"""
        if len(self.processed_messages) <= self.cache_size:
            return
        
        # 按时间戳排序，移除最老的消息
        sorted_items = sorted(self.processed_messages.items(), key=lambda x: x[1])
        remove_count = len(self.processed_messages) - self.cache_size + 1000  # 多删除一些
        
        for msg_id, _ in sorted_items[:remove_count]:
            del self.processed_messages[msg_id]
        
        logger.debug(f"🧹 清理最老消息ID: {remove_count}个")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "cached_messages": len(self.processed_messages),
            "cache_size_limit": self.cache_size,
            "ttl_seconds": self.ttl
        }

class ContactMessageProcessor:
    """单个联系人的消息处理器"""
    
    def __init__(self, contact_id: str):
        self.contact_id = contact_id
        self.message_queue = asyncio.Queue()
        self.processing_task = None
        self.is_running = False
        self.last_activity = time.time()  # 记录最后活动时间
        
    async def add_message(self, message_data: dict):
        """添加消息到队列"""
        self.last_activity = time.time()
        await self.message_queue.put(message_data)
    
    async def start(self):
        """启动消息处理器"""
        if not self.is_running:
            self.is_running = True
            self.processing_task = asyncio.create_task(self._process_messages())
            logger.debug(f"🚀 启动联系人 {self.contact_id} 的消息处理器")
    
    async def stop(self):
        """停止消息处理器"""
        self.is_running = False
        
        if self.processing_task and not self.processing_task.done():
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
        
        # 清空剩余消息
        while not self.message_queue.empty():
            try:
                self.message_queue.get_nowait()
                self.message_queue.task_done()
            except asyncio.QueueEmpty:
                break
        
        logger.debug(f"🔴 停止联系人 {self.contact_id} 的消息处理器")
    
    async def _process_messages(self):
        """处理消息的主循环"""
        while self.is_running:
            try:
                # 等待消息，设置超时以便能够响应停止信号
                message_data = await asyncio.wait_for(
                    self.message_queue.get(), 
                    timeout=1.0
                )
                
                # 更新活动时间
                self.last_activity = time.time()
                
                # 处理消息
                try:
                    await process_callback_message(message_data)
                    logger.debug(f"✅ 成功处理联系人 {self.contact_id} 的消息")
                except Exception as e:
                    logger.error(f"❌ 处理联系人 {self.contact_id} 消息失败: {e}")
                
                # 标记任务完成
                self.message_queue.task_done()
                
            except asyncio.TimeoutError:
                # 超时是正常的，继续循环
                continue
            except Exception as e:
                logger.error(f"❌ 联系人 {self.contact_id} 消息处理器出错: {e}")
                await asyncio.sleep(0.1)  # 短暂休息避免快速循环

# 全局去重器和处理器管理
deduplicator = MessageDeduplicator(cache_size=1000, ttl=3600)  # 1小时过期
contact_processors: Dict[str, ContactMessageProcessor] = {}
processor_lock = asyncio.Lock()

# 统计信息
stats = {
  "total_messages": 0,
  "duplicate_messages": 0,
  "processed_messages": 0,
  "failed_messages": 0
}

async def get_or_create_processor(contact_id: str) -> ContactMessageProcessor:
    """获取或创建联系人处理器"""
    async with processor_lock:
        if contact_id not in contact_processors:
            processor = ContactMessageProcessor(contact_id)
            await processor.start()
            contact_processors[contact_id] = processor
            logger.debug(f"📝 为联系人 {contact_id} 创建新的处理器")
        return contact_processors[contact_id]

async def cleanup_idle_processors():
    """清理空闲的处理器"""
    while True:
        try:
            await asyncio.sleep(300)  # 每5分钟检查一次
            
            async with processor_lock:
                current_time = time.time()
                idle_contacts = []
                
                for contact_id, processor in contact_processors.items():
                    # 检查队列是否为空且最后活动时间超过10分钟
                    if (processor.message_queue.empty() and 
                        current_time - processor.last_activity > 600):  # 10分钟无活动
                        idle_contacts.append(contact_id)
                
                # 只清理长时间无活动的处理器，保留活跃的
                for contact_id in idle_contacts[:10]:  # 限制每次最多清理10个
                    processor = contact_processors.pop(contact_id)
                    await processor.stop()
                    logger.debug(f"🧹 清理空闲处理器: {contact_id}")
                    
        except Exception as e:
            logger.error(f"❌ 清理处理器时出错: {e}")

# 登陆检测
login_status = None

async def login_check():
    """异步登录检测"""
    global login_status
    
    status_response = await qq_api("GET_STATUS", {})
    status_data = status_response.get('data', {})
    status = status_data.get('online') and status_data.get('good')
    
    tg_user_id = get_user_id()
    if not status:
        # 只有当上一次状态不是离线时才发送离线提示
        if login_status != "offline":
            await telegram_sender.send_text(tg_user_id, locale.common('offline'))
            login_status = "offline"
        return {"success": True, "message": "用户可能退出"}
    
    else:
        # 当前不是离线状态
        # 如果上一次是离线状态，发送上线提示
        if login_status == "offline":
            await telegram_sender.send_text(tg_user_id, locale.common('online'))
        login_status = "online"
        return {"success": True, "message": "正常状态"}

async def process_callback_data(callback_data: Dict[str, Any]) -> Dict[str, Any]:
    """异步处理回调数据"""
    try:
        # 检查是否在线
        # await login_check(callback_data)
               
        # 处理每条消息 - 改进去重逻辑
        processed_count = 0
        failed_count = 0
        duplicate_count = 0

        msg_id = callback_data.get('message_id')
        from_id = callback_data.get('group_id') or callback_data.get('target_id') or callback_data.get('user_id')
        post_type = callback_data.get('post_type', 'unknown')
        
        if not msg_id or not from_id:
            return
        
        stats["total_messages"] += 1
        
        # 使用复合键进行去重，包含消息ID
        msg_key = f"{msg_id}"
        
        # 先检查去重，立即标记为处理中
        if post_type == "message" and deduplicator.is_duplicate(msg_key):
            duplicate_count += 1
            stats["duplicate_messages"] += 1
            logger.warning(f"🔄 跳过重复消息: {msg_id} (来自: {from_id})")
            return

        try:
            # 立即标记为已处理，防止竞态条件
            deduplicator.mark_processed(msg_key)
            
            # 获取或创建该联系人的处理器
            processor = await get_or_create_processor(from_id)
            # 只传递单个消息数据
            await processor.add_message(callback_data)
            
            stats["processed_messages"] += 1
            processed_count += 1
                
        except Exception as e:
            failed_count += 1
            stats["failed_messages"] += 1
            logger.error(f"❌ 分发消息 {msg_id} 到联系人 {from_id} 失败: {e}")
            
            # 处理失败时，从去重缓存中移除，允许重试
            try:
                # 从已处理消息中移除，允许后续重试
                if msg_key in deduplicator.processed_messages:
                    del deduplicator.processed_messages[msg_key]
            except Exception as cleanup_error:
                logger.error(f"清理失败消息缓存时出错: {cleanup_error}")
        
        # 记录处理结果
        if duplicate_count > 0:
            logger.info(f"📊 消息处理完成 - 处理: {processed_count}, 失败: {failed_count}, 重复: {duplicate_count}")
        elif processed_count > 0 or failed_count > 0:
            logger.debug(f"📊 消息处理完成 - 处理: {processed_count}, 失败: {failed_count}")
        
        return {
            "success": True,
            "message": f"处理 {processed_count} 条新消息，跳过 {duplicate_count} 条重复消息，失败 {failed_count} 条"
        }
        
    except Exception as e:
        logger.error(f"❌ 处理回调数据失败: {e}")
        stats["failed_messages"] += 1
        return {"success": False, "message": str(e)}

async def handle_message(request):
    """处理微信消息的异步处理器"""
    try:
        # 检查请求体大小
        if request.content_length and request.content_length > 5 * 1024 * 1024:
            return web.json_response(
                {"success": False, "message": "请求体过大"}, 
                status=400
            )
        # 读取请求体
        try:
            callback_data = await request.json()

            # 记录接收到的事件类型
            post_type = callback_data.get('post_type', 'unknown')
            logger.info(f"收到事件: {post_type}")

        except json.JSONDecodeError:
            return web.json_response(
                {"success": False, "message": "JSON格式错误"}, 
                status=400
            )
        
        # 立即响应，避免重试
        response = web.json_response({"success": True, "message": "已接收"})
        
        # 异步处理消息（不等待结果）
        asyncio.create_task(async_process_message(callback_data))
        
        return response
        
    except Exception as e:
        logger.error(f"❌ 请求处理失败: {e}")
        return web.json_response(
            {"success": False, "message": "服务器错误"}, 
            status=500
        )

async def async_process_message(callback_data: Dict[str, Any]):
    """异步处理消息任务"""
    try:
        result = await process_callback_data(callback_data)
        if not result.get("success"):
            logger.error(f"❌ 异步处理失败: {result}")
    except Exception as e:
        logger.error(f"❌ 异步处理出错: {e}")

async def handle_options(request):
    """处理OPTIONS请求"""
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }
    return web.Response(headers=headers)

@web.middleware
async def cors_middleware(request, handler):
    """CORS 中间件"""
    try:
        response = await handler(request)
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return response
    except Exception as e:
        logger.error(f"❌ 中间件处理错误: {e}")
        return web.json_response(
            {"success": False, "message": "中间件错误"}, 
            status=500
        )

async def create_app():
    """创建aiohttp应用"""
    app = web.Application(middlewares=[cors_middleware])
    
    # 添加路由 - 移除路径检查，因为路由已经处理了
    app.router.add_post(NAPCAT_CALLBACK_PATH, handle_message)
    app.router.add_options(NAPCAT_CALLBACK_PATH, handle_options)
    
    # 添加健康检查路由
    async def health_check(request):
        return web.json_response({"status": "healthy", "service": "qgram"})
    
    app.router.add_get("/health", health_check)
    
    return app

async def run_server():
    """启动异步服务器"""
    try:
        # 启动清理任务
        cleanup_task = asyncio.create_task(cleanup_idle_processors())
        
        app = await create_app()
        runner = web.AppRunner(app)
        await runner.setup()
        
        site = web.TCPSite(runner, '0.0.0.0', NAPCAT_CALLBACK_PORT)
        await site.start()
        
        logger.info(f"✅ NapCat消息服务启动, 端口: {NAPCAT_CALLBACK_PORT}, 路径: {NAPCAT_CALLBACK_PATH}")
        
        # 保持服务运行
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("⚠️ 服务正在关闭...")
        finally:
            # 停止清理任务
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass
            
            # 停止所有联系人处理器
            async with processor_lock:
                for processor in contact_processors.values():
                    await processor.stop()
                contact_processors.clear()
            
            await runner.cleanup()
            
    except OSError as e:
        if e.errno == 48:
            logger.error(f"⚠️ 端口 {NAPCAT_CALLBACK_PORT} 已被占用")
        else:
            logger.error(f"❌ 网络错误: {e}")
    except Exception as e:
        logger.error(f"❌ 服务器错误: {e}")

async def main():
    """异步主函数"""    
    # 检查配置
    if not NAPCAT_CALLBACK_PATH or not NAPCAT_CALLBACK_PORT:
        logger.error("❌ NAPCAT_CALLBACK 配置不能为空")
        return
    
    # 启动异步服务器
    await run_server()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⚠️ 收到中断信号，正在关闭服务...")
    except Exception as e:
        logger.error(f"❌ 启动失败: {e}")
