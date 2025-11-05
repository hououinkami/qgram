import asyncio
import logging
import os
from typing import Dict, Optional

from telethon import events

import config
from service.telethon_client import get_client_instance, create_client, get_client, get_user_id
from utils.telegram_to_qq import revoke_telethon

logger = logging.getLogger(__name__)

# 全局监控器实例
monitor: Optional['TelethonMonitor'] = None

class TelethonMonitor:
    def __init__(self):
        self.is_running = False
        
        # 群组缓存：记录已检查过的群组
        self.chat_cache: Dict[int, bool] = {}
        
        # 从配置获取目标BOT ID
        bot_token = getattr(config, 'BOT_TOKEN', '')
        self.target_bot_id = bot_token.split(':')[0] if ':' in bot_token else None
    
    async def initialize(self):
        """初始化监控器（确保客户端已连接）"""
        client_instance = get_client_instance()
        if not client_instance or not client_instance.is_initialized:
            raise RuntimeError("Telethon客户端未初始化，请先调用create_client")
        
        # 更新全局实例
        global monitor
        monitor = self
    
    async def check_bot_in_chat(self, chat_id: int) -> bool:
        """检查群组是否包含目标BOT"""
        if chat_id in self.chat_cache:
            return self.chat_cache[chat_id]
        
        try:
            client = get_client()
            if not client:
                return False
                
            chat = await client.get_entity(chat_id)
            
            # 跳过私聊
            if not hasattr(chat, 'participants_count'):
                self.chat_cache[chat_id] = False
                return False
            
            # 检查群组成员
            has_bot = await self._check_participants(chat)
            self.chat_cache[chat_id] = has_bot
            return has_bot
                    
        except Exception as e:
            logger.debug(f"❌ 检查群组 {chat_id} 失败: {e}")
            self.chat_cache[chat_id] = False
            return False
    
    async def _check_participants(self, chat) -> bool:
        """检查群组成员中是否有目标BOT"""
        try:
            client = get_client()
            if not client:
                return False
                
            # 分批检查，避免大群组问题
            participants = await client.get_participants(chat, limit=500)
            
            for participant in participants:
                if (participant.bot and self.target_bot_id and 
                    str(participant.id) == self.target_bot_id):
                    logger.debug(f"⚠️ 在群组 {chat.title} 中找到目标BOT")
                    return True
            
            return False
            
        except Exception as e:
            logger.debug(f"❌ 检查群组成员失败: {e}")
            return False
    
    async def process_new_message(self, event):
        """处理新消息事件"""
        try:
            user_id = get_user_id()
            # 只处理当前用户在群组中发送的消息
            if event.sender_id != user_id or not event.is_group:
                return
            
            # 检查群组是否包含目标BOT
            if await self.check_bot_in_chat(event.chat_id):
                message = event.message
                chat = await event.get_chat()
                
                # 调试输出
                logger.info(f"📝 [Telethon] 处理新消息: {event}")
                await process_telethon_update(event)
            
        except Exception as e:
            logger.error(f"❌ 处理Telethon新消息出错: {e}")
    
    async def process_deleted_message(self, event):
        """处理删除消息事件"""
        try:
            logger.debug(f"🗑️ [Telethon] 检测到消息删除事件")
            await revoke_telethon(event)
            
        except Exception as e:
            logger.error(f"❌ 处理删除消息出错: {e}")
    
    async def start_monitoring(self, handle_new_messages: bool = True, handle_deleted_messages: bool = True):
        """开始监控"""
        # 确保监控器已初始化
        if not get_monitor():
            await self.initialize()
        
        client = get_client()
        if not client:
            raise RuntimeError("无法获取Telethon客户端，请确保客户端已初始化")
        
        self.is_running = True
        
        # 注册事件处理器
        if handle_new_messages:
            @client.on(events.NewMessage)
            async def handle_new_message(event):
                await self.process_new_message(event)
            logger.info("📝 已启用Telethon新消息监听")
        
        if handle_deleted_messages:
            @client.on(events.MessageDeleted)
            async def handle_deleted_message(event):
                await self.process_deleted_message(event)
            logger.info("🗑️ 已启用Telethon消息删除监听")
        
        logger.info("✅ Telethon监控已启动")
        
        try:
            # 添加重连机制
            while self.is_running:
                try:
                    await client.run_until_disconnected()
                except Exception as e:
                    if "Invalid HTTP response" in str(e) or "aiohttp" in str(e):
                        logger.warning(f"⚠️ 网络连接异常，5秒后重连: {e}")
                        await asyncio.sleep(5)
                        # 重新连接
                        if not client.is_connected():
                            await client.connect()
                        continue
                    else:
                        raise e
        except KeyboardInterrupt:
            logger.info("⚠️ 收到中断信号")
        except Exception as e:
            logger.error(f"❌ Telethon监控运行出错: {e}")
        finally:
            self.is_running = False
    
    async def stop_monitoring(self):
        """停止监控"""
        self.is_running = False
        client_instance = get_client_instance()
        if client_instance:
            await client_instance.disconnect()
        logger.info("🔴 Telethon监控已停止")
    
    def clear_cache(self):
        """清空缓存"""
        self.chat_cache.clear()
        logger.info("⚠️ 已清空Telethon群组缓存")
    
    def get_client(self):
        """获取Telethon客户端"""
        return get_client()
    
    def get_user_id(self):
        """获取当前用户ID"""
        return get_user_id()

# ==================== 便捷函数 ====================
def get_monitor() -> Optional[TelethonMonitor]:
    """获取监控器实例"""
    global monitor
    return monitor

def is_monitoring() -> bool:
    """检查是否正在监控"""
    global monitor
    return monitor.is_running if monitor else False

async def create_monitor() -> TelethonMonitor:
    """创建监控器实例"""
    global monitor
    monitor = TelethonMonitor()
    await monitor.initialize()
    return monitor

# ==================== 独立运行 ====================
async def main():
    """独立运行Telethon监控"""
    try:
        # 配置参数
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        SESSION_PATH = os.path.join(project_root, 'sessions', 'tg_session')
        
        # 检查session文件
        if not os.path.exists(SESSION_PATH + '.session'):
            logger.error(f"❌ Session文件不存在: {SESSION_PATH}.session")
            return
        
        # 创建Telethon客户端
        await create_client(
            SESSION_PATH, 
            config.API_ID, 
            config.API_HASH, 
            config.DEVICE_MODEL
        )
        
        # 创建监控器
        monitor_instance = await create_monitor()
        
        # Telethon不监控新消息
        handle_new = False
            
        await monitor_instance.start_monitoring(
            handle_new_messages=handle_new,
            handle_deleted_messages=True
        )
        
    except KeyboardInterrupt:
        logger.info("⚠️ 收到中断信号，正在停止Telethon监控...")
        if get_monitor():
            await get_monitor().stop_monitoring()
    except Exception as e:
        logger.error(f"❌ Telethon监控失败: {e}")

if __name__ == "__main__":
    asyncio.run(main())
