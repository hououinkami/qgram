import asyncio
import logging
import concurrent.futures
from typing import Dict, Optional
from contextlib import asynccontextmanager

import aiohttp
from telethon.tl.functions.messages import CreateChatRequest, EditChatAdminRequest, EditChatPhotoRequest, GetDialogFiltersRequest, UpdateDialogFilterRequest
from telethon.tl.types import InputChatUploadedPhoto, InputPeerChat, InputPeerChannel, DialogFilter, TextWithEntities

import config
from service.telethon_client import get_client, get_client_instance
from utils import tools

logger = logging.getLogger(__name__)

class GroupManager:
    """基于跨线程通信的群组管理器"""
    
    def __init__(self):
        self._session = None
        self._session_lock = asyncio.Lock()
        self._contact_manager = None

    # 延迟导入
    @property
    def contact_manager(self):
        if self._contact_manager is None:
            from utils.contact_manager import contact_manager
            self._contact_manager = contact_manager
        return self._contact_manager

    @asynccontextmanager
    async def _get_session(self):
        """安全获取 aiohttp 会话的上下文管理器"""
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=30)
                )
            
            try:
                yield self._session
            finally:
                pass

    async def cleanup(self):
        """清理资源"""
        async with self._session_lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

    def _get_telethon_client(self):
        """获取跨线程安全的 Telethon 客户端"""
        try:
            client = get_client()
            if not client:
                raise Exception("无法获取 Telethon 客户端")
            
            # 检查客户端是否可用
            client_instance = get_client_instance()
            if not client_instance or not client_instance.is_initialized:
                raise Exception("客户端未初始化")
            
            return client
            
        except Exception as e:
            logger.error(f"获取 Telethon 客户端失败: {e}")
            raise

    async def _get_bot_entity(self, client):
        """获取机器人实体"""
        try:
            # 方法1: 从BOT_TOKEN解析机器人ID
            if hasattr(config, 'BOT_TOKEN') and config.BOT_TOKEN:
                try:
                    bot_id = config.BOT_TOKEN.split(':')[0]
                    bot_entity = await client.get_entity(int(bot_id))
                    return bot_entity
                except Exception as e:
                    logger.warning(f"通过BOT_TOKEN获取机器人实体失败: {e}")
            
            # 方法2: 从BOT_USERNAME获取
            if hasattr(config, 'BOT_USERNAME') and config.BOT_USERNAME:
                try:
                    bot_entity = await client.get_entity(config.BOT_USERNAME)
                    logger.info(f"通过BOT_USERNAME获取机器人实体成功: {config.BOT_USERNAME}")
                    return bot_entity
                except Exception as e:
                    logger.warning(f"通过BOT_USERNAME获取机器人实体失败: {e}")
            
            # 方法3: 通过API获取机器人信息然后用username获取
            if hasattr(config, 'BOT_TOKEN') and config.BOT_TOKEN:
                try:
                    bot_info = await self._get_bot_info_from_api(config.BOT_TOKEN)
                    if bot_info and 'username' in bot_info:
                        bot_username = bot_info['username']
                        bot_entity = await client.get_entity(bot_username)
                        logger.info(f"通过API+username获取机器人实体成功: {bot_username}")
                        return bot_entity
                except Exception as e:
                    logger.warning(f"通过API+username获取机器人实体失败: {e}")
            
            raise Exception("所有获取机器人实体的方法都失败了")
                
        except Exception as e:
            logger.error(f"获取机器人实体失败: {e}")
            return None

    async def _get_bot_info_from_api(self, bot_token: str) -> Optional[Dict]:
        """通过 Telegram Bot API 获取机器人信息"""
        try:
            url = f"https://api.telegram.org/bot{bot_token}/getMe"
            
            async with self._get_session() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('ok'):
                            return data['result']
                        else:
                            logger.error(f"Bot API返回错误: {data}")
                            return None
                    else:
                        logger.error(f"Bot API请求失败: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"通过API获取机器人信息失败: {e}")
            return None

    async def _set_group_avatar(self, client, chat_id: int, avatar_url: str) -> bool:
        """设置群组头像"""
        if not avatar_url:
            return True
        
        try:
            processed_image_data = await tools.process_avatar_from_url(avatar_url)
            
            if not processed_image_data:
                logger.error("下载或处理头像图片失败")
                return False
            
            if chat_id < 0:
                original_chat_id = abs(chat_id)
                processed_image_data.seek(0)
                
                uploaded_photo = await client.upload_file(
                    processed_image_data,
                    file_name="avatar.jpg"
                )
                
                await client(EditChatPhotoRequest(
                    chat_id=original_chat_id,
                    photo=InputChatUploadedPhoto(uploaded_photo)
                ))
            
            return True
            
        except Exception as e:
            logger.error(f"设置群组头像失败: {e}")
            return False

    async def create_group_with_bot(self, qqid: str, contact_name: str,
                                  description: str = "", avatar_url: str = None) -> Dict:
        """创建群组并添加机器人"""
        try:
            # 检查是否已经有群组映射
            existing_contact = await self.contact_manager.check_existing_mapping(qqid)
            
            if existing_contact:
                return {
                    'success': True,
                    'chat_id': existing_contact.get('chatId'),
                    'group_name': existing_contact.get('name'),
                    'group_type': 'group',
                    'bot_invited': True,
                    'bot_is_admin': True,
                    'avatar_set': True,
                    'already_exists': True
                }
            
            # 获取跨线程安全的客户端
            client = self._get_telethon_client()
            
            # 获取机器人实体
            bot_entity = await self._get_bot_entity(client)
            if not bot_entity:
                raise Exception("无法获取机器人实体")
            
            # 创建群组
            group_name = f"{contact_name}"
            
            result = await client(CreateChatRequest(
                users=[bot_entity],
                title=group_name
            ))
            
            # 获取群组ID
            chat_id = await self._extract_chat_id(client, result, group_name)
            if chat_id is None:
                raise Exception("无法获取创建的群组ID")
            
            # 设置 bot 为管理员
            bot_is_admin = await self._set_bot_admin(client, chat_id, bot_entity)
            
            # 设置群组头像
            avatar_set = False
            if avatar_url:
                avatar_set = await self._set_group_avatar(client, chat_id, avatar_url)
            
            # 将群组移动到文件夹
            folder_name = config.QQ_CHAT_FOLDER
            
            moved_to_folder = False
            moved_to_folder = await self._move_chat_to_folder(client, chat_id, folder_name)
            if not moved_to_folder:
                logger.warning(f"移动群组到文件夹失败，但群组创建成功")

            # 保存映射关系
            await self.contact_manager.save_chat_qqid_mapping(qqid, contact_name, chat_id, avatar_url)
            
            return {
                'success': True,
                'chat_id': chat_id,
                'group_name': group_name,
                'group_type': 'group',
                'bot_invited': True,
                'bot_is_admin': bot_is_admin,
                'avatar_set': avatar_set,
                'mapping_updated': True,
                'moved_to_folder': moved_to_folder
            }
            
        except Exception as e:
            logger.error(f"创建群组失败: {e}")
            return {'success': False, 'error': str(e)}

    async def _extract_chat_id(self, client, result, group_name):
        """提取群组ID"""
        chat_id = None
        
        if hasattr(result, 'chats') and result.chats:
            chat = result.chats[0]
            chat_id = -chat.id
        elif hasattr(result, 'updates') and hasattr(result.updates, 'chats') and result.updates.chats:
            chat = result.updates.chats[0]
            chat_id = -chat.id
        
        if chat_id is None:
            await asyncio.sleep(1)
            dialogs = await client.get_dialogs(limit=20)
            for dialog in dialogs:
                if (dialog.title == group_name and
                    dialog.is_group and
                    not dialog.is_channel):
                    chat_id = dialog.id
                    break
        
        return chat_id

    async def _set_bot_admin(self, client, chat_id, bot_entity):
        """设置机器人为管理员"""
        try:
            original_chat_id = abs(chat_id)
            await client(EditChatAdminRequest(
                chat_id=original_chat_id,
                user_id=bot_entity,
                is_admin=True
            ))
            return True
        except Exception as e:
            logger.error(f"设置 bot 为管理员失败: {e}")
            return False

    async def _move_chat_to_folder(self, client, chat_id: int, folder_name: str) -> bool:
        """将聊天移动到指定文件夹"""
        try:            
            filters_result = await client(GetDialogFiltersRequest())
            
            target_filter = None
            for filter_obj in filters_result.filters:
                if filter_obj.__class__.__name__ == 'DialogFilterDefault':
                    continue
                if hasattr(filter_obj, 'title'):
                    title_text = filter_obj.title.text if hasattr(filter_obj.title, 'text') else str(filter_obj.title)
                    if title_text == folder_name:
                        target_filter = filter_obj
                        break
            
            chat_entity = await client.get_entity(chat_id)
            
            if hasattr(chat_entity, 'access_hash'):
                input_peer = InputPeerChannel(chat_entity.id, chat_entity.access_hash)
            else:
                input_peer = InputPeerChat(abs(chat_id))
            
            if target_filter is None:
                existing_ids = [f.id for f in filters_result.filters 
                              if hasattr(f, 'id') and f.__class__.__name__ != 'DialogFilterDefault']
                new_id = max(existing_ids) + 1 if existing_ids else 1
                
                title_obj = TextWithEntities(text=folder_name, entities=[])
                
                target_filter = DialogFilter(
                    id=new_id,
                    title=title_obj,
                    emoticon="📱",
                    pinned_peers=[],
                    include_peers=[input_peer],
                    exclude_peers=[],
                    contacts=False,
                    non_contacts=False,
                    groups=True,
                    broadcasts=False,
                    bots=False,
                    exclude_muted=False,
                    exclude_read=False,
                    exclude_archived=False
                )
                
                await client(UpdateDialogFilterRequest(
                    id=new_id,
                    filter=target_filter
                ))
                
                return True
            
            else:
                peer_exists = any(
                    (hasattr(p, 'chat_id') and hasattr(input_peer, 'chat_id') and p.chat_id == input_peer.chat_id) or
                    (hasattr(p, 'channel_id') and hasattr(input_peer, 'channel_id') and p.channel_id == input_peer.channel_id)
                    for p in target_filter.include_peers
                )
                
                if peer_exists:
                    return True
                
                new_include_peers = list(target_filter.include_peers)
                new_include_peers.append(input_peer)
                
                updated_filter = DialogFilter(
                    id=target_filter.id,
                    title=target_filter.title,
                    emoticon=getattr(target_filter, 'emoticon', "📱"),
                    pinned_peers=list(target_filter.pinned_peers),
                    include_peers=new_include_peers,
                    exclude_peers=list(target_filter.exclude_peers),
                    contacts=getattr(target_filter, 'contacts', False),
                    non_contacts=getattr(target_filter, 'non_contacts', False),
                    groups=getattr(target_filter, 'groups', True),
                    broadcasts=getattr(target_filter, 'broadcasts', False),
                    bots=getattr(target_filter, 'bots', False),
                    exclude_muted=getattr(target_filter, 'exclude_muted', False),
                    exclude_read=getattr(target_filter, 'exclude_read', False),
                    exclude_archived=getattr(target_filter, 'exclude_archived', False)
                )
                
                await client(UpdateDialogFilterRequest(
                    id=target_filter.id,
                    filter=updated_filter
                ))
                
                return True
            
        except Exception as e:
            logger.error(f"移动群组到文件夹失败: {e}")
            return False

# ==================== 调用接口 ====================

async def create_group(qqid: str, contact_name: str, description: str = "", avatar_url: str = None) -> Dict:
    """异步方式创建群组"""
    async with GroupManager() as group_manager:
        return await group_manager.create_group_with_bot(qqid, contact_name, description, avatar_url)

def create_group_sync(qqid: str, contact_name: str, description: str = "", avatar_url: str = None) -> Dict:
    """同步方式创建群组"""
    def run_in_thread():
        """在新线程中运行异步代码"""
        try:
            # 创建新的事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                return loop.run_until_complete(
                    create_group(qqid, contact_name, description, avatar_url)
                )
            finally:
                loop.close()
        except Exception as e:
            logger.error(f"线程中运行异步代码失败: {e}")
            return {'success': False, 'error': str(e)}
    
    try:
        # 使用线程池执行
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_in_thread)
            return future.result(timeout=120)  # 2分钟超时
    except concurrent.futures.TimeoutError:
        logger.error("创建群组超时")
        return {'success': False, 'error': '操作超时'}
    except Exception as e:
        logger.error(f"同步创建群组失败: {e}")
        return {'success': False, 'error': str(e)}
