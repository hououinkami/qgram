import asyncio
import json
import logging
import os
from typing import Dict, Optional, List
from dataclasses import dataclass, field

import aiosqlite

import config
from api import qq_contacts
from api.telegram_sender import telegram_sender
from api.qq_api import qq_api
from utils.group_binding import create_group

logger = logging.getLogger(__name__)

def single_execution(func):
    """确保函数同时只能执行一次的装饰器"""
    def wrapper(self, *args, **kwargs):
        if not hasattr(self, f'_{func.__name__}_lock'):
            setattr(self, f'_{func.__name__}_lock', asyncio.Lock())
            setattr(self, f'_{func.__name__}_running', False)
        
        lock = getattr(self, f'_{func.__name__}_lock')
        is_running = getattr(self, f'_{func.__name__}_running')
        
        async def async_wrapper():
            if is_running:
                if args:  # 假设第一个参数是chat_id
                    logger.debug("⚠️ 操作正在进行中，请等待完成")
                    # await telegram_sender.send_text(args[0], "⚠️ 操作正在进行中，请等待完成")
                return
            
            async with lock:
                try:
                    setattr(self, f'_{func.__name__}_running', True)
                    return await func(self, *args, **kwargs)
                finally:
                    setattr(self, f'_{func.__name__}_running', False)
        
        return async_wrapper()
    return wrapper

@dataclass
class Contact:
    """联系人数据类"""
    qqid: str
    name: str
    chat_id: int = -9999999999
    is_group: bool = False
    is_receive: bool = True
    avatar_url: str = ""
    qq_name: str = ""
    
    def __post_init__(self):
        """初始化后处理"""
        pass
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Contact':
        """从字典创建Contact对象"""
        return cls(**data)
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'qqid': self.qqid,
            'name': self.name,
            'chat_id': self.chat_id,
            'is_group': self.is_group,
            'is_receive': self.is_receive,
            'avatar_url': self.avatar_url,
            'qq_name': self.qq_name
        }

class ContactManager:
    """联系人管理器 - SQLite优化版本"""
    
    def __init__(self, db_path: str = None):
        """初始化联系人管理器"""
        if db_path is None:
            # 默认数据库路径
            self.db_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                "database", 
                "contact.db"
            )
        else:
            self.db_path = db_path
        
        self._initialized = False
        
        # 确保数据库目录存在
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    async def initialize(self):
        """初始化数据库连接和表结构"""
        if self._initialized:
            return
        
        try:
            # 创建表结构
            await self._create_tables()
            
            # 创建索引
            await self._create_indexes()
            
            self._initialized = True

        except Exception as e:
            logger.error(f"❌ 联系人管理器初始化失败: {e}")
            raise
    
    async def _create_tables(self):
        """创建数据库表"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS contacts (
            qqid TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            chat_id INTEGER DEFAULT -9999999999,
            is_group INTEGER DEFAULT 0,
            is_receive INTEGER DEFAULT 1,
            avatar_url TEXT DEFAULT '',
            qq_name TEXT DEFAULT ''
        );
        """
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(create_table_sql)
            await db.commit()
    
    async def _create_indexes(self):
        """创建数据库索引"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_contacts_chat_id ON contacts(chat_id);",
            "CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(name);",
            "CREATE INDEX IF NOT EXISTS idx_contacts_is_group ON contacts(is_group);",
            "CREATE INDEX IF NOT EXISTS idx_contacts_is_receive ON contacts(is_receive);",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_qqid ON contacts(qqid);"
        ]
        
        async with aiosqlite.connect(self.db_path) as db:
            for index_sql in indexes:
                await db.execute(index_sql)
            await db.commit()

    async def get_contact(self, qqid: str) -> Optional[Contact]:
        """获取联系人信息"""
        if not self._initialized:
            await self.initialize()
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM contacts WHERE qqid = ?", (qqid,)
                )
                row = await cursor.fetchone()
                
                if row:
                    return Contact(
                        qqid=row['qqid'],
                        name=row['name'],
                        chat_id=row['chat_id'],
                        is_group=bool(row['is_group']),
                        is_receive=bool(row['is_receive']),
                        avatar_url=row['avatar_url'],
                        qq_name=row['qq_name']
                    )
                return None
                
        except Exception as e:
            logger.error(f"获取联系人失败 {qqid}: {e}")
            return None
    
    async def get_qqid_by_chatid(self, chat_id: int) -> Optional[str]:
        """通过chatId获取qqid"""
        if not self._initialized:
            await self.initialize()
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT qqid FROM contacts WHERE chat_id = ?", (int(chat_id),)
                )
                row = await cursor.fetchone()
                return row['qqid'] if row else None
                
        except Exception as e:
            logger.error(f"❌ 通过ChatID获取qqid失败 {chat_id}: {e}")
            return None
    
    async def get_contact_by_chatid(self, chat_id: int) -> Optional[Contact]:
        """通过chatId获取联系人完整信息"""
        if not self._initialized:
            await self.initialize()
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM contacts WHERE chat_id = ?", (int(chat_id),)
                )
                row = await cursor.fetchone()
                
                if row:
                    return Contact(
                        qqid=row['qqid'],
                        name=row['name'],
                        chat_id=row['chat_id'],
                        is_group=bool(row['is_group']),
                        is_receive=bool(row['is_receive']),
                        avatar_url=row['avatar_url'],
                        qq_name=row['qq_name']
                    )
                return None
                
        except Exception as e:
            logger.error(f"通过ChatID获取联系人失败 {chat_id}: {e}")
            return None
    
    async def search_contacts_by_name(self, username: str = "") -> List[Contact]:
        """根据用户名搜索联系人"""
        if not self._initialized:
            await self.initialize()
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                
                if not username or not username.strip():
                    # 返回所有联系人
                    cursor = await db.execute("SELECT * FROM contacts ORDER BY name")
                else:
                    # 搜索匹配的联系人
                    username_pattern = f"%{username.strip()}%"
                    cursor = await db.execute(
                        "SELECT * FROM contacts WHERE name LIKE ? ORDER BY name", 
                        (username_pattern,)
                    )
                
                rows = await cursor.fetchall()
                
                return [
                    Contact(
                        qqid=row['qqid'],
                        name=row['name'],
                        chat_id=row['chat_id'],
                        is_group=bool(row['is_group']),
                        is_receive=bool(row['is_receive']),
                        avatar_url=row['avatar_url'],
                        qq_name=row['qq_name']
                    ) for row in rows
                ]
            
        except Exception as e:
            logger.error(f"❌ 搜索联系人失败 - 用户名: {username}, 错误: {e}")
            return []

    async def save_contact(self, contact: Contact) -> bool:
        """保存或更新联系人信息"""
        if not self._initialized:
            await self.initialize()
        
        try:
            
            # 修改为 SQLite 语法
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
                    INSERT OR REPLACE INTO contacts (
                        qqid, name, chat_id, is_group, is_receive, 
                        avatar_url, qq_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    contact.qqid, contact.name, contact.chat_id, int(contact.is_group),
                    int(contact.is_receive), contact.avatar_url, 
                    contact.qq_name
                ))
                await db.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 保存联系人失败 {contact.qqid}: {e}")
            return False
    
    async def delete_contact(self, qqid: str) -> bool:
        """删除联系人信息"""
        if not self._initialized:
            await self.initialize()
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("DELETE FROM contacts WHERE qqid = ?", (qqid,))
                await db.commit()
                
                # 删除wx好友
                payload = {
                    "user_id": config.MY_QQ_ID,
                    "friend_id": qqid,
                    "temp_block": False,
                    "temp_both_del": False
                }
                await qq_api("DELETE_FRIEND", payload)
                
                if cursor.rowcount > 0:
                    logger.info(f"🗑️ 成功删除联系人: {qqid}")
                    return True
                else:
                    logger.warning(f"⚠️ 联系人不存在: {qqid}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ 删除联系人失败: {qqid}, 错误: {e}")
            return False
    
    async def delete_contact_by_chatid(self, chat_id: int) -> bool:
        """通过ChatID删除联系人信息"""
        if not self._initialized:
            await self.initialize()
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("DELETE FROM contacts WHERE chat_id = ?", (int(chat_id),))
                await db.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"🗑️ 成功通过ChatID删除联系人: {chat_id}")
                    return True
                else:
                    logger.warning(f"⚠️ 未找到ChatID对应的联系人: {chat_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ 通过ChatID删除联系人失败: {chat_id}, 错误: {e}")
            return False
    
    async def update_contact(self, qqid: str, updates: dict) -> bool:
        """通过qqid更新联系人的指定字段"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # 首先获取当前联系人信息
            contact = await self.get_contact(qqid)
            if not contact:
                logger.warning(f"⚠️ 未找到qqid对应的联系人: {qqid}")
                return False
            
            # 处理更新字段
            update_fields = []
            update_values = []
            
            for key, value in updates.items():
                # 验证字段是否存在
                if not hasattr(contact, key):
                    logger.warning(f"⚠️ 无效字段: {key}")
                    continue
                
                # 特殊处理切换布尔值
                if value == "toggle" and key in ["is_receive", "is_group"]:
                    current_value = getattr(contact, key)
                    value = not current_value
                elif key in ["is_receive", "is_group"] and isinstance(value, str):
                    value = value.lower() in ['true', '1', 'yes', 'on']
                
                # SQLite 布尔值转整数
                if key in ["is_receive", "is_group"]:
                    value = int(value)
                
                update_fields.append(f"{key} = ?")
                update_values.append(value)
            
            if not update_fields:
                return True
            
            # 添加WHERE条件的参数
            update_values.append(qqid)
            
            # 构建并执行SQL
            sql = f"UPDATE contacts SET {', '.join(update_fields)} WHERE qqid = ?"
            
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(sql, update_values)
                await db.commit()
                
                if cursor.rowcount > 0:
                    logger.info(f"✅ 成功更新联系人: {qqid}, 更新字段: {list(updates.keys())}")
                    return True
                else:
                    logger.warning(f"⚠️ 未更新任何记录: {qqid}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ 更新联系人字段失败 - qqid: {qqid}, 更新: {updates}, 错误: {e}")
            return False

    async def update_contact_by_chatid(self, chat_id: int, updates: dict) -> bool:
        """通过ChatID更新联系人的指定字段"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # 首先获取当前联系人信息
            contact = await self.get_contact_by_chatid(chat_id)
            if not contact:
                logger.warning(f"⚠️ 未找到ChatID对应的联系人: {chat_id}")
                return False
            
            # 处理更新字段
            update_fields = []
            update_values = []
            
            for key, value in updates.items():
                # 验证字段是否存在
                if not hasattr(contact, key):
                    logger.warning(f"⚠️ 无效字段: {key}")
                    continue
                
                # 特殊处理切换布尔值
                if value == "toggle" and key in ["is_receive", "is_group"]:
                    current_value = getattr(contact, key)
                    value = not current_value
                elif key in ["is_receive", "is_group"] and isinstance(value, str):
                    value = value.lower() in ['true', '1', 'yes', 'on']
                
                # SQLite 布尔值转整数
                if key in ["is_receive", "is_group"]:
                    value = int(value)
                
                update_fields.append(f"{key} = ?")
                update_values.append(value)
            
            if not update_fields:
                return True
            
            # 添加WHERE条件的参数
            update_values.append(int(chat_id))
            
            # 构建并执行SQL
            sql = f"UPDATE contacts SET {', '.join(update_fields)} WHERE chat_id = ?"
            
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(sql, update_values)
                await db.commit()
                
                return cursor.rowcount > 0
                
        except Exception as e:
            logger.error(f"❌ 更新联系人字段失败 - ChatID: {chat_id}, 更新: {updates}, 错误: {e}")
            return False

    async def check_existing_mapping(self, qqid: str) -> Optional[Contact]:
        """检查是否已有映射"""
        if not self._initialized:
            await self.initialize()
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT * FROM contacts WHERE qqid = ? AND chat_id != -9999999999", 
                    (qqid,)
                )
                row = await cursor.fetchone()
                
                if row:
                    return Contact(
                        qqid=row['qqid'],
                        name=row['name'],
                        chat_id=row['chat_id'],
                        is_group=bool(row['is_group']),
                        is_receive=bool(row['is_receive']),
                        avatar_url=row['avatar_url'],
                        qq_name=row['qq_name']
                    )
                return None
                
        except Exception as e:
            logger.error(f"❌ 检查映射失败 {qqid}: {e}")
            return None

    async def save_chat_qqid_mapping(self, qqid: str, name: str, chat_id: int, avatar_url: str = None, is_group: bool = False):
        """保存群组ID和微信ID的映射关系"""
        if not self._initialized:
            await self.initialize()
        
        try:
            # 检查是否已存在相同的映射
            existing = await self.get_contact(qqid)
            if existing and existing.chat_id == chat_id:
                return
            
            contact = Contact(
                qqid=qqid,
                name=name,
                chat_id=chat_id,
                is_group=is_group,
                is_receive=True,
                avatar_url=avatar_url or "",
                qq_name=""
            )
            
            await self.save_contact(contact)
            
        except Exception as e:
            logger.error(f"❌ 保存映射关系失败: {e}")

    async def batch_save_contacts(self, contacts: List[Contact]) -> int:
        """批量保存联系人"""
        if not self._initialized:
            await self.initialize()
        
        if not contacts:
            return 0
        
        try:
            saved_count = 0
            async with aiosqlite.connect(self.db_path) as db:
                for contact in contacts:
                    await db.execute("""
                        INSERT OR REPLACE INTO contacts (
                            qqid, name, chat_id, is_group, is_receive, 
                            avatar_url, qq_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        contact.qqid, contact.name, contact.chat_id, int(contact.is_group),
                        int(contact.is_receive), contact.avatar_url, 
                        contact.qq_name
                    ))
                    saved_count += 1
                await db.commit()
            
            logger.info(f"✅ 批量保存联系人完成: {saved_count} 个")
            return saved_count
            
        except Exception as e:
            logger.error(f"❌ 批量保存联系人失败: {e}")
            return 0

    async def import_from_json(self, json_file_path: str = None) -> int:
        """从JSON文件导入联系人数据到数据库"""
        if not self._initialized:
            await self.initialize()
        
        if json_file_path is None:
            # 使用默认的contact.json路径
            json_file_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                "database", 
                "contact.json"
            )
        
        try:
            if not os.path.exists(json_file_path):
                logger.warning(f"⚠️ JSON文件不存在: {json_file_path}")
                return 0
            
            # 读取JSON文件
            with open(json_file_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
            
            if not isinstance(json_data, list):
                logger.error("❌ JSON文件格式错误，应该是联系人数组")
                return 0
            
            # 转换为Contact对象
            contacts = []
            for item in json_data:
                try:
                    contact = Contact.from_dict(item)
                    contacts.append(contact)
                except Exception as e:
                    logger.warning(f"⚠️ 跳过无效联系人数据: {item}, 错误: {e}")
                    continue
            
            # 批量保存到数据库
            imported_count = await self.batch_save_contacts(contacts)
            
            logger.info(f"✅ 从JSON导入联系人完成: {imported_count} 个")
            return imported_count
            
        except Exception as e:
            logger.error(f"❌ 从JSON导入联系人失败: {e}")
            return 0

    async def export_to_json(self, json_file_path: str = None) -> int:
        """导出联系人数据到JSON文件"""
        if not self._initialized:
            await self.initialize()
        
        if json_file_path is None:
            # 使用默认的contact.json路径
            json_file_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                "database",
                "contact.json"
            )
        
        try:
            # 获取所有联系人
            contacts = await self.search_contacts_by_name("")
            
            # 转换为字典格式
            json_data = [contact.to_dict() for contact in contacts]
            
            # 写入JSON文件
            with open(json_file_path, 'w', encoding='utf-8') as file:
                json.dump(json_data, file, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ 导出联系人到JSON完成: {len(contacts)} 个，文件: {json_file_path}")
            return len(contacts)
            
        except Exception as e:
            logger.error(f"❌ 导出联系人到JSON失败: {e}")
            return 0

    async def create_group_for_contact_async(self, qqid: str, contact_name: str, description: str = "", avatar_url: str = None, is_group: bool = False) -> Optional[Dict]:
        """异步方式创建群组"""        
        try:
            # 先创建群组
            result = await create_group(qqid, contact_name, description, avatar_url)
            
            # 创建成功后处理数据库
            if result.get('success') and result.get('chat_id'):
                new_chat_id = result['chat_id']
                
                # 检查是否存在联系人记录
                contact = await self.get_contact(qqid)
                
                if contact:
                    # 如果存在记录，更新chat_id
                    success = await self.update_contact(qqid, {'chat_id': new_chat_id})
                    if not success:
                        logger.error(f"❌ 更新联系人 {qqid} 的chat_id失败")
                else:
                    # 如果不存在记录，创建新的映射关系
                    await self.save_chat_qqid_mapping(
                        qqid, contact_name, new_chat_id, avatar_url, is_group
                    )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 创建群组失败: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _update_existing_contacts_batch(self, contacts_batch: List[Contact], update_tg: bool = True, user_info_dict: dict = None) -> int:
        """
        批量更新已存在联系人信息的内部函数（仅更新，不创建新联系人）
        
        Args:
            contacts_batch: 联系人批次列表
            update_tg: 是否同时更新Telegram群组信息
            user_info_dict: 可选的用户信息字典，如果提供则不重新获取
            
        Returns:
            int: 更新数量
        """
        try:
            # 如果没有提供用户信息字典，则获取用户信息
            if user_info_dict is None:
                # 获取qqid列表用于批量查询用户信息
                qqid_list = [contact.qqid for contact in contacts_batch]
                
                # 调用get_user_info获取用户信息
                user_info_dict = await qq_contacts.get_user_info(qqid_list)
                
                if not user_info_dict:
                    logger.warning(f"⚠️ 未获取到用户信息")
                    return 0
            
            updated_contacts_count = 0
            
            # 遍历联系人
            for contact in contacts_batch:
                qqid = contact.qqid
                user_info = user_info_dict.get(qqid)
                
                if user_info is None:
                    logger.warning(f"⚠️ 用户 {qqid} 信息获取失败")
                    continue
                
                # 检查是否需要更新name和avatar_url
                need_update = False
                need_tg_update = False
                updates = {}
                
                # 检查name是否需要更新
                if contact.name != user_info.name:
                    updates['name'] = user_info.name
                    need_update = True
                    need_tg_update = True
                    logger.info(f"🔄 更新联系人姓名: {qqid} -> {user_info.name}")
                
                # 检查avatar_url是否需要更新
                new_avatar_url = user_info.avatar_url if user_info.avatar_url else ""
                if contact.avatar_url != new_avatar_url:
                    updates['avatar_url'] = new_avatar_url
                    need_update = True
                    need_tg_update = True
                    logger.info(f"🔄 更新联系人头像: {qqid}")
                
                # 如果需要更新数据库
                if need_update:
                    success = await self.update_contact(qqid, updates)
                    if success:
                        updated_contacts_count += 1
                        
                        # 如果联系人已绑定到Telegram群组且需要更新TG信息
                        if update_tg and need_tg_update and contact.chat_id != -9999999999:
                            try:
                                # 准备更新参数
                                name_to_use = updates.get('name') if 'name' in updates else None
                                avatar_to_use = updates.get('avatar_url') if 'avatar_url' in updates else None
                                
                                # 更新TG群组信息
                                await qq_contacts.update_info(
                                    contact.chat_id, 
                                    name_to_use, 
                                    avatar_to_use
                                )
                                
                            except Exception as e:
                                logger.error(f"❌ 更新Telegram群组信息失败 {contact.chat_id}: {e}")
            
            return updated_contacts_count
            
        except Exception as e:
            logger.error(f"❌ 批量更新联系人信息失败: {str(e)}")
            return 0

    @single_execution
    async def refresh_contacts_info(self, chat_id: int):
        """
        刷新数据库中所有联系人的信息
        从数据库获取所有联系人，按20人一组获取最新信息并更新
        """
        try:
            # 发送开始处理的消息
            logger.info("🔄 正在刷新所有联系人信息...")
            
            # 获取数据库中的所有联系人
            all_contacts = await self.search_contacts_by_name("")
            
            if not all_contacts:
                logger.warning("❌ 数据库中没有联系人记录")
                return
            
            logger.info(f"📋 数据库中共有 {len(all_contacts)} 个联系人，正在刷新信息...")
            
            # 将联系人按每组20个分割
            batch_size = 20
            batches = [all_contacts[i:i + batch_size] for i in range(0, len(all_contacts), batch_size)]
            
            total_updated_contacts = 0
            total_batches = len(batches)
            
            # 处理每个批次
            for batch_index, batch in enumerate(batches):
                try:
                    # 发送进度更新
                    if batch_index % 5 == 0 or batch_index == total_batches - 1:
                        progress = f"⏳ 刷新进度: {batch_index + 1}/{total_batches} 批次"
                        logger.info(progress)
                    
                    # 调用更新函数（不传入user_info_dict，让函数内部获取）
                    updated_count = await self._update_existing_contacts_batch(batch, update_tg=True)
                    total_updated_contacts += updated_count
                    
                    # 每处理几个批次休眠一下，避免请求过于频繁
                    if batch_index < total_batches - 1:
                        await asyncio.sleep(0.5)
                        
                except Exception as e:
                    logger.error(f"❌ 处理批次 {batch_index + 1} 时出错: {str(e)}")
                    continue
            
            # 生成结果消息
            if total_updated_contacts > 0:
                success_msg = f"✅ 刷新完成！更新了 {total_updated_contacts} 个联系人"
            else:
                success_msg = "✅ 刷新完成！所有联系人信息已是最新，无需更新"
            
            logger.info(success_msg)
            
            # 发送统计信息
            stats_msg = f"""
📊 **更新結果**
• 友人: {len(all_contacts)}
• 更新: {total_updated_contacts}
• 維持: {len(all_contacts) - total_updated_contacts}
            """
            logger.info(stats_msg)
            await telegram_sender.send_text(chat_id, stats_msg)
            
        except Exception as e:
            error_msg = f"❌ 更新失敗: {str(e)}"
            await telegram_sender.send_text(chat_id, error_msg)
            logger.error(f"❌ 刷新联系人信息执行失败: {str(e)}")

    @single_execution
    async def update_contacts_and_sync_to_db(self, chat_id: int, update: bool = False):
        """获取联系人列表并同步到数据库"""
        try:
            # 发送开始处理的消息
            logger.info("🔄 正在获取联系人列表...")
            
            # 获取联系人列表
            friend_contacts, chatroom_contacts, gh_contacts = await qq_contacts.get_friends()
            all_contacts = friend_contacts + chatroom_contacts
            if not all_contacts:
                await telegram_sender.send_text(chat_id, "❌ 友人データなし")
                return
            
            logger.info(f"📋 获取到 {len(all_contacts)} 个好友，正在同步信息...")
            
            # 将all_contacts按每组20个分割
            batch_size = 20
            batches = [all_contacts[i:i + batch_size] for i in range(0, len(all_contacts), batch_size)]
            
            new_contacts_count = 0
            updated_contacts_count = 0
            total_batches = len(batches)
            new_contacts = []
            
            # 处理每个批次
            for batch_index, batch in enumerate(batches):
                try:
                    # 发送进度更新
                    if batch_index == total_batches - 1:
                        progress = f"⏳ 处理进度: {batch_index + 1}/{total_batches} 批次"
                        logger.info(progress)
                    
                    # 调用get_user_info获取用户信息
                    user_info_dict = await qq_contacts.get_user_info(batch)
                    
                    if not user_info_dict:
                        logger.warning(f"⚠️ 批次 {batch_index + 1} 未获取到用户信息")
                        continue
                    
                    # 分离新联系人和需要更新的现有联系人
                    existing_contacts_to_update = []
                    
                    # 遍历用户信息
                    for qqid, user_info in user_info_dict.items():
                        if user_info is None:
                            logger.warning(f"⚠️ 用户 {qqid} 信息获取失败")
                            continue
                        
                        # 检查qqid是否已存在
                        existing_contact = await self.get_contact(qqid)
                        
                        if existing_contact is None:
                            # 不存在则创建新联系人
                            new_contact = Contact(
                                qqid=qqid,
                                name=user_info.name,
                                chat_id=-9999999999,
                                is_group=qqid.endswith('@chatroom'),
                                is_receive=True,
                                avatar_url=user_info.avatar_url if user_info.avatar_url else "",
                                qq_name=""
                            )
                            
                            new_contacts.append(new_contact)
                            new_contacts_count += 1
                            logger.info(f"➕ 添加新联系人: {user_info.name} ({qqid})")
                        elif update:
                            # 如果需要更新，收集需要更新的现有联系人
                            existing_contacts_to_update.append(existing_contact)
                    
                    # 如果有需要更新的现有联系人，批量更新（复用函数，传入已获取的用户信息）
                    if update and existing_contacts_to_update:
                        updated_count = await self._update_existing_contacts_batch(
                            existing_contacts_to_update, 
                            update_tg=True, 
                            user_info_dict=user_info_dict  # 传入已获取的用户信息，避免重复API调用
                        )
                        updated_contacts_count += updated_count
                    
                    # 每处理几个批次休眠一下，避免请求过于频繁
                    if batch_index < total_batches - 1:
                        await asyncio.sleep(0.5)
                        
                except Exception as e:
                    logger.error(f"❌ 处理批次 {batch_index + 1} 时出错: {str(e)}")
                    continue
            
            # 批量保存所有新联系人
            new_saved_count = 0
            if new_contacts:
                new_saved_count = await self.batch_save_contacts(new_contacts)
            
            # 生成结果消息
            if new_saved_count > 0 or updated_contacts_count > 0:
                success_msg = f"✅ 同步完成！新增 {new_saved_count} 个联系人，更新 {updated_contacts_count} 个联系人"
            else:
                success_msg = "✅ 同步完成！所有联系人信息已是最新，无需更新"
            
            logger.info(success_msg)
            
            # 获取当前总数
            total_contacts = await self.get_contacts_count()
            
            # 发送统计信息
            stats_msg = f"""
📊 **更新結果**
• 友人: {len(all_contacts)}
• 新規: {new_saved_count}
• 更新: {updated_contacts_count}
• 全部: {total_contacts}
            """
            logger.info(stats_msg)
            await telegram_sender.send_text(chat_id, stats_msg)
            
        except Exception as e:
            error_msg = f"❌ 更新失敗: {str(e)}"
            await telegram_sender.send_text(chat_id, error_msg)
            logger.error(f"❌ 更新联系人执行失败: {str(e)}")

    async def get_contacts_count(self) -> int:
        """获取联系人总数"""
        if not self._initialized:
            await self.initialize()
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("SELECT COUNT(*) as count FROM contacts")
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"❌ 获取联系人总数失败: {e}")
            return 0

    def get_contact_type_icon(self, contact: Contact) -> str:
        """
        获取联系人类型图标
        
        Args:
            contact (Contact): 联系人对象
            
        Returns:
            str: 对应的图标
                👤 - 个人好友
                👥 - 群组聊天
                📢 - 公众号
        """
        if contact.is_group:
            return "👥"  # 群组
        else:
            if contact.qqid.startswith('gh_'):
                return "📢"  # 公众号
            elif contact.qqid.endswith('@openim'):
                return "🈺"  # 企业微信
            else:
                return "👤"  # 个人好友

    def get_contact_type_text(self, contact: Contact) -> str:
        """
        获取联系人类型文本描述
        
        Args:
            contact (Contact): 联系人对象
            
        Returns:
            str: 类型描述文本
        """
        if contact.is_group:
            if contact.qqid.startswith('gh_'):
                return f"📢 公众号"
            else:
                return f"👥 群"
        else:
            return f"👤 私聊"

    def get_contact_receive_icon(self, contact: Contact) -> str:
        """
        获取接收状态图标
        
        Args:
            contact (Contact): 联系人对象
            
        Returns:
            str: 对应的图标
                🔕 - 不接收消息
                "" - 接收消息（无图标）
        """
        if not contact.is_receive:
            return "🔕"
        else:
            return ""

    async def get_statistics(self) -> Dict[str, int]:
        """获取联系人统计信息"""
        if not self._initialized:
            await self.initialize()
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # 总数
                cursor = await db.execute("SELECT COUNT(*) FROM contacts")
                total_count = (await cursor.fetchone())[0]
                
                # 群组数
                cursor = await db.execute("SELECT COUNT(*) FROM contacts WHERE is_group = 1")
                group_count = (await cursor.fetchone())[0]
                
                # 个人联系人数
                personal_count = total_count - group_count
                
                # 已绑定的联系人数
                cursor = await db.execute("SELECT COUNT(*) FROM contacts WHERE chat_id != -9999999999")
                bound_count = (await cursor.fetchone())[0]
                
                # 接收消息的联系人数
                cursor = await db.execute("SELECT COUNT(*) FROM contacts WHERE is_receive = 1")
                receive_count = (await cursor.fetchone())[0]
                
                return {
                    'total': total_count,
                    'groups': group_count,
                    'personal': personal_count,
                    'bound': bound_count,
                    'receiving': receive_count
                }
                
        except Exception as e:
            logger.error(f"❌ 获取统计信息失败: {e}")
            return {
                'total': 0,
                'groups': 0,
                'personal': 0,
                'bound': 0,
                'receiving': 0
            }

# 创建全局实例
contact_manager = ContactManager()

# 添加一个初始化函数，用于应用启动时调用
async def initialize_contact_manager():
    """初始化联系人管理器"""
    await contact_manager.initialize()
    logger.info("✅ 全局联系人管理器初始化完成")
