import asyncio
import logging
import os
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Optional, List, Any

import aiosqlite

logger = logging.getLogger(__name__)

class MappingResult:
    """映射结果对象，支持obj.attr访问方式"""
    def __init__(self, data: dict):
        self.tgmsgid = data.get('tgmsgid', 0)
        self.from_id = data.get('from_id', '')
        self.to_id = data.get('to_id', '')
        self.msgid = data.get('msgid', 0)
        self.telethonmsgid = data.get('telethonmsgid', 0)
    
    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            'tgmsgid': self.tgmsgid,
            'from_id': self.from_id,
            'to_id': self.to_id,
            'msgid': self.msgid,
            'telethonmsgid': self.telethonmsgid
        }
    
    def __repr__(self):
        return f"MappingResult(tgmsgid={self.tgmsgid}, msgid={self.msgid})"

class MappingManager:
    def __init__(self):
        """
        初始化映射管理器 - SQLite版本
        """
        
        # 数据库路径 - 使用相对于当前文件的路径
        self.db_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            "database", 
            "msgid.db"
        )
        self.database_dir = os.path.dirname(self.db_path)
        
        # 内存缓存，用于快速查询今日数据
        self.memory_cache = []
        self.cache_lock = threading.RLock()
        
        # 定期清理配置
        self.cleanup_enabled = True
        self.cleanup_days_to_keep = 7  # 默认保留7天
        self.cleanup_hour = 2  # 凌晨2点执行清理
        self.cleanup_task = None
        
        # 确保数据库目录存在
        if not os.path.exists(self.database_dir):
            os.makedirs(self.database_dir)
        
        # 初始化数据库
        asyncio.create_task(self._init_database())
        
        # 加载今日数据到缓存
        asyncio.create_task(self._load_today_to_cache())
        
        # 启动定期清理任务
        asyncio.create_task(self._start_cleanup_scheduler())

    async def _init_database(self):
        """初始化数据库表结构"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS message_mappings (
                        tgmsgid INTEGER PRIMARY KEY,
                        from_id TEXT NOT NULL,
                        to_id TEXT NOT NULL,
                        msgid INTEGER NOT NULL,
                        telethonmsgid INTEGER DEFAULT 0,
                        date TEXT NOT NULL
                    )
                ''')
                
                # 创建索引
                await db.execute('CREATE INDEX IF NOT EXISTS idx_date ON message_mappings(date)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_msgid ON message_mappings(msgid)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_telethonmsgid ON message_mappings(telethonmsgid)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_from_id ON message_mappings(from_id)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_date_tgmsgid ON message_mappings(date, tgmsgid)')
                
                await db.commit()
                logger.info("✅ 消息映射数据库初始化完成")
        except Exception as e:
            logger.error(f"❌ 数据库初始化失败: {e}")

    async def _load_today_to_cache(self):
        """将今日数据加载到内存缓存"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                    'SELECT * FROM message_mappings WHERE date = ? ORDER BY tgmsgid DESC',
                    (today,)
                ) as cursor:
                    rows = await cursor.fetchall()
                    
                with self.cache_lock:
                    self.memory_cache = []
                    for row in rows:
                        self.memory_cache.append(MappingResult({
                            'tgmsgid': row[0],
                            'from_id': row[1],
                            'to_id': row[2],
                            'msgid': row[3],
                            'telethonmsgid': row[4]
                        }))
                    
                logger.info(f"📅 加载了 {len(self.memory_cache)} 条今日映射到内存缓存")
        except Exception as e:
            logger.error(f"❌ 加载今日数据到缓存失败: {e}")
            with self.cache_lock:
                self.memory_cache = []

    async def add(self, tg_msg_id: int, from_qq_id: str, to_qq_id: str, 
                  qq_msg_id: int, telethon_msg_id: int = 0):
        """
        添加TG消息ID到微信消息的映射
        """
        today = datetime.now().strftime("%Y-%m-%d")
        
        mapping_data = MappingResult({
            'tgmsgid': int(tg_msg_id),
            'from_id': str(from_qq_id),
            'to_id': str(to_qq_id),
            'msgid': int(qq_msg_id),
            'telethonmsgid': int(telethon_msg_id)
        })
        
        try:
            # 1. 先更新内存缓存
            with self.cache_lock:
                # 检查是否已存在相同的tg_msg_id，如果存在则更新
                found = False
                for i, item in enumerate(self.memory_cache):
                    if item.tgmsgid == int(tg_msg_id):
                        self.memory_cache[i] = mapping_data
                        found = True
                        break
                if not found:
                    self.memory_cache.append(mapping_data)
                    # 按tgmsgid降序排序
                    self.memory_cache.sort(key=lambda x: x.tgmsgid, reverse=True)
            
            # 2. 保存到数据库
            await self._save_to_database(today, mapping_data)
            
            logger.debug(f"成功添加映射: TG({tg_msg_id}) -> WX({qq_msg_id})")
            
        except Exception as e:
            logger.error(f"❌ 添加映射失败: {e}")
            # 如果数据库写入失败，从缓存中移除
            with self.cache_lock:
                self.memory_cache = [item for item in self.memory_cache 
                                   if item.tgmsgid != int(tg_msg_id)]

    async def _save_to_database(self, date: str, mapping_data: MappingResult):
        """保存数据到数据库"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # 使用 INSERT OR REPLACE 来处理重复数据
                await db.execute('''
                    INSERT OR REPLACE INTO message_mappings 
                    (tgmsgid, from_id, to_id, msgid, telethonmsgid, date)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    mapping_data.tgmsgid,
                    mapping_data.from_id,
                    mapping_data.to_id,
                    mapping_data.msgid,
                    mapping_data.telethonmsgid,
                    date
                ))
                await db.commit()
        except Exception as e:
            logger.error(f"❌ 数据库保存失败: {e}")
            raise

    async def tg_to_qq(self, tg_msg_id: int) -> Optional[MappingResult]:
        """
        根据TG消息ID获取对应的微信消息信息
        返回MappingResult对象，支持obj.attr访问
        """
        tg_id_int = int(tg_msg_id)
        
        # 1. 首先检查内存缓存（今日数据）
        with self.cache_lock:
            for item in self.memory_cache:
                if item.tgmsgid == tg_id_int:
                    return item
        
        # 2. 检查数据库（往前搜索指定天数）
        return await self._search_in_database_by_days('tgmsgid', tg_id_int, days=3)

    async def qq_to_tg(self, qq_msg_id: int) -> Optional[int]:
        """
        根据微信消息ID反向查找TG消息ID
        返回tgmsgid整数
        """
        wx_id_int = int(qq_msg_id)
        
        # 1. 首先检查内存缓存
        with self.cache_lock:
            for item in self.memory_cache:
                if item.msgid == wx_id_int:
                    return item.tgmsgid
        
        # 2. 检查数据库
        result = await self._search_in_database_by_days('msgid', wx_id_int, days=3)
        return result.tgmsgid if result else None

    async def telethon_to_wx(self, telethon_msg_id: int) -> Optional[MappingResult]:
        """
        根据Telethon消息ID反向查找微信消息对象
        返回MappingResult对象，支持obj.attr访问
        """
        telethon_msg_id_int = int(telethon_msg_id)
        
        # 1. 首先检查内存缓存
        with self.cache_lock:
            for item in self.memory_cache:
                if item.telethonmsgid == telethon_msg_id_int:
                    return item
        
        # 2. 检查数据库
        return await self._search_in_database_by_days('telethonmsgid', telethon_msg_id_int, days=3)

    async def _search_in_database_by_days(self, field: str, value: int, days: int = 3) -> Optional[MappingResult]:
        """
        在数据库中按天数范围搜索数据
        """
        try:
            # 生成日期范围
            today = datetime.now()
            date_list = []
            for i in range(1, days + 1):  # 从昨天开始搜索
                date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
                date_list.append(date)
            
            async with aiosqlite.connect(self.db_path) as db:
                # 构建查询条件
                placeholders = ','.join(['?' for _ in date_list])
                query = f'''
                    SELECT * FROM message_mappings 
                    WHERE date IN ({placeholders}) AND {field} = ?
                    ORDER BY tgmsgid DESC
                    LIMIT 1
                '''
                
                async with db.execute(query, (*date_list, value)) as cursor:
                    row = await cursor.fetchone()
                    
                    if row:
                        result = MappingResult({
                            'tgmsgid': row[0],
                            'from_id': row[1],
                            'to_id': row[2],
                            'msgid': row[3],
                            'telethonmsgid': row[4]
                        })
                        
                        # 将历史数据也加入缓存（可选优化）
                        with self.cache_lock:
                            # 检查是否已存在，避免重复
                            exists = any(item.tgmsgid == result.tgmsgid for item in self.memory_cache)
                            if not exists:
                                self.memory_cache.append(result)
                                # 重新排序
                                self.memory_cache.sort(key=lambda x: x.tgmsgid, reverse=True)
                        
                        return result
        except Exception as e:
            logger.error(f"❌ 数据库搜索失败: {e}")
        
        return None

    async def get_tg_id_by_wx_user(self, from_qq_id: str, days: int = 3) -> List[int]:
        """
        根据微信用户ID查找相关的TG消息ID列表
        返回tgmsgid整数列表，按tgmsgid降序排序
        """
        result = []
        
        # 1. 检查内存缓存
        with self.cache_lock:
            for item in self.memory_cache:
                if item.from_id == from_qq_id:
                    if item.tgmsgid not in result:
                        result.append(item.tgmsgid)
        
        # 2. 检查数据库
        try:
            today = datetime.now()
            date_list = []
            for i in range(1, days + 1):
                date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
                date_list.append(date)
            
            async with aiosqlite.connect(self.db_path) as db:
                placeholders = ','.join(['?' for _ in date_list])
                query = f'''
                    SELECT DISTINCT tgmsgid FROM message_mappings 
                    WHERE date IN ({placeholders}) AND from_id = ?
                    ORDER BY tgmsgid DESC
                '''
                
                async with db.execute(query, (*date_list, from_qq_id)) as cursor:
                    rows = await cursor.fetchall()
                    
                    for row in rows:
                        tg_id = row[0]
                        if tg_id not in result:
                            result.append(tg_id)
        except Exception as e:
            logger.error(f"❌ 查询用户TG消息失败: {e}")
        
        # 确保结果按tgmsgid降序排序
        result.sort(reverse=True)
        return result
    
    async def _start_cleanup_scheduler(self):
        """启动定期清理调度器"""
        if not self.cleanup_enabled:
            return
        
        try:
            # 等待数据库初始化完成
            await asyncio.sleep(5)
            
            # 启动时执行一次清理（可选）
            await self._perform_scheduled_cleanup(startup=True)
            
            # 启动定期清理任务
            self.cleanup_task = asyncio.create_task(self._cleanup_scheduler_loop())
            logger.info(f"⏰ 定期清理任务已启动，每天{self.cleanup_hour}:00执行，保留{self.cleanup_days_to_keep}天数据")
            
        except Exception as e:
            logger.error(f"❌ 启动定期清理任务失败: {e}")
    
    async def _cleanup_scheduler_loop(self):
        """定期清理循环任务"""
        while self.cleanup_enabled:
            try:
                # 计算到下次清理时间的等待时间
                now = datetime.now()
                next_cleanup = now.replace(hour=self.cleanup_hour, minute=0, second=0, microsecond=0)
                
                # 如果今天的清理时间已过，设置为明天
                if next_cleanup <= now:
                    next_cleanup = next_cleanup + timedelta(days=1)
                
                wait_seconds = (next_cleanup - now).total_seconds()
                logger.debug(f"⏰ 下次清理时间: {next_cleanup}, 等待 {wait_seconds/3600:.1f} 小时")
                
                # 等待到清理时间
                await asyncio.sleep(wait_seconds)
                
                # 执行清理
                if self.cleanup_enabled:
                    await self._perform_scheduled_cleanup()
                    
            except asyncio.CancelledError:
                logger.info("🔴 定期清理任务被取消")
                break
            except Exception as e:
                logger.error(f"❌ 定期清理循环出错: {e}")
                # 出错后等待1小时再重试
                await asyncio.sleep(3600)
    
    async def _perform_scheduled_cleanup(self, startup: bool = False):
        """执行定期清理"""
        try:
            action = "启动清理" if startup else "定期清理"
            
            # 获取清理前统计
            stats_before = await self.get_stats()
            start_time = datetime.now()
            
            # 执行清理
            deleted_count = await self.cleanup_old_data(self.cleanup_days_to_keep)
            
            # 计算耗时
            duration = (datetime.now() - start_time).total_seconds()
            
            # 获取清理后统计
            stats_after = await self.get_stats()
            
            # 记录清理结果
            logger.info(f"🗑️ {action}完成 - 删除: {deleted_count}条, 清理前: {stats_before['total_mappings']}条, "
                       f"清理后: {stats_after['total_mappings']}条, 耗时: {duration:.2f}秒")
            
        except Exception as e:
            logger.error(f"❌ 执行定期清理失败: {e}")

    def configure_cleanup(self, enabled: bool = True, days_to_keep: int = 7, cleanup_hour: int = 2):
        """
        配置定期清理参数
        
        Args:
            enabled: 是否启用定期清理
            days_to_keep: 保留数据的天数
            cleanup_hour: 每天清理的小时数 (0-23)
        """
        self.cleanup_enabled = enabled
        self.cleanup_days_to_keep = max(1, days_to_keep)  # 至少保留1天
        self.cleanup_hour = max(0, min(23, cleanup_hour))  # 限制在0-23小时
        
        logger.info(f"🔄 清理配置已更新 - 启用: {enabled}, 保留天数: {self.cleanup_days_to_keep}, 清理时间: {self.cleanup_hour}:00")
    
    async def stop_cleanup_scheduler(self):
        """停止定期清理任务"""
        self.cleanup_enabled = False
        if self.cleanup_task and not self.cleanup_task.done():
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("🔴 定期清理任务已停止")
    
    async def trigger_manual_cleanup(self) -> dict:
        """手动触发一次清理"""
        try:
            stats_before = await self.get_stats()
            start_time = datetime.now()
            
            deleted_count = await self.cleanup_old_data(self.cleanup_days_to_keep)
            
            duration = (datetime.now() - start_time).total_seconds()
            stats_after = await self.get_stats()
            
            result = {
                'success': True,
                'deleted_count': deleted_count,
                'before_total': stats_before['total_mappings'],
                'after_total': stats_after['total_mappings'],
                'duration': duration
            }
            
            logger.info(f"🗑️ 手动清理完成 - {result}")
            
            # 重新加载今日缓存
            await self._load_today_to_cache()
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 手动清理失败: {e}")
            return {'success': False, 'error': str(e)}

    async def get_stats(self) -> dict:
        """
        获取映射统计信息
        """
        cache_count = 0
        total_mappings = 0
        
        # 统计内存缓存
        with self.cache_lock:
            cache_count = len(self.memory_cache)
        
        # 统计数据库中的数据
        try:
            today = datetime.now()
            date_list = []
            for i in range(7):  # 统计最近7天
                date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
                date_list.append(date)
            
            async with aiosqlite.connect(self.db_path) as db:
                placeholders = ','.join(['?' for _ in date_list])
                query = f'SELECT COUNT(*) FROM message_mappings WHERE date IN ({placeholders})'
                
                async with db.execute(query, date_list) as cursor:
                    row = await cursor.fetchone()
                    total_mappings = row[0] if row else 0
        except Exception as e:
            logger.error(f"❌ 获取统计信息失败: {e}")
        
        return {
            'total_mappings': total_mappings,
            'cache_count': cache_count,
            'cache_hit_rate': f"{cache_count}/{total_mappings}" if total_mappings > 0 else "0/0",
            'cleanup_enabled': self.cleanup_enabled,
            'cleanup_days_to_keep': self.cleanup_days_to_keep,
            'cleanup_hour': self.cleanup_hour,
            'cleanup_task_running': self.cleanup_task is not None and not self.cleanup_task.done()
        }

    async def clear_old_cache(self, days_to_keep: int = 1):
        """
        清理旧的缓存数据
        """
        if days_to_keep == 1:
            await self._load_today_to_cache()

    async def cleanup_old_data(self, days_to_keep: int = 30):
        """
        清理数据库中的旧数据（可选功能）
        """
        try:
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime("%Y-%m-%d")
            
            async with aiosqlite.connect(self.db_path) as db:
                result = await db.execute(
                    'DELETE FROM message_mappings WHERE date < ?',
                    (cutoff_date,)
                )
                await db.commit()
                
                deleted_count = result.rowcount
                logger.info(f"🗑️ 清理了 {deleted_count} 条旧数据（{days_to_keep}天前）")
                
                return deleted_count
        except Exception as e:
            logger.error(f"❌ 清理旧数据失败: {e}")
            return 0

# 创建映射管理器实例
msgid_mapping = MappingManager()