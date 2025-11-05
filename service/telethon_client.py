import logging
import asyncio
import threading
from typing import Optional

from telethon import TelegramClient

from utils.user_profile import set_user_info, get_user_id as get_stored_user_id

logger = logging.getLogger(__name__)

# 全局客户端实例
client_instance: Optional['TelethonClient'] = None

class CrossThreadTelegramClient:
    """跨线程安全的 Telegram 客户端包装器"""
    
    def __init__(self, telethon_client_instance):
        self.telethon_client = telethon_client_instance
    
    def _run_async(self, coro, timeout=30):
        """在主线程中运行异步操作"""
        return self.telethon_client.run_async(coro, timeout)
    
    # ==================== 异步接口（返回 awaitable） ====================
    async def send_message(self, entity, message, **kwargs):
        """发送消息（异步接口）"""
        async def _send():
            return await self.telethon_client.client.send_message(entity, message, **kwargs)
        
        # 如果在主线程中，直接返回协程
        current_thread_id = threading.get_ident()
        if current_thread_id == self.telethon_client._main_thread_id:
            return await _send()
        else:
            # 在其他线程中，同步执行
            return self._run_async(_send())
    
    async def get_messages(self, entity, limit=None, **kwargs):
        """获取消息（异步接口）"""
        async def _get_messages():
            return await self.telethon_client.client.get_messages(entity, limit=limit, **kwargs)
        
        current_thread_id = threading.get_ident()
        if current_thread_id == self.telethon_client._main_thread_id:
            return await _get_messages()
        else:
            return self._run_async(_get_messages())
    
    async def get_me(self):
        """获取当前用户（异步接口）"""
        async def _get_me():
            return await self.telethon_client.client.get_me()
        
        current_thread_id = threading.get_ident()
        if current_thread_id == self.telethon_client._main_thread_id:
            return await _get_me()
        else:
            return self._run_async(_get_me())
    
    async def get_entity(self, entity):
        """获取实体信息（异步接口）"""
        async def _get_entity():
            return await self.telethon_client.client.get_entity(entity)
        
        current_thread_id = threading.get_ident()
        if current_thread_id == self.telethon_client._main_thread_id:
            return await _get_entity()
        else:
            return self._run_async(_get_entity())
    
    async def download_media(self, message, file=None, **kwargs):
        """下载媒体文件（异步接口）"""
        async def _download():
            return await self.telethon_client.client.download_media(message, file, **kwargs)
        
        current_thread_id = threading.get_ident()
        if current_thread_id == self.telethon_client._main_thread_id:
            return await _download()
        else:
            return self._run_async(_download(), timeout=60)
    
    def iter_messages(self, entity, limit=None, **kwargs):
        """迭代消息（返回迭代器）"""
        return self.telethon_client.client.iter_messages(entity, limit=limit, **kwargs)
    
    def iter_dialogs(self, limit=None, **kwargs):
        """迭代对话（返回迭代器）"""
        return self.telethon_client.client.iter_dialogs(limit=limit, **kwargs)
    
    # ==================== 同步接口 ====================
    def send_message_sync(self, entity, message, timeout=30, **kwargs):
        """发送消息（同步接口）"""
        async def _send():
            return await self.telethon_client.client.send_message(entity, message, **kwargs)
        return self._run_async(_send(), timeout)
    
    def get_messages_sync(self, entity, limit=None, timeout=30, **kwargs):
        """获取消息（同步接口）"""
        async def _get_messages():
            return await self.telethon_client.client.get_messages(entity, limit=limit, **kwargs)
        return self._run_async(_get_messages(), timeout)
    
    def get_me_sync(self, timeout=30):
        """获取当前用户（同步接口）"""
        async def _get_me():
            return await self.telethon_client.client.get_me()
        return self._run_async(_get_me(), timeout)
    
    def get_entity_sync(self, entity, timeout=30):
        """获取实体信息（同步接口）"""
        async def _get_entity():
            return await self.telethon_client.client.get_entity(entity)
        return self._run_async(_get_entity(), timeout)
    
    # ==================== 属性代理 ====================
    @property
    def is_connected(self):
        """检查是否已连接"""
        return self.telethon_client.client.is_connected() if self.telethon_client.client else False
    
    def __getattr__(self, name):
        """代理其他属性到原始客户端"""
        if hasattr(self.telethon_client.client, name):
            attr = getattr(self.telethon_client.client, name)
            # 如果是方法，需要特殊处理
            if callable(attr):
                # 对于其他方法，如果在非主线程中调用，给出警告
                def wrapper(*args, **kwargs):
                    current_thread_id = threading.get_ident()
                    if current_thread_id != self.telethon_client._main_thread_id:
                        logger.warning(f"⚠️ 方法 {name} 可能不是线程安全的，建议使用提供的同步接口")
                    return attr(*args, **kwargs)
                return wrapper
            return attr
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
    
    async def __call__(self, request):
        """支持 client(request) 语法 - 异步版本"""
        async def _call():
            return await self.telethon_client.client(request)
        
        current_thread_id = threading.get_ident()
        if current_thread_id == self.telethon_client._main_thread_id:
            return await _call()
        else:
            return self._run_async(_call())
    
    async def get_dialogs(self, limit=None, **kwargs):
        """获取对话列表（异步接口）"""
        async def _get_dialogs():
            return await self.telethon_client.client.get_dialogs(limit=limit, **kwargs)
        
        current_thread_id = threading.get_ident()
        if current_thread_id == self.telethon_client._main_thread_id:
            return await _get_dialogs()
        else:
            return self._run_async(_get_dialogs())
    
    async def upload_file(self, file, **kwargs):
        """上传文件（异步接口）"""
        async def _upload():
            return await self.telethon_client.client.upload_file(file, **kwargs)
        
        current_thread_id = threading.get_ident()
        if current_thread_id == self.telethon_client._main_thread_id:
            return await _upload()
        else:
            return self._run_async(_upload())
    
    async def connect(self):
        """连接客户端（异步接口）"""
        async def _connect():
            return await self.telethon_client.client.connect()
        
        current_thread_id = threading.get_ident()
        if current_thread_id == self.telethon_client._main_thread_id:
            return await _connect()
        else:
            return self._run_async(_connect())

class TelethonClient:
    def __init__(self, session_path: str, api_id: int, api_hash: str, 
                device_model: str = "QGram"):
        self.session_path = session_path
        self.api_id = api_id
        self.api_hash = api_hash
        self.device_model = device_model
        self.client = None
        self.user_id = None
        self._is_initialized = False
        # 添加线程和事件循环跟踪
        self._main_loop = None
        self._main_thread_id = None
    
    async def initialize(self):
        """初始化Telethon客户端"""
        if self._is_initialized:
            return
            
        try:
            # 记录主线程和事件循环
            self._main_loop = asyncio.get_running_loop()
            self._main_thread_id = threading.get_ident()
            
            self.client = TelegramClient(
                self.session_path, 
                self.api_id, 
                self.api_hash, 
                device_model=self.device_model
            )
            await self.client.start()
            
            me = await self.client.get_me()
            self.user_id = me.id
            
            # 保存用户信息到用户管理器
            set_user_info(me.id, me.username)
            
            self._is_initialized = True
            logger.info(f"🔗 Telethon已连接 - 用户: {me.first_name} (ID: {self.user_id})")
            logger.info(f"💾 用户信息已保存到用户管理器")
            
            # 更新全局实例
            global client_instance
            client_instance = self
            
        except Exception as e:
            logger.error(f"❌ Telethon初始化失败: {e}")
            raise
    
    async def disconnect(self):
        """断开连接"""
        if self.client and self.client.is_connected():
            await self.client.disconnect()
        self._is_initialized = False
        logger.info("🔴 Telethon客户端已断开连接")
    
    def get_client(self):
        """获取Telethon客户端"""
        return self.client
    
    def get_user_id(self):
        """获取当前用户ID - 优先从内存获取，备用从持久化存储获取"""
        if self.user_id:
            return self.user_id
        # 如果内存中没有，尝试从用户管理器获取
        return get_stored_user_id()
    
    @property
    def is_initialized(self) -> bool:
        """检查是否已初始化"""
        return self._is_initialized
    
    def run_async(self, coro, timeout=30):
        """在主线程中运行异步操作"""
        current_thread_id = threading.get_ident()
        
        if current_thread_id == self._main_thread_id:
            # 在主线程中
            try:
                loop = asyncio.get_running_loop()
                # 如果已经在事件循环中，创建任务
                return loop.create_task(coro)
            except RuntimeError:
                # 没有运行的事件循环，直接运行
                return asyncio.run(coro)
        else:
            # 在其他线程中，提交到主线程执行
            if not self._main_loop or self._main_loop.is_closed():
                raise RuntimeError("主事件循环不可用")
            
            future = asyncio.run_coroutine_threadsafe(coro, self._main_loop)
            return future.result(timeout=timeout)

# ==================== 便捷函数 ====================
def get_user_id() -> Optional[int]:
    """获取当前用户ID - 多重备用策略"""
    global client_instance
    
    # 优先级1: 从客户端实例获取
    if client_instance and client_instance.user_id:
        return client_instance.user_id
    
    # 优先级2: 从用户管理器获取（持久化存储）
    stored_user_id = get_stored_user_id()
    if stored_user_id:
        return stored_user_id
    
    # 优先级3: 如果用户管理器中有信息，但客户端实例没有，同步一下
    if client_instance and stored_user_id:
        client_instance.user_id = stored_user_id
        return stored_user_id
    
    return None

def get_client():
    """获取跨线程安全的Telethon客户端"""
    global client_instance
    if client_instance and client_instance.client and client_instance.is_initialized:
        # 检查是否在主线程
        current_thread_id = threading.get_ident()
        if current_thread_id == client_instance._main_thread_id:
            # 在主线程中，返回原生客户端
            return client_instance.client
        else:
            # 在其他线程中，返回跨线程安全的包装器
            return CrossThreadTelegramClient(client_instance)
    return None

def get_client_instance() -> Optional[TelethonClient]:
    """获取客户端实例"""
    global client_instance
    return client_instance

def is_client_initialized() -> bool:
    """检查客户端是否已初始化"""
    global client_instance
    return client_instance.is_initialized if client_instance else False

async def create_client(session_path: str, api_id: int, api_hash: str, 
                        device_model: str = "QGram") -> TelethonClient:
    """创建并初始化客户端"""
    global client_instance
    client_instance = TelethonClient(session_path, api_id, api_hash, device_model)
    await client_instance.initialize()
    return client_instance
