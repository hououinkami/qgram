import json
import logging
import os
from typing import Optional
from threading import Lock

logger = logging.getLogger(__name__)

class UserManager:
    """用户信息管理器 - 单例模式"""
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        self._user_id: Optional[int] = None
        self._username: Optional[str] = None
        self._file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "user.json")
        self._ensure_data_dir()
        
        # 启动时加载用户信息
        self._load_user_info()
    
    def _ensure_data_dir(self):
        """确保数据目录存在，并且文件路径正确"""
        dir_path = os.path.dirname(self._file_path)
        os.makedirs(dir_path, exist_ok=True)
        
        # 确保文件存在（如果不存在就创建空的 JSON 文件）
        if not os.path.exists(self._file_path):
            with open(self._file_path, 'w', encoding='utf-8') as f:
                f.write('{}')
    
    def _load_user_info(self):
        """从文件加载用户信息"""
        try:
            if os.path.exists(self._file_path):
                with open(self._file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self._user_id = data.get('user_id')
                    self._username = data.get('username')
                    if self._user_id:
                        logger.info(f"✅ 已加载用户信息: ID={self._user_id}, Username={self._username}")
        except Exception as e:
            logger.warning(f"⚠️ 加载用户信息失败: {e}")
    
    def _save_user_info(self):
        """保存用户信息到文件"""
        try:
            data = {
                'user_id': self._user_id,
                'username': self._username,
                'updated_at': str(int(__import__('time').time()))
            }
            with open(self._file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.debug(f"💾 用户信息已保存到 {self._file_path}")
        except Exception as e:
            logger.error(f"❌ 保存用户信息失败: {e}")
    
    def set_user_info(self, user_id: int, username: str = None):
        """设置用户信息"""
        with self._lock:
            self._user_id = user_id
            self._username = username
            self._save_user_info()
            logger.info(f"✅ 用户信息已更新: ID={user_id}, Username={username}")
    
    def get_user_id(self) -> Optional[int]:
        """获取用户ID"""
        return self._user_id
    
    def get_username(self) -> Optional[str]:
        """获取用户名"""
        return self._username
    
    def is_user_set(self) -> bool:
        """检查用户信息是否已设置"""
        return self._user_id is not None
    
    def clear_user_info(self):
        """清空用户信息"""
        with self._lock:
            self._user_id = None
            self._username = None
            if os.path.exists(self._file_path):
                os.remove(self._file_path)
            logger.info("🗑️ 用户信息已清空")

# 全局实例
_user_manager = UserManager()

# 便捷函数
def set_user_info(user_id: int, username: str = None):
    """设置用户信息"""
    _user_manager.set_user_info(user_id, username)

def get_user_id() -> Optional[int]:
    """获取当前用户ID"""
    return _user_manager.get_user_id()

def get_username() -> Optional[str]:
    """获取当前用户名"""
    return _user_manager.get_username()

def is_user_set() -> bool:
    """检查用户信息是否已设置"""
    return _user_manager.is_user_set()

def clear_user_info():
    """清空用户信息"""
    _user_manager.clear_user_info()
