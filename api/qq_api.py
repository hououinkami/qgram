import asyncio
import logging
from typing import Any, Dict, Optional, Union

import aiohttp
import requests

import config

logger = logging.getLogger(__name__)

class QQAPIPaths:
    """QQAPI路径配置"""
    
    # 消息
    SEND_PRIVATE = "/send_private_msg"
    SEND_GROUP = "/send_group_msg"
    REVOKE = "/delete_msg"
    GET_FORWARD = "/get_forward_msg"
    GET_MEMBER_INFO= "/get_group_member_info"
    GET_FRIEND_INFO="/get_stranger_info"
    GET_GROUP_INFO="/get_group_info"
    DELETE_FRIEND="/delete_friend"
    GET_STATUS="/get_status"
    
    @classmethod
    def get_path(cls, name: str) -> Optional[str]:
        """根据名称获取路径"""
        # 统一转换为大写
        attr_name = name.upper()
        path = getattr(cls, attr_name, None)
        
        if path is None:
            available_paths = cls.list_paths()
            logger.warning(f"未找到API路径 '{name}'，可用路径: {available_paths}")
        
        return path
    
    @classmethod
    def list_paths(cls) -> list:
        """列出所有可用的路径名称"""
        return [attr for attr in dir(cls) 
                if not attr.startswith('_') 
                and not callable(getattr(cls, attr))
                and isinstance(getattr(cls, attr), str)]
    
    @classmethod
    def get_path_mapping(cls) -> Dict[str, str]:
        """获取所有路径映射"""
        return {attr.lower(): getattr(cls, attr) 
                for attr in cls.list_paths()}

def _resolve_api_path(api_path: str) -> Optional[str]:
  """解析API路径"""
  if api_path.startswith('/'):
      # 已经是完整路径
      return api_path
  else:
      # 需要从配置中获取路径
      resolved_path = QQAPIPaths.get_path(api_path)
      if resolved_path is None:
          available = QQAPIPaths.list_paths()
          logger.error(f"无效的API路径名称: '{api_path}'，可用名称: {[p.lower() for p in available]}")
      return resolved_path

async def qq_api(
    api_path: str, 
    body: Optional[Dict[str, Any]] = None, 
    query_params: Optional[Dict[str, Any]] = None,
    timeout: int = 30
    ) -> Union[Dict[str, Any], bool]:
    """
    异步微信API调用函数
    
    Args:
        api_path: API路径或路径名称（如 'get_profile' 或 '/User/GetContractProfile'）
        body: 请求体数据
        query_params: URL查询参数
        timeout: 超时时间（秒）
    
    Returns:
        成功时返回响应JSON，失败时返回False
    """
    # 解析API路径
    resolved_path = _resolve_api_path(api_path)
    if resolved_path is None:
        return False
    
    api_url = f"{config.NAPCAT_API_URL}{resolved_path}"
    
    try:
        # 设置超时时间
        client_timeout = aiohttp.ClientTimeout(total=timeout)
        
        async with aiohttp.ClientSession(timeout=client_timeout) as session:
            async with session.post(
                url=api_url,
                json=body,
                params=query_params
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    response_text = await response.text()
                    logger.error(f"API调用失败 [{api_path}]，状态码: {response.status}, 响应: {response_text}")
                    return False
                    
    except asyncio.TimeoutError:
        logger.error(f"API调用超时 [{api_path}]: {api_url}")
        return False
    except aiohttp.ClientError as e:
        logger.error(f"HTTP客户端错误 [{api_path}]: {e}")
        return False
    except Exception as e:
        logger.error(f"调用微信API时出错 [{api_path}]: {e}")
        return False

def wechat_api_sync(
    api_path: str, 
    body: Optional[Dict[str, Any]] = None, 
    query_params: Optional[Dict[str, Any]] = None,
    timeout: int = 30
    ) -> Union[Dict[str, Any], bool]:
    """
    同步微信API调用函数
    
    Args:
        api_path: API路径或路径名称
        body: 请求体数据
        query_params: URL查询参数
        timeout: 超时时间（秒）
    
    Returns:
        成功时返回响应JSON，失败时返回False
    """
    # 解析API路径
    resolved_path = _resolve_api_path(api_path)
    if resolved_path is None:
        return False
    
    api_url = f"{config.NAPCAT_API_URL}{resolved_path}"
    
    try:
        response = requests.post(
            url=api_url,
            json=body,
            params=query_params,
            timeout=timeout
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"API调用失败 [{api_path}]，状态码: {response.status_code}, 响应: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        logger.error(f"API调用超时 [{api_path}]: {api_url}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP请求错误 [{api_path}]: {e}")
        return False
    except Exception as e:
        logger.error(f"调用微信API时出错 [{api_path}]: {e}")
        return False
