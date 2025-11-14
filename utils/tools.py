import asyncio
import base64
import logging
import os
import re
import requests
import tempfile
import time
import urllib.parse
import warnings
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional, Union, Tuple

import aiohttp
import aiofiles
from PIL import Image

from config import locale
from service.telethon_client import get_client
from utils.message_formatter import escape_html_chars

logger = logging.getLogger(__name__)

async def get_file_from_url(
    url: str, 
    file_type: str = "auto",
    save_file: bool = False, 
    save_dir: str = "/app/download"
) -> Union[Tuple[Optional[BytesIO], str], Tuple[Optional[str], str]]:
    """从URL下载任意类型的文件并处理为BytesIO对象或保存为文件"""

    # 根据file_type设置默认文件名
    default_names = {
        "photo": locale.type(3),
        "document": locale.type(6), 
        "video": locale.type(43),
        "sticker": locale.type(47),
        "audio": locale.type(34),
        "auto": locale.type(6)
    }
    default_filename = default_names.get(file_type) or file_type or locale.type(6)

    try:
        # ✅ 增强请求头，特别针对QQ文件
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # ✅ 如果是QQ域名，添加特殊处理
        if 'qlogo.cn' in url or 'ftn.qq.com' in url or 'gzc-download.ftn.qq.com' in url:
            headers['Referer'] = 'https://web.qun.qq.com/'
            logger.debug(f"检测到QQ文件链接，添加Referer头")
        
        # ✅ 增加超时时间和重试机制
        timeout = aiohttp.ClientTimeout(total=60, connect=10)  # 总超时60秒
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=headers
        ) as session:
            
            # ✅ 添加重试机制
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    logger.debug(f"尝试下载文件 (第{attempt+1}/{max_retries}次): {url}")
                    
                    async with session.get(
                        url, 
                        allow_redirects=True,  # ✅ 允许重定向
                        max_redirects=10       # ✅ 最多10次重定向
                    ) as response:
                        
                        # ✅ 详细的状态码检查
                        logger.debug(f"响应状态码: {response.status}")
                        logger.debug(f"响应头: {dict(response.headers)}")
                        
                        if response.status == 403:
                            logger.error("403 Forbidden - 可能需要登录或权限")
                            return None, default_filename
                        elif response.status == 404:
                            logger.error("404 Not Found - 文件不存在或链接已失效")
                            return None, default_filename
                        elif response.status >= 400:
                            logger.error(f"HTTP错误: {response.status} - {response.reason}")
                            if attempt == max_retries - 1:  # 最后一次尝试
                                return None, default_filename
                            continue
                        
                        response.raise_for_status()
                        
                        # ✅ 检查Content-Type
                        content_type = response.headers.get('Content-Type', '')
                        content_length = response.headers.get('Content-Length', '0')
                        logger.debug(f"Content-Type: {content_type}")
                        logger.debug(f"Content-Length: {content_length}")
                        
                        # ✅ 获取文件名
                        filename = get_filename_from_response(response, url, default_filename)
                        logger.debug(f"解析到的文件名: {filename}")
                        
                        # ✅ 如果需要保存文件，创建完整路径
                        file_path = None
                        if save_file:
                            os.makedirs(save_dir, exist_ok=True)  # 确保目录存在
                            file_path = os.path.join(save_dir, filename)
                            logger.debug(f"文件将保存到: {file_path}")
                        
                        # ✅ 分块下载大文件
                        file_data = BytesIO() if not save_file else None
                        downloaded_size = 0
                        chunk_size = 8192  # 8KB chunks
                        
                        if save_file:
                            # 保存文件模式：直接写入文件
                            with open(file_path, 'wb') as f:
                                async for chunk in response.content.iter_chunked(chunk_size):
                                    if chunk:
                                        f.write(chunk)
                                        downloaded_size += len(chunk)
                        else:
                            # BytesIO模式：写入内存
                            async for chunk in response.content.iter_chunked(chunk_size):
                                if chunk:
                                    file_data.write(chunk)
                                    downloaded_size += len(chunk)
                        
                        logger.debug(f"下载完成，文件大小: {downloaded_size} bytes")
                        
                        if downloaded_size == 0:
                            logger.warning("下载的文件数据为空")
                            return None, filename
                        
                        # ✅ 根据模式返回不同结果
                        if save_file:
                            return file_path, filename
                        else:
                            # ✅ 重置BytesIO指针到开头
                            file_data.seek(0)
                            return file_data, filename
                            
                except aiohttp.ClientError as e:
                    logger.warning(f"第{attempt+1}次下载失败: {e}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(1)  # 重试前等待1秒
                    
        return None, default_filename
        
    except aiohttp.ClientError as e:
        logger.error(f"网络请求失败: {e}")
        return None, default_filename
    except asyncio.TimeoutError as e:
        logger.error(f"下载超时: {e}")
        return None, default_filename
    except Exception as e:
        logger.error(f"下载文件失败: {e}", exc_info=True)
        return None, default_filename

def get_filename_from_response(response, url: str, default_filename: str) -> str:
    """从响应中获取文件名"""
    try:
        # ✅ 优先从Content-Disposition获取
        content_disposition = response.headers.get('Content-Disposition', '')
        if content_disposition:
            # 支持多种编码格式
            patterns = [
                r'filename\*=UTF-8\'\'([^;]+)',  # RFC 5987
                r'filename\*=([^;]+)',
                r'filename="([^"]+)"',
                r'filename=([^;]+)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, content_disposition, re.IGNORECASE)
                if match:
                    filename = match.group(1).strip()
                    # URL解码
                    try:
                        filename = urllib.parse.unquote(filename)
                        if filename and filename != 'undefined':
                            logger.debug(f"从Content-Disposition获取文件名: {filename}")
                            return filename
                    except:
                        pass
        
        # ✅ 从URL参数获取文件名
        if '?fname=' in url or '&fname=' in url:
            parsed_url = urllib.parse.urlparse(url)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            if 'fname' in query_params:
                fname = query_params['fname'][0]
                if fname:
                    logger.debug(f"从URL参数获取文件名: {fname}")
                    return fname
        
        # ✅ 从URL路径获取文件名
        parsed_url = urllib.parse.urlparse(url)
        path = urllib.parse.unquote(parsed_url.path)
        filename = os.path.basename(path)
        
        if filename and '.' in filename:
            logger.debug(f"从URL路径获取文件名: {filename}")
            return filename
        
        # ✅ 根据Content-Type推断扩展名
        content_type = response.headers.get('Content-Type', '').lower()
        extension = ''
        
        if 'pdf' in content_type:
            extension = '.pdf'
        elif 'image/jpeg' in content_type:
            extension = '.jpg'
        elif 'image/png' in content_type:
            extension = '.png'
        elif 'image/gif' in content_type:
            extension = '.gif'
        elif 'video/mp4' in content_type:
            extension = '.mp4'
        elif 'audio' in content_type:
            extension = '.mp3'
        
        if extension:
            return f"{default_filename}{extension}"
        
        return default_filename
        
    except Exception as e:
        logger.warning(f"解析文件名失败: {e}")
        return default_filename

def parse_time_without_seconds(time_str):
    """解析时间并忽略秒数"""
    time_str = re.sub(r'(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}):\d{1,2}', r'\1', time_str)
    
    try:
        return datetime.strptime(time_str, "%Y-%m-%d %H:%M")
    except ValueError:
        logger.warning(f"无法解析时间格式: {time_str}，使用当前时间")
        return datetime.now()

async def get_telegram_file(
    file_id: str = None,
    file_obj = None,
    chat_id = None,
    message_id = None,
    size_threshold_mb: int = 20,
    force_method: Optional[str] = None,
    save_file: bool = False,
    save_dir: str = "/app/download",
    filename: str = None
) -> Union[str, bool]:
    """
    统一的Telegram文件获取接口
    
    Args:
        file_id: 文件ID（直接通过Bot API下载）
        file_obj: API的video对象（用于API下载）
        chat_id: 聊天ID（用于Telethon下载）
        message_id: 消息ID（用于Telethon下载）
        size_threshold_mb: 文件大小阈值(MB)
        force_method: 强制使用的方法 ('api' 或 'telethon')
        save_file: 是否保存文件
        save_dir: 文件保存目录（仅当output_type="path"时使用）
    
    Returns:
        str: Base64字符串或文件路径，失败返回False
    """
    try:
        # 参数验证
        if not any([file_id, file_obj, (chat_id and message_id)]):
            raise ValueError("必须提供 file_id 或 file_obj 或 (chat_id + message_id)")
        
        # 根据输出类型调用相应函数
        if not save_file:
            return await telegram_file_to_base64(
                file_id=file_id,
                file_obj=file_obj,
                chat_id=chat_id,
                message_id=message_id,
                size_threshold_mb=size_threshold_mb,
                force_method=force_method
            )
        
        else:            
            return await telegram_file_to_path(
                file_id=file_id,
                file_obj=file_obj,
                chat_id=chat_id,
                message_id=message_id,
                size_threshold_mb=size_threshold_mb,
                force_method=force_method,
                save_dir=save_dir,
                filename=filename
            )
            
    except Exception as e:
        logger.error(f"❌ get_telegram_file 失败: {e}")
        return False

async def telegram_file_to_base64(
        file_id: str = None,
        file_obj=None,
        chat_id=None, 
        message_id=None,
        size_threshold_mb: int = 20,
        force_method: Optional[str] = None
    ):
    """
    下载Telegram文件并转换为 Base64 格式
    
    Args:
        file_id: 文件ID（直接通过Bot API下载）
        file_obj: API的video对象（用于API下载）
        chat_id: 聊天ID（用于Telethon下载）
        message_id: 消息ID（用于Telethon下载）
        size_threshold_mb: 文件大小阈值(MB)，超过此大小使用telethon下载
        force_method: 强制使用的方法 ('api' 或 'telethon')
    
    Returns:
        str: Base64编码的文件内容，失败返回False
    """
    try:        
        # 参数验证
        if not any([file_id, file_obj, (chat_id and message_id)]):
            raise ValueError("必须提供 file_id 或 file_obj 或 (chat_id + message_id)")
        
        # 如果有file_id，优先使用（最简单的方式）
        if file_id:
            return await _download_via_api(file_id)

        # 如果强制指定方法
        if force_method == 'api':
            if not file_obj:
                raise ValueError("使用API方法必须提供file_obj")
            return await _download_via_api(file_obj.file_id)
        elif force_method == 'telethon':
            if not (chat_id and message_id):
                raise ValueError("使用Telethon方法必须提供chat_id和message_id")
            return await _download_via_telethon(chat_id, message_id)
        
        # 智能选择逻辑
        if file_obj:
            try:
                # 从video对象获取文件大小
                file_size = getattr(file_obj, 'file_size', 0)
                file_size_mb = file_size / (1024 * 1024)
                
                # 根据文件大小选择下载方式
                if file_size_mb < size_threshold_mb:
                    logger.info(f"🚀 使用Bot API下载 (< {size_threshold_mb}MB)")
                    try:
                        return await _download_via_api(file_obj.file_id)
                    except Exception as api_error:
                        logger.warning(f"⚠️ Bot API下载失败: {api_error}")
                        if chat_id and message_id:
                            return await _download_via_telethon(chat_id, message_id)
                        else:
                            raise api_error
                else:
                    logger.info(f"🔄 使用Telethon下载 (≥ {size_threshold_mb}MB)")
                    if chat_id and message_id:
                        return await _download_via_telethon(chat_id, message_id)
                    else:
                        return await _download_via_api(file_obj.file_id)
                        
            except Exception as e:
                logger.warning(f"⚠️ 处理file_obj失败: {e}")
                if chat_id and message_id:
                    return await _download_via_telethon(chat_id, message_id)
                else:
                    raise e
        else:
            # 只有Telethon参数
            logger.info("🔄 使用Telethon下载")
            return await _download_via_telethon(chat_id, message_id)
            
    except Exception as e:
        logger.error(f"❌ 获取文件并转换为Base64失败: {e}")
        return False

async def _download_via_api(file_id):
    """通过API下载文件"""
    from api.telegram_sender import telegram_sender
    
    start_time = time.time()
    
    # 获取文件（使用video对象的file_id）
    file = await telegram_sender.get_file(file_id)
    
    # 下载文件到内存
    file_content = await file.download_as_bytearray()
    
    # 转换为Base64
    file_base64 = base64.b64encode(file_content).decode('utf-8')
    
    download_time = time.time() - start_time
    file_size_mb = len(file_content) / (1024 * 1024)
    logger.info(f"✅ Bot API下载完成，大小: {file_size_mb:.2f}MB，耗时: {download_time:.2f}s")
    
    return file_base64

async def _download_via_telethon(chat_id, message_id):
    """通过Telethon下载文件"""   
    start_time = time.time()
    
    client = get_client()
    
    # 获取消息
    message = await client.get_messages(chat_id, ids=message_id)
    if not message or not message.media:
        raise ValueError(f"消息 {message_id} 不存在或不包含媒体文件")
    
    # 下载文件到内存
    file_content = await client.download_media(message, file=bytes)
    
    if not file_content:
        raise RuntimeError("Telethon下载失败，文件内容为空")
    
    # 转换为Base64
    file_base64 = base64.b64encode(file_content).decode('utf-8')
    
    download_time = time.time() - start_time
    file_size_mb = len(file_content) / (1024 * 1024)
    logger.info(f"✅ Telethon下载完成，大小: {file_size_mb:.2f}MB，耗时: {download_time:.2f}s")
    
    return file_base64

async def telegram_file_to_path(
    file_id: str = None,
    file_obj = None,
    chat_id = None,
    message_id = None,
    size_threshold_mb: int = 20,
    force_method: Optional[str] = None,
    save_dir: str = "/app/download",
    filename: str = None
):
    """
    通过智能选择下载Telegram文件到指定目录
    
    Args:
        file_id: 文件ID（直接通过Bot API下载）
        file_obj: API的文件对象（用于API下载）
        chat_id: 聊天ID（用于Telethon下载）
        message_id: 消息ID（用于Telethon下载）
        size_threshold_mb: 文件大小阈值(MB)，超过此大小使用telethon下载
        force_method: 强制使用的方法 ('api' 或 'telethon')
        save_dir: 保存目录
        filename: 自定义文件名（可选）
    
    Returns:
        str: 文件路径，失败返回False
    """
    try:
        # 参数验证
        if not any([file_id, file_obj, (chat_id and message_id)]):
            raise ValueError("必须提供 file_id 或 file_obj 或 (chat_id + message_id)")
        
        # 确保保存目录存在
        os.makedirs(save_dir, exist_ok=True)
        
        # 如果有file_id，优先使用（最简单的方式）
        if file_id:
            return await _download_to_path_via_api(file_id, save_dir, filename)

        # 如果强制指定方法
        if force_method == 'api':
            if not file_obj:
                raise ValueError("使用API方法必须提供file_obj")
            return await _download_to_path_via_api(file_obj.file_id, save_dir, filename)
        elif force_method == 'telethon':
            if not (chat_id and message_id):
                raise ValueError("使用Telethon方法必须提供chat_id和message_id")
            return await _download_to_path_via_telethon(chat_id, message_id, save_dir, filename)
        
        # 智能选择逻辑
        if file_obj:
            try:
                # 从文件对象获取文件大小
                file_size = getattr(file_obj, 'file_size', 0)
                file_size_mb = file_size / (1024 * 1024)
                
                # 根据文件大小选择下载方式
                if file_size_mb < size_threshold_mb:
                    logger.info(f"🚀 使用Bot API下载到文件 (< {size_threshold_mb}MB)")
                    try:
                        return await _download_to_path_via_api(file_obj.file_id, save_dir, filename)
                    except Exception as api_error:
                        logger.warning(f"⚠️ Bot API下载失败: {api_error}")
                        if chat_id and message_id:
                            return await _download_to_path_via_telethon(chat_id, message_id, save_dir, filename)
                        else:
                            raise api_error
                else:
                    logger.info(f"🔄 使用Telethon下载到文件 (≥ {size_threshold_mb}MB)")
                    if chat_id and message_id:
                        return await _download_to_path_via_telethon(chat_id, message_id, save_dir, filename)
                    else:
                        return await _download_to_path_via_api(file_obj.file_id, save_dir, filename)
                        
            except Exception as e:
                logger.warning(f"⚠️ 处理file_obj失败: {e}")
                if chat_id and message_id:
                    return await _download_to_path_via_telethon(chat_id, message_id, save_dir, filename)
                else:
                    raise e
        else:
            # 只有Telethon参数
            logger.info("🔄 使用Telethon下载到文件")
            return await _download_to_path_via_telethon(chat_id, message_id, save_dir, filename)
            
    except Exception as e:
        logger.error(f"❌ 下载Telegram文件到路径失败: {e}")
        return False

async def _download_to_path_via_api(file_id: str, save_dir: str, filename: str = None):
    """通过API下载文件到指定路径"""
    from api.telegram_sender import telegram_sender
    
    start_time = time.time()
    
    try:
        # 获取文件信息
        file = await telegram_sender.get_file(file_id)
        
        # 生成文件名
        if filename:
            final_filename = filename
        else:
            original_path = file.file_path
            if original_path:
                final_filename = os.path.basename(original_path)
            else:
                final_filename = f"{file_id}"
        
        # 构建保存路径
        save_path = os.path.join(save_dir, final_filename)
        
        # 下载文件到指定路径
        await file.download_to_drive(save_path)
        
        download_time = time.time() - start_time
        file_size_mb = os.path.getsize(save_path) / (1024 * 1024)
        logger.info(f"✅ Bot API下载到文件完成，大小: {file_size_mb:.2f}MB，耗时: {download_time:.2f}s")
        logger.info(f"📁 文件已保存到: {save_path}")
        
        return save_path
        
    except Exception as e:
        logger.error(f"Bot API下载到文件失败: {e}")
        raise e

async def _download_to_path_via_telethon(chat_id, message_id, save_dir: str, filename: str = None):
    """通过Telethon下载文件到指定路径"""
    start_time = time.time()
    
    try:
        client = get_client()
        
        # 获取消息
        message = await client.get_messages(chat_id, ids=message_id)
        if not message or not message.media:
            raise ValueError(f"消息 {message_id} 不存在或不包含媒体文件")
        
        # 生成文件名
        if filename:
            final_filename = filename
        else:
            # 尝试从消息中获取文件名
            media = message.media
            if hasattr(media, 'document') and media.document:
                # 文档类型
                for attr in media.document.attributes:
                    if hasattr(attr, 'file_name') and attr.file_name:
                        final_filename = attr.file_name
                        break
                else:
                    final_filename = f"document_{message_id}"
            elif hasattr(media, 'photo'):
                # 图片类型
                final_filename = f"photo_{message_id}.jpg"
            else:
                # 其他类型
                final_filename = f"media_{message_id}"
        
        # 构建保存路径
        save_path = os.path.join(save_dir, final_filename)
        
        # 下载文件到指定路径
        await client.download_media(message, file=save_path)
        
        if not os.path.exists(save_path):
            raise RuntimeError("Telethon下载失败，文件未保存")
        
        download_time = time.time() - start_time
        file_size_mb = os.path.getsize(save_path) / (1024 * 1024)
        logger.info(f"✅ Telethon下载到文件完成，大小: {file_size_mb:.2f}MB，耗时: {download_time:.2f}s")
        logger.info(f"📁 文件已保存到: {save_path}")
        
        return save_path
        
    except Exception as e:
        logger.error(f"Telethon下载到文件失败: {e}")
        raise e

def local_file_to_base64(file_path: str) -> str:
    """将本地文件转换为base64编码"""
    try:
        if not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            return None
            
        with open(file_path, 'rb') as f:
            file_content = f.read()
            
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        return file_base64
        
    except Exception as e:
        logger.error(f"转换文件为base64失败 {file_path}: {e}")
        return None

async def local_file_to_bytesio(file_path: str) -> BytesIO | None:
    """将本地文件转换为BytesIO"""
    try:
        if not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            return None
            
        async with aiofiles.open(file_path, 'rb') as f:
            data = await f.read()
            file_buffer = BytesIO(data)
            file_buffer.seek(0)
            return file_buffer
        
    except Exception as e:
        logger.error(f"转换文件为BytesIO失败 {file_path}: {e}")
        return None

async def process_avatar_from_url(url: str, min_size: int = 512) -> Optional[BytesIO]:
    """从URL下载图片并处理为头像格式"""
    try:
        image_bytesio, _ = await get_file_from_url(url)
        if image_bytesio is None:
            return None
        
        loop = asyncio.get_event_loop()
        processed_image = await loop.run_in_executor(
            None,
            process_avatar_image,
            image_bytesio.getvalue(),
            min_size
        )
        
        return processed_image
        
    except Exception as e:
        logger.error(f"下载处理图片失败: {e}")
        return None

def process_avatar_image(image_data: bytes, min_size: int = 512) -> BytesIO:
    """处理头像图片内容"""
    try:
        img = Image.open(BytesIO(image_data))
        
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        width, height = img.size
        if width < min_size or height < min_size:
            ratio = max(min_size / width, min_size / height)
            new_width = int(width * ratio)
            new_height = int(height * ratio)
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        
        if img.width != img.height:
            size = min(img.size)
            left = (img.width - size) // 2
            top = (img.height - size) // 2
            img = img.crop((left, top, left + size, top + size))
        
        output = BytesIO()
        img.save(output, format='JPEG', quality=95)
        output.seek(0)
        return output
        
    except Exception as e:
        logger.error(f"图片处理失败: {e}")
        try:
            img = Image.open(BytesIO(image_data))
            if img.mode != 'RGB':
                img = img.convert('RGB')
            
            output = BytesIO()
            img.save(output, format='JPEG', quality=95)
            output.seek(0)
            return output
        except Exception:
            return BytesIO(image_data)

def multi_get(data, *keys, default=''):
    """从多个键中获取第一个有效值"""
    for key in keys:
        if '.' in key:
            # 处理嵌套键如 'ToUserName.string'
            parts = key.split('.')
            value = data
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part, {})
                else:
                    value = {}
                    break
            if value != {} and value is not None:
                return value
        else:
            value = data.get(key)
            if value is not None:
                return value
    return default