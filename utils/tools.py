import asyncio
import base64
import logging
import mimetypes
import os
import re
import requests
import tempfile
import time
import warnings
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional, Union, BinaryIO, Tuple
from urllib.parse import urlparse

import aiohttp
from PIL import Image

from service.telethon_client import get_client
from utils.message_formatter import escape_html_chars

logger = logging.getLogger(__name__)

async def get_image_from_url(url: str) -> Tuple[Optional[BytesIO], str]:
    """从URL下载图片并处理为BytesIO对象"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                image_data = await response.read()
        
        return BytesIO(image_data), "[写真]"
        
    except Exception as e:
        logger.error(f"下载处理图片失败: {e}")
        return None, "[写真]"

async def get_file_from_url(url: str, default_filename: str = "[ファイル]") -> Tuple[Optional[BytesIO], str]:
    """从URL下载任意类型的文件并处理为BytesIO对象"""
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
                        
                        # ✅ 分块下载大文件
                        file_data = BytesIO()
                        downloaded_size = 0
                        chunk_size = 8192  # 8KB chunks
                        
                        async for chunk in response.content.iter_chunked(chunk_size):
                            if chunk:
                                file_data.write(chunk)
                                downloaded_size += len(chunk)
                        
                        logger.debug(f"下载完成，文件大小: {downloaded_size} bytes")
                        
                        if downloaded_size == 0:
                            logger.warning("下载的文件数据为空")
                            return None, filename
                        
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
            import re
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
                        import urllib.parse
                        filename = urllib.parse.unquote(filename)
                        if filename and filename != 'undefined':
                            logger.debug(f"从Content-Disposition获取文件名: {filename}")
                            return filename
                    except:
                        pass
        
        # ✅ 从URL参数获取文件名
        if '?fname=' in url or '&fname=' in url:
            import urllib.parse
            parsed_url = urllib.parse.urlparse(url)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            if 'fname' in query_params:
                fname = query_params['fname'][0]
                if fname:
                    logger.debug(f"从URL参数获取文件名: {fname}")
                    return fname
        
        # ✅ 从URL路径获取文件名
        import urllib.parse
        import os
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

async def download_file_to_bytesio(url: str, file_type: str = "auto") -> Tuple[Optional[BytesIO], str]:
    """根据文件类型下载文件"""
    
    # 根据file_type设置默认文件名
    default_names = {
        "photo": "[写真]",
        "document": "[ファイル]", 
        "video": "[動画]",
        "sticker": "[ステッカー]",
        "audio": "[音声]",
        "auto": "[ファイル]"
    }
    
    default_filename = default_names.get(file_type, "file")
    return await get_file_from_url(url, default_filename)

def parse_time_without_seconds(time_str):
    """解析时间并忽略秒数"""
    time_str = re.sub(r'(\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}):\d{1,2}', r'\1', time_str)
    
    try:
        return datetime.strptime(time_str, "%Y-%m-%d %H:%M")
    except ValueError:
        logger.warning(f"无法解析时间格式: {time_str}，使用当前时间")
        return datetime.now()

async def telegram_file_to_base64(video_obj=None,
                                chat_id=None, 
                                message_id=None,
                                size_threshold_mb: int = 20,
                                force_method: Optional[str] = None):
    """
    获取文件并转换为 Base64 格式
    
    Args:
        video_obj: API的video对象（用于API下载）
        chat_id: 聊天ID（用于Telethon下载）
        message_id: 消息ID（用于Telethon下载）
        size_threshold_mb: 文件大小阈值(MB)，超过此大小使用telethon下载
        force_method: 强制使用的方法 ('api' 或 'telethon')
    
    Returns:
        str: Base64编码的文件内容，失败返回False
    """
    try:        
        # 参数验证
        if not video_obj and not (chat_id and message_id):
            raise ValueError("必须提供 video_obj 或者 (chat_id + message_id)")
        
        # 如果强制指定方法
        if force_method == 'api':
            if not video_obj:
                raise ValueError("使用API方法必须提供video_obj")
            return await _download_via_api(video_obj)
        elif force_method == 'telethon':
            if not (chat_id and message_id):
                raise ValueError("使用Telethon方法必须提供chat_id和message_id")
            return await _download_via_telethon(chat_id, message_id)
        
        # 智能选择逻辑
        if video_obj:
            try:
                # 从video对象获取文件大小
                file_size = getattr(video_obj, 'file_size', 0)
                file_size_mb = file_size / (1024 * 1024)
                
                # 根据文件大小选择下载方式
                if file_size_mb < size_threshold_mb:
                    logger.info(f"🚀 使用Bot API下载 (< {size_threshold_mb}MB)")
                    try:
                        return await _download_via_api(video_obj)
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
                        return await _download_via_api(video_obj)
                        
            except Exception as e:
                logger.warning(f"⚠️ 处理video_obj失败: {e}")
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

async def _download_via_api(video_obj):
    """通过API下载文件"""
    from api.telegram_sender import telegram_sender
    
    start_time = time.time()
    
    # 获取文件（使用video对象的file_id）
    file = await telegram_sender.get_file(video_obj.file_id)
    
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

async def telegram_file_to_path(file_id, save_dir="../download"):
    """通过file_id下载文件到指定目录"""
    try:
        from api.telegram_sender import telegram_sender
        
        # Step 1: 获取文件信息
        file = await telegram_sender.get_file(file_id)
        
        # Step 2: 生成文件名
        original_path = file.file_path
        if original_path:
            filename = os.path.basename(original_path)
        else:
            filename = f"{file_id}"
        
        # Step 3: 构建保存路径
        save_path = os.path.join(save_dir, filename)
        
        # Step 4: 下载文件到指定路径
        await file.download_to_drive(save_path)
        
        return save_path
        
    except Exception as e:
        logger.error(f"下载Telegram文件失败: {e}")
        return False

async def telegram_file_to_base64_by_file_id(file_id):
    """通过file_id获取文件并转换为 Base64 格式"""
    try:
        from api.telegram_sender import telegram_sender
        
        # Step 1: 获取文件信息
        file = await telegram_sender.get_file(file_id)
        
        # Step 2: 下载文件到内存
        file_content = await file.download_as_bytearray()
        
        # Step 3: 转换为 Base64
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        
        return file_base64
        
    except Exception as e:
        logger.error(f"获取文件并转换为Base64失败: {e}")
        return False

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

async def process_avatar_from_url(url: str, min_size: int = 512) -> Optional[BytesIO]:
    """从URL下载图片并处理为头像格式"""
    try:
        image_bytesio, _ = await get_image_from_url(url)
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


def get_60s(format_type="text"):
    """获取API内容并格式化为指定格式
    
    Args:
        url (str): API地址
        format_type (str): 输出格式类型
            - "text": 普通文本格式（默认）
            - "html": HTML blockquote格式
            - "both": 返回两种格式的字典
    
    Returns:
        str or dict: 根据format_type返回相应格式的内容
    """
    url="https://60s-api.viki.moe/v2/60s"

    try:       
        # 发送GET请求
        response = requests.get(url, timeout=10)
        
        # 检查响应状态码
        if response.status_code == 200:
            # 获取JSON数据
            data = response.json()
            
            if 'data' in data:
                news_data = data['data']
                date = news_data.get('date', 'N/A')
                news_list = news_data.get('news', [])
                
                # 构建普通文本格式
                text_format = "📰 每天60秒读懂世界\n"
                text_format += f"日期：{date}\n"
                
                # 构建HTML格式
                html_format = "<blockquote>📰 每天60秒读懂世界</blockquote>\n"
                html_format += f"<blockquote>日期：{date}</blockquote>\n"
                
                # 圈数字符号列表
                circle_numbers = ['①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨', '⑩', 
                                '⑪', '⑫', '⑬', '⑭', '⑮', '⑯', '⑰', '⑱', '⑲', '⑳']
                
                # 添加编号的新闻条目
                for i, news in enumerate(news_list):
                    if i < len(circle_numbers):  # 确保不超出圈数字符号范围
                        # 普通文本格式
                        text_format += f"{circle_numbers[i]}{news}\n"
                        # HTML格式
                        html_format += f"<blockquote>{circle_numbers[i]}{escape_html_chars(news)}</blockquote>\n"
                    else:
                        # 如果超出20条，使用普通数字
                        text_format += f"{i+1}. {news}\n"
                        html_format += f"<blockquote>{i+1}. {escape_html_chars(news)}</blockquote>\n"
                
                # 根据format_type返回相应格式
                if format_type == "text":
                    return {
                        "date": date,
                        "text": text_format.strip()  # 去掉最后的换行符
                    }
                elif format_type == "html":
                    return {
                        "date": date,
                        "html": html_format.strip()  # 去掉最后的换行符
                    }
                elif format_type == "both":
                    return {
                        "date": date,
                        "text": text_format.strip(),
                        "html": html_format.strip()
                    }
                else:
                    logger.warning(f"未知的格式类型: {format_type}，使用默认文本格式")
                    return text_format.strip()
                    
            else:
                logger.error("❌ API响应中没有找到data字段")
                return None
                
        else:
            logger.error(f"❌ 请求失败，状态码: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"❌ 错误: {e}")
        return None

