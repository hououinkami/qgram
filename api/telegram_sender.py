import asyncio
import logging
import threading
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from telegram import Bot, InlineKeyboardMarkup, InputFile, InputMedia, InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAnimation
from telegram.constants import ParseMode
from telegram.error import NetworkError, TelegramError, TimedOut
from telegram.request import HTTPXRequest

import config
from utils import tools
from utils.message_formatter import escape_html_chars, escape_markdown_chars

logger = logging.getLogger(__name__)

class TelegramSender:
    """
    基于线程本地存储的 Telegram 消息发送器
    
    特性：
    - 线程安全：每个线程独立的 Bot 实例
    - 自动重试：网络错误自动重试
    - 资源管理：支持手动和自动清理
    - 错误恢复：Bot 实例异常时自动重新创建
    """
    
    def __init__(self, bot_token: str, default_chat_id: Optional[int] = None, 
        max_retries: int = 3, retry_delay: float = 1.0,
        pool_timeout: float = 60.0,
        connection_pool_size: int = 30):
        """
        初始化 TelegramSender
        
        Args:
            bot_token: Telegram Bot Token
            default_chat_id: 默认聊天ID（可选）
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            pool_timeout: 连接池超时时间
            connection_pool_size: 连接池大小
        """
        self.bot_token = bot_token
        self.default_chat_id = default_chat_id
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.pool_timeout = pool_timeout
        self.connection_pool_size = connection_pool_size
        self._local = threading.local()
        
        logger.info(f"TelegramSender 初始化完成，线程本地存储模式")

    @property
    def bot(self) -> Bot:
        """
        线程安全的 Bot 实例获取
        
        Returns:
            Bot: 当前线程的 Bot 实例
        """
        try:
            # 检查现有 Bot 是否有效
            if (hasattr(self._local, 'bot') and 
                self._local.bot is not None):
                return self._local.bot
        except Exception as e:
            logger.warning(f"访问现有 Bot 实例时出错: {e}")
            # 清理可能损坏的实例
            self._local.bot = None
        
        # 创建新的 Bot 实例
        try:
            # 配置连接池参数（针对微信转发场景优化）
            request = HTTPXRequest(
                connection_pool_size=30,     # 增加连接池大小，适应微信群消息转发
                pool_timeout=60.0,           # 增加连接池超时时间
                read_timeout=45.0,           # 读取超时
                write_timeout=45.0,          # 写入超时
                connect_timeout=15.0        # 连接超时
            )
            
            self._local.bot = Bot(token=self.bot_token, request=request)
            thread_name = threading.current_thread().name
            logger.debug(f"为线程 {thread_name} 创建新的 Bot 实例，连接池大小: 30")
            return self._local.bot
        except Exception as e:
            logger.error(f"创建 Bot 实例失败: {e}")
            raise

    def cleanup_current_bot(self):
        """清理当前线程的 Bot 实例"""
        if hasattr(self._local, 'bot'):
            self._local.bot = None
            thread_name = threading.current_thread().name
            logger.debug(f"清理线程 {thread_name} 的 Bot 实例")

    async def cleanup_current_bot_async(self):
        """异步清理当前线程的 Bot 实例"""
        if hasattr(self._local, 'bot') and self._local.bot is not None:
            try:
                await self._local.bot.shutdown()
                thread_name = threading.current_thread().name
                logger.debug(f"异步清理线程 {thread_name} 的 Bot 实例")
            except Exception as e:
                logger.warning(f"清理 Bot 实例时出错: {e}")
            finally:
                self._local.bot = None

    async def _retry_operation(self, operation, *args, **kwargs):
        """
        带重试的操作执行器（针对微信转发优化）
        
        Args:
            operation: 要执行的异步操作
            *args, **kwargs: 操作参数
            
        Returns:
            操作结果
        """
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return await operation(*args, **kwargs)
            except (NetworkError, TimedOut) as e:
                error_msg = str(e).lower()

                # 检查是否为参数错误（不应该重试的错误）
                non_retryable_errors = [
                    "invalid file http url specified",
                    "unsupported url protocol",
                    "invalid url",
                    "bad request",
                    "invalid parameter",
                    "file not found",
                    "permission denied"
                ]

                # 如果是参数错误，直接抛出不重试
                if any(error_pattern in error_msg for error_pattern in non_retryable_errors):
                    logger.error(f"❌ 参数错误，不进行重试: {e}")
                    raise

                last_exception = e
                if attempt < self.max_retries:
                    # 针对连接池超时使用更长的等待时间
                    if "Pool timeout" in str(e) or "connection pool" in str(e).lower():
                        wait_time = self.retry_delay * (3 ** attempt)  # 更激进的退避策略
                        logger.warning(f"⚠️ 连接池超时，{wait_time}秒后重试 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                    else:
                        wait_time = self.retry_delay * (2 ** attempt)  # 普通网络错误
                        logger.warning(f"⚠️ 网络错误，{wait_time}秒后重试 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                    
                    await asyncio.sleep(wait_time)
                    
                    # 连接池问题时强制重新创建 Bot 实例
                    if "Pool timeout" in str(e) or "connection pool" in str(e).lower():
                        logger.info("检测到连接池问题，重新创建 Bot 实例")
                        await self.cleanup_current_bot_async()
                    else:
                        self.cleanup_current_bot()
                else:
                    logger.error(f"操作最终失败，已重试 {self.max_retries} 次: {e}")
                    break
            except TelegramError as e:
                # 🆕 新增：对特定 Telegram 错误的处理
                error_msg = str(e).lower()
                if "flood control" in error_msg or "too many requests" in error_msg:
                    # 触发限流，等待更长时间
                    wait_time = 60  # 等待1分钟
                    logger.warning(f"触发 Telegram 限流，等待 {wait_time} 秒后重试")
                    await asyncio.sleep(wait_time)
                    if attempt < self.max_retries:
                        continue
                
                logger.error(f"Telegram API 错误: {e}")
                raise
            except Exception as e:
                logger.error(f"未知错误: {e}")
                raise
        
        # 所有重试都失败了
        raise last_exception

    async def send_text(self, chat_id: Optional[int] = None, text: str = "", 
                       reply_to_message_id: Optional[int] = None, 
                       parse_mode: str = ParseMode.HTML, 
                       disable_web_page_preview: bool = False,
                       reply_markup: Optional[InlineKeyboardMarkup] = None):
        """
        发送文本消息
        
        Args:
            chat_id: 聊天ID，为空时使用默认值
            text: 消息文本
            parse_mode: 解析模式
            reply_to_message_id: 回复的消息ID
            disable_web_page_preview: 禁用网页预览
            reply_markup: 内联键盘
            
        Returns:
            Message: 发送的消息对象
        """
        if not text.strip():
            raise ValueError("消息文本不能为空")
        
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        return await self._retry_operation(
            self.bot.send_message,
            chat_id=target_chat_id,
            text=self.text_formatter(text, parse_mode),
            parse_mode=parse_mode,
            reply_to_message_id=reply_to_message_id,
            disable_web_page_preview=disable_web_page_preview,
            reply_markup=reply_markup
        )

    async def send_photo(self, chat_id: Optional[int] = None, photo: Union[str, Path, BytesIO, bytes] = None, caption: str = "", 
                        parse_mode: str = ParseMode.HTML,
                        reply_to_message_id: Optional[int] = None,
                        reply_markup: Optional[InlineKeyboardMarkup] = None):
        """
        发送图片
        
        Args:
            photo: 图片文件路径、Path对象、BytesIO对象或字节数据
            chat_id: 聊天ID，为空时使用默认值
            caption: 图片说明
            parse_mode: 解析模式
            reply_to_message_id: 回复的消息ID
            reply_markup: 内联键盘
            
        Returns:
            Message: 发送的消息对象
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        # 处理不同类型的图片输入
        if isinstance(photo, (str, Path)):
            photo_path = Path(photo)
            if not photo_path.exists():
                raise FileNotFoundError(f"图片文件不存在: {photo_path}")
            photo_input = InputFile(photo_path.open('rb'), filename=photo_path.name)
        elif isinstance(photo, BytesIO):
            photo_input = InputFile(photo, filename=f"image.jpg")
        elif isinstance(photo, bytes):
            photo_input = InputFile(BytesIO(photo), filename=f"image.jpg")
        else:
            photo_input = photo
        
        return await self._retry_operation(
            self.bot.send_photo,
            chat_id=target_chat_id,
            photo=photo_input,
            caption=self.text_formatter(caption, parse_mode),
            parse_mode=parse_mode,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup
        )

    async def send_document(self, chat_id: Optional[int] = None, document: Union[str, Path, BytesIO, bytes] = None, caption: str = "", 
                           parse_mode: str = ParseMode.HTML,
                           reply_to_message_id: Optional[int] = None,
                           reply_markup: Optional[InlineKeyboardMarkup] = None,
                           filename: Optional[str] = None):
        """
        发送文档
        
        Args:
            document: 文档文件路径、Path对象、BytesIO对象或字节数据
            chat_id: 聊天ID，为空时使用默认值
            caption: 文档说明
            parse_mode: 解析模式
            reply_to_message_id: 回复的消息ID
            reply_markup: 内联键盘
            filename: 自定义文件名
            
        Returns:
            Message: 发送的消息对象
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        # 处理不同类型的文档输入
        if isinstance(document, (str, Path)):
            doc_path = Path(document)
            if not doc_path.exists():
                raise FileNotFoundError(f"文档文件不存在: {doc_path}")
            doc_input = InputFile(doc_path.open('rb'), 
                                 filename=filename or doc_path.name)
        elif isinstance(document, BytesIO):
            doc_input = InputFile(document, filename=filename or "[ファイル]")
        elif isinstance(document, bytes):
            doc_input = InputFile(BytesIO(document), 
                                 filename=filename or "[ファイル]")
        else:
            doc_input = document
        
        return await self._retry_operation(
            self.bot.send_document,
            chat_id=target_chat_id,
            document=doc_input,
            caption=self.text_formatter(caption, parse_mode),
            parse_mode=parse_mode,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup
        )

    async def send_video(self, chat_id: Optional[int] = None, video: Union[str, Path, BytesIO, bytes] = None, caption: str = "", 
                        parse_mode: str = ParseMode.HTML,
                        duration: Optional[int] = None,
                        width: Optional[int] = None,
                        height: Optional[int] = None,
                        reply_to_message_id: Optional[int] = None,
                        reply_markup: Optional[InlineKeyboardMarkup] = None,
                        filename: Optional[str] = None):
        """
        发送视频
        
        Args:
            video: 视频文件路径、Path对象、BytesIO对象或字节数据
            chat_id: 聊天ID，为空时使用默认值
            caption: 视频说明
            parse_mode: 解析模式
            duration: 视频时长（秒）
            width: 视频宽度
            height: 视频高度
            reply_to_message_id: 回复的消息ID
            reply_markup: 内联键盘
            filename: 自定义文件名
            
        Returns:
            Message: 发送的消息对象
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        # 处理不同类型的视频输入
        if isinstance(video, (str, Path)):
            video_path = Path(video)
            if not video_path.exists():
                raise FileNotFoundError(f"视频文件不存在: {video_path}")
            video_input = InputFile(video_path.open('rb'), 
                                   filename=filename or video_path.name)
        elif isinstance(video, BytesIO):
            video_input = InputFile(video, filename=filename or f"[動画].mp4")
        elif isinstance(video, bytes):
            video_input = InputFile(BytesIO(video), 
                                   filename=filename or f"[動画].mp4")
        else:
            video_input = video
        
        return await self._retry_operation(
            self.bot.send_video,
            chat_id=target_chat_id,
            video=video_input,
            caption=self.text_formatter(caption, parse_mode),
            parse_mode=parse_mode,
            duration=duration,
            width=width,
            height=height,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup
        )

    async def send_audio(self, chat_id: Optional[int] = None, audio: Union[str, Path, BytesIO, bytes] = None, caption: str = "", 
                        parse_mode: str = ParseMode.HTML,
                        duration: Optional[int] = None,
                        performer: Optional[str] = None,
                        title: Optional[str] = None,
                        reply_to_message_id: Optional[int] = None,
                        reply_markup: Optional[InlineKeyboardMarkup] = None,
                        filename: Optional[str] = None):
        """
        发送音频
        
        Args:
            audio: 音频文件路径、Path对象、BytesIO对象或字节数据
            chat_id: 聊天ID，为空时使用默认值
            caption: 音频说明
            parse_mode: 解析模式
            duration: 音频时长（秒）
            performer: 演唱者
            title: 音频标题
            reply_to_message_id: 回复的消息ID
            reply_markup: 内联键盘
            filename: 自定义文件名
            
        Returns:
            Message: 发送的消息对象
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        # 处理不同类型的音频输入
        if isinstance(audio, (str, Path)):
            audio_path = Path(audio)
            if not audio_path.exists():
                raise FileNotFoundError(f"音频文件不存在: {audio_path}")
            audio_input = InputFile(audio_path.open('rb'), 
                                   filename=filename or audio_path.name)
        elif isinstance(audio, BytesIO):
            audio_input = InputFile(audio, filename=filename or f"[音声].mp3")
        elif isinstance(audio, bytes):
            audio_input = InputFile(BytesIO(audio), 
                                   filename=filename or f"[音声].mp3")
        else:
            audio_input = audio
        
        return await self._retry_operation(
            self.bot.send_audio,
            chat_id=target_chat_id,
            audio=audio_input,
            caption=self.text_formatter(caption, parse_mode),
            parse_mode=parse_mode,
            duration=duration,
            performer=performer,
            title=title,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup
        )
    
    async def send_voice(self, chat_id: Optional[int] = None, 
                        voice: Union[str, Path, BytesIO, bytes] = None, 
                        caption: str = "", 
                        duration: Optional[int] = None,
                        parse_mode: str = ParseMode.HTML,
                        reply_to_message_id: Optional[int] = None,
                        reply_markup: Optional[InlineKeyboardMarkup] = None,
                        filename: Optional[str] = None):
        """
        发送语音消息
        
        Args:
            chat_id: 聊天ID，为空时使用默认值
            voice: 语音文件路径、Path对象、BytesIO对象或字节数据
            caption: 语音说明
            parse_mode: 解析模式
            duration: 语音时长（秒）
            reply_to_message_id: 回复的消息ID
            reply_markup: 内联键盘
            filename: 自定义文件名
            
        Returns:
            Message: 发送的消息对象
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        if voice is None:
            raise ValueError("必须提供 voice 参数")
        
        # 处理不同类型的语音输入
        if isinstance(voice, (str, Path)):
            voice_path = Path(voice)
            if not voice_path.exists():
                raise FileNotFoundError(f"语音文件不存在: {voice_path}")
            voice_input = InputFile(voice_path.open('rb'), 
                                   filename=filename or voice_path.name)
        elif isinstance(voice, BytesIO):
            voice_input = InputFile(voice, filename=filename or f"[音声].ogg")
        elif isinstance(voice, bytes):
            voice_input = InputFile(BytesIO(voice), 
                                   filename=filename or f"[音声].ogg")
        else:
            voice_input = voice
        
        return await self._retry_operation(
            self.bot.send_voice,
            chat_id=target_chat_id,
            voice=voice_input,
            caption=self.text_formatter(caption, parse_mode),
            parse_mode=parse_mode,
            duration=duration,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup
        )

    async def send_media_group(self, chat_id: Optional[int] = None,
                              media: List[Union[Dict[str, Any], Any]] = None, 
                              parse_mode: str = ParseMode.HTML,
                              reply_to_message_id: Optional[int] = None):
        """
        发送媒体组（相册）
        
        Args:
            media: 媒体列表
            chat_id: 聊天ID，为空时使用默认值
            parse_mode: 解析模式，会应用到所有媒体的caption中
            reply_to_message_id: 回复的消息ID
            
        Returns:
            List[Message]: 发送的消息列表
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        if not media:
            raise ValueError("媒体列表不能为空")
        
        # 处理媒体列表，为每个媒体项设置parse_mode
        processed_media = []
        for item in media:
            if hasattr(item, 'caption') and item.caption:
                # 如果媒体项有caption，格式化它并设置parse_mode
                formatted_caption = self.text_formatter(item.caption, parse_mode)
                
                # 创建新的媒体对象，保持原有属性但更新caption和parse_mode
                if hasattr(item, '__class__'):
                    # 获取原对象的所有属性
                    kwargs = {}
                    for attr in ['media', 'caption', 'parse_mode', 'width', 'height', 
                            'duration', 'performer', 'title', 'thumbnail']:
                        if hasattr(item, attr):
                            kwargs[attr] = getattr(item, attr)
                    
                    # 更新caption和parse_mode
                    kwargs['caption'] = formatted_caption
                    kwargs['parse_mode'] = parse_mode
                    
                    # 创建同类型的新对象
                    new_item = item.__class__(**kwargs)
                    processed_media.append(new_item)
                else:
                    processed_media.append(item)
            else:
                processed_media.append(item)
        
        raw_response = await self._retry_operation(
            self.bot.send_media_group,
            chat_id=target_chat_id,
            media=processed_media,
            reply_to_message_id=reply_to_message_id
        )
        
        # 返回第一条消息而不是消息列表
        if isinstance(raw_response, (list, tuple)) and raw_response:
            return raw_response[0]
        else:
            return raw_response
    
    async def send_animation(self, chat_id: Optional[int] = None, animation: Union[str, Path, BytesIO, bytes] = None, caption: str = "",  
                           parse_mode: str = ParseMode.HTML,
                           duration: Optional[int] = None,
                           width: Optional[int] = None,
                           height: Optional[int] = None,
                           thumbnail: Optional[Union[str, Path, BytesIO, bytes]] = None,
                           reply_to_message_id: Optional[int] = None,
                           reply_markup: Optional[InlineKeyboardMarkup] = None,
                           filename: Optional[str] = None):
        """
        发送动画（GIF 或 H.264/MPEG-4 AVC 视频，无声音）
        
        Args:
            chat_id: 聊天ID，为空时使用默认值
            animation: 动画文件路径、Path对象、BytesIO对象或字节数据
            caption: 动画说明
            parse_mode: 解析模式
            duration: 动画时长（秒）
            width: 动画宽度
            height: 动画高度
            thumbnail: 缩略图文件（可选）
            reply_to_message_id: 回复的消息ID
            reply_markup: 内联键盘
            filename: 自定义文件名
            
        Returns:
            Message: 发送的消息对象
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        # 处理不同类型的动画输入
        if isinstance(animation, (str, Path)):
            animation_path = Path(animation)
            if not animation_path.exists():
                raise FileNotFoundError(f"动画文件不存在: {animation_path}")
            animation_input = InputFile(animation_path.open('rb'), 
                                       filename=filename or animation_path.name)
        elif isinstance(animation, BytesIO):
            animation_input = InputFile(animation, filename=filename or f"[ステッカー].gif")
        elif isinstance(animation, bytes):
            animation_input = InputFile(BytesIO(animation), 
                                       filename=filename or f"[ステッカー].gif")
        else:
            animation_input = animation
        
        # 处理缩略图（如果提供）
        thumb_input = None
        if thumbnail is not None:
            if isinstance(thumbnail, (str, Path)):
                thumb_path = Path(thumbnail)
                if thumb_path.exists():
                    thumb_input = InputFile(thumb_path.open('rb'), filename=thumb_path.name)
            elif isinstance(thumbnail, BytesIO):
                thumb_input = InputFile(thumbnail, filename="thumb.jpg")
            elif isinstance(thumbnail, bytes):
                thumb_input = InputFile(BytesIO(thumbnail), filename="thumb.jpg")
            else:
                thumb_input = thumbnail
        
        return await self._retry_operation(
            self.bot.send_animation,
            chat_id=target_chat_id,
            animation=animation_input,
            caption=self.text_formatter(caption, parse_mode),
            parse_mode=parse_mode,
            duration=duration,
            width=width,
            height=height,
            thumbnail=thumb_input,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup
        )
    
    async def send_location(self, chat_id: Optional[int] = None,
                        latitude: float = None,
                        longitude: float = None,
                        title: str = "",
                        address: str = "",
                        foursquare_id: Optional[str] = None,
                        foursquare_type: Optional[str] = None,
                        google_place_id: Optional[str] = None,
                        google_place_type: Optional[str] = None,
                        reply_to_message_id: Optional[int] = None,
                        reply_markup: Optional[InlineKeyboardMarkup] = None):
        """
        发送场所信息（包含标题和地址的位置）
        
        Args:
            chat_id: 聊天ID，为空时使用默认值
            latitude: 纬度（必需）
            longitude: 经度（必需）
            title: 场所标题（必需）
            address: 场所地址（必需）
            foursquare_id: Foursquare ID
            foursquare_type: Foursquare 类型
            google_place_id: Google Places ID
            google_place_type: Google Places 类型
            reply_to_message_id: 回复的消息ID
            reply_markup: 内联键盘
            
        Returns:
            Message: 发送的消息对象
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        if latitude is None or longitude is None:
            raise ValueError("必须提供 latitude 和 longitude 参数")
        
        if not title.strip():
            raise ValueError("必须提供 title 参数")
        
        if not address.strip():
            raise ValueError("必须提供 address 参数")
        
        # 验证纬度和经度范围
        if not (-90 <= latitude <= 90):
            raise ValueError("纬度必须在 -90 到 90 度之间")
        
        if not (-180 <= longitude <= 180):
            raise ValueError("经度必须在 -180 到 180 度之间")
        
        return await self._retry_operation(
            self.bot.send_venue,
            chat_id=target_chat_id,
            latitude=latitude,
            longitude=longitude,
            title=title,
            address=address,
            foursquare_id=foursquare_id,
            foursquare_type=foursquare_type,
            google_place_id=google_place_id,
            google_place_type=google_place_type,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup
        )

    async def send_realtime_location(self, chat_id: Optional[int] = None, 
                        latitude: float = None, 
                        longitude: float = None,
                        live_period: Optional[int] = None,
                        heading: Optional[int] = None,
                        proximity_alert_radius: Optional[int] = None,
                        reply_to_message_id: Optional[int] = None,
                        reply_markup: Optional[InlineKeyboardMarkup] = None):
        """
        发送地理位置
        
        Args:
            chat_id: 聊天ID，为空时使用默认值
            latitude: 纬度（必需）
            longitude: 经度（必需）
            live_period: 实时位置更新周期（秒），范围 60-86400
            heading: 移动方向（度），范围 1-360，仅适用于实时位置
            proximity_alert_radius: 接近警报半径（米），范围 1-100000
            reply_to_message_id: 回复的消息ID
            reply_markup: 内联键盘
            
        Returns:
            Message: 发送的消息对象
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        if latitude is None or longitude is None:
            raise ValueError("必须提供 latitude 和 longitude 参数")
        
        # 验证纬度和经度范围
        if not (-90 <= latitude <= 90):
            raise ValueError("纬度必须在 -90 到 90 度之间")
        
        if not (-180 <= longitude <= 180):
            raise ValueError("经度必须在 -180 到 180 度之间")
        
        # 验证实时位置参数
        if live_period is not None:
            if not (60 <= live_period <= 86400):
                raise ValueError("live_period 必须在 60 到 86400 秒之间")
        
        # 验证移动方向
        if heading is not None:
            if not (1 <= heading <= 360):
                raise ValueError("heading 必须在 1 到 360 度之间")
            if live_period is None:
                logger.warning("heading 参数仅在设置 live_period 时有效")
        
        # 验证接近警报半径
        if proximity_alert_radius is not None:
            if not (1 <= proximity_alert_radius <= 100000):
                raise ValueError("proximity_alert_radius 必须在 1 到 100000 米之间")
        
        return await self._retry_operation(
            self.bot.send_location,
            chat_id=target_chat_id,
            latitude=latitude,
            longitude=longitude,
            live_period=live_period,
            heading=heading,
            proximity_alert_radius=proximity_alert_radius,
            reply_to_message_id=reply_to_message_id,
            reply_markup=reply_markup
        )

    async def edit_message_text(self, chat_id: Optional[int] = None, text: str = "",  
                               message_id: Optional[int] = None,
                               inline_message_id: Optional[str] = None,
                               parse_mode: str = ParseMode.HTML,
                               disable_web_page_preview: bool = False,
                               reply_markup: Optional[InlineKeyboardMarkup] = None):
        """
        编辑消息文本
        
        Args:
            text: 新的消息文本
            chat_id: 聊天ID
            message_id: 消息ID
            inline_message_id: 内联消息ID
            parse_mode: 解析模式
            disable_web_page_preview: 禁用网页预览
            reply_markup: 内联键盘
            
        Returns:
            Message: 编辑后的消息对象
        """
        if not text.strip():
            raise ValueError("消息文本不能为空")
        
        return await self._retry_operation(
            self.bot.edit_message_text,
            text=self.text_formatter(text, parse_mode),
            chat_id=chat_id,
            message_id=message_id,
            inline_message_id=inline_message_id,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            reply_markup=reply_markup
        )

    async def edit_message_caption(self, chat_id: Optional[int] = None, 
                              caption: str = "",
                              message_id: Optional[int] = None,
                              inline_message_id: Optional[str] = None,
                              parse_mode: str = ParseMode.HTML,
                              reply_markup: Optional[InlineKeyboardMarkup] = None):
        """
        编辑媒体消息的说明文字
        
        Args:
            chat_id: 聊天ID
            caption: 新的说明文字
            message_id: 消息ID
            inline_message_id: 内联消息ID
            parse_mode: 解析模式
            reply_markup: 内联键盘
            
        Returns:
            Message: 编辑后的消息对象
        """
        return await self._retry_operation(
            self.bot.edit_message_caption,
            chat_id=chat_id,
            message_id=message_id,
            inline_message_id=inline_message_id,
            caption=self.text_formatter(caption, parse_mode),
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )

    async def edit_message_media(self, chat_id: Optional[int] = None,
                               message_id: Optional[int] = None,
                               inline_message_id: Optional[str] = None,
                               media: Union[str, BytesIO, InputMedia] = None,
                               media_type: str = "photo",
                               filename: Optional[str] = None,
                               caption: Optional[str] = None, 
                               parse_mode: str = ParseMode.HTML,
                               reply_markup: Optional[InlineKeyboardMarkup] = None):
        """
        编辑消息的媒体内容
        
        Args:
            chat_id: 聊天ID
            message_id: 消息ID
            inline_message_id: 内联消息ID
            media: 媒体文件，可以是文件路径(str)、BytesIO对象或InputMedia对象
            media_type: 媒体类型 ("photo", "video", "document", "animation")
            filename: 文件名（当media为BytesIO时使用）
            caption: 媒体说明文字（如果不传入则不设置caption）
            parse_mode: 解析模式
            reply_markup: 内联键盘
            
        Returns:
            Message: 编辑后的消息对象
        """
        
        # 如果已经是InputMedia对象，直接使用
        if isinstance(media, (InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAnimation)):
            input_media = media
        else:
            # 处理文件路径或BytesIO对象
            media_file = media
            
            # 如果是BytesIO对象且没有name属性，设置filename
            if isinstance(media, BytesIO) and filename and not hasattr(media, 'name'):
                media.name = filename
            
            # 根据媒体类型创建对应的InputMedia对象
            if media_type.lower() == "photo":
                input_media = InputMediaPhoto(
                    media=media_file,
                    caption=caption,
                    parse_mode=parse_mode if caption else None
                )
            elif media_type.lower() == "video":
                input_media = InputMediaVideo(
                    media=media_file,
                    caption=caption,
                    parse_mode=parse_mode if caption else None
                )
            elif media_type.lower() == "document":
                input_media = InputMediaDocument(
                    media=media_file,
                    filename=filename,
                    caption=caption,
                    parse_mode=parse_mode if caption else None
                )
            elif media_type.lower() == "animation":
                input_media = InputMediaAnimation(
                    media=media_file,
                    caption=caption,
                    parse_mode=parse_mode if caption else None
                )
            else:
                raise ValueError(f"不支持的媒体类型: {media_type}")
        
        return await self._retry_operation(
            self.bot.edit_message_media,
            chat_id=chat_id,
            message_id=message_id,
            inline_message_id=inline_message_id,
            media=input_media,
            reply_markup=reply_markup
        )

    async def delete_message(self, chat_id: Optional[int] = None, 
                            message_id: int = None):
        """
        删除消息
        
        Args:
            chat_id: 聊天ID，为空时使用默认值
            message_id: 消息ID
            
        Returns:
            bool: 删除是否成功
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        if message_id is None:
            raise ValueError("必须提供 message_id")
        
        return await self._retry_operation(
            self.bot.delete_message,
            chat_id=target_chat_id,
            message_id=message_id
        )

    async def get_chat(self, chat_id: Optional[int] = None):
        """
        获取聊天信息
        
        Args:
            chat_id: 聊天ID，为空时使用默认值
            
        Returns:
            Chat: 聊天对象
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        return await self._retry_operation(
            self.bot.get_chat,
            chat_id=target_chat_id
        )

    async def get_me(self):
        """
        获取机器人信息
        
        Returns:
            User: 机器人用户对象
        """
        return await self._retry_operation(self.bot.get_me)
    
    async def get_file(self, file_id: str):
        """
        获取文件信息
        
        Args:
            file_id: 文件ID
            
        Returns:
            File: 文件对象
        """
        return await self._retry_operation(
            self.bot.get_file,
            file_id=file_id
        )

    async def set_chat_title(self, chat_id: Optional[int] = None, 
                            title: str = ""):
        """
        设置群组标题
        
        Args:
            chat_id: 聊天ID，为空时使用默认值
            title: 新的群组标题（1-128个字符）
            
        Returns:
            bool: 设置是否成功
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        if not title.strip():
            raise ValueError("群组标题不能为空")
        
        # 验证标题长度
        if len(title) > 128:
            raise ValueError("群组标题不能超过128个字符")
        
        return await self._retry_operation(
            self.bot.set_chat_title,
            chat_id=target_chat_id,
            title=title
        )
    
    async def set_chat_photo(self, chat_id: Optional[int] = None, 
                            photo: Union[str, Path, BytesIO, bytes] = None,
                            delete_old_photo: bool = False):
        """
        设置群组头像
        
        Args:
            chat_id: 聊天ID，为空时使用默认值
            photo: 头像图片文件路径、Path对象、BytesIO对象、字节数据或图片URL
            delete_old_photo: 是否在设置新头像前删除旧头像（默认False）
            
        Returns:
            bool: 设置是否成功
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        if photo is None:
            raise ValueError("必须提供 photo 参数")
        
        # 如果需要删除旧头像，先尝试删除
        if delete_old_photo:
            try:
                # 先获取聊天信息，检查是否有头像
                chat_info = await self.get_chat(target_chat_id)
                if hasattr(chat_info, 'photo') and chat_info.photo is not None:
                    await self.delete_chat_photo(target_chat_id)
                    # 等待一小段时间确保删除操作完成
                    await asyncio.sleep(0.5)
            except Exception as e:
                # 删除失败不影响设置新头像的操作
                logger.warning(f"删除旧头像时出错（将继续设置新头像）: {e}")
        
        # 处理不同类型的头像输入
        if isinstance(photo, str):
            # 检查是否为URL
            if photo.startswith(('http://', 'https://')):
                # 如果是URL，需要先下载图片
                photo_bytesio = await tools.get_image_from_url(photo)
                photo_input = InputFile(photo_bytesio, filename="avatar.jpg")
            else:
                # 本地文件路径
                photo_path = Path(photo)
                if not photo_path.exists():
                    raise FileNotFoundError(f"头像文件不存在: {photo_path}")
                photo_input = InputFile(photo_path.open('rb'), filename=photo_path.name)
        elif isinstance(photo, Path):
            if not photo.exists():
                raise FileNotFoundError(f"头像文件不存在: {photo}")
            photo_input = InputFile(photo.open('rb'), filename=photo.name)
        elif isinstance(photo, BytesIO):
            photo_input = InputFile(photo, filename="avatar.jpg")
        elif isinstance(photo, bytes):
            photo_input = InputFile(BytesIO(photo), filename="avatar.jpg")
        else:
            photo_input = photo
        
        return await self._retry_operation(
            self.bot.set_chat_photo,
            chat_id=target_chat_id,
            photo=photo_input
        )

    async def set_chat_description(self, chat_id: Optional[int] = None, 
                                  description: str = ""):
        """
        设置群组描述
        
        Args:
            chat_id: 聊天ID，��空时使用默认值
            description: 新的群组描述（0-255个字符）
            
        Returns:
            bool: 设置是否成功
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        # 验证描述长度
        if len(description) > 255:
            raise ValueError("群组描述不能超过255个字符")
        
        return await self._retry_operation(
            self.bot.set_chat_description,
            chat_id=target_chat_id,
            description=description
        )

    async def delete_chat_photo(self, chat_id: Optional[int] = None):
        """
        删除群组头像
        
        Args:
            chat_id: 聊天ID，为空时使用默认值
            
        Returns:
            bool: 删除是否成功
        """
        target_chat_id = chat_id or self.default_chat_id
        if target_chat_id is None:
            raise ValueError("必须提供 chat_id 或设置默认 chat_id")
        
        return await self._retry_operation(
            self.bot.delete_chat_photo,
            chat_id=target_chat_id
        )
    
    def text_formatter(self, text: str, parse_mode: str = ""):
        """格式化发送文本"""
        if parse_mode == ParseMode.HTML:
            return escape_html_chars(text)
        elif parse_mode == ParseMode.MARKDOWN:
            return escape_markdown_chars(text)
        else:
            return text

    def get_thread_info(self) -> Dict[str, Any]:
        """
        获取当前线程的信息（调试用）
        
        Returns:
            Dict: 线程信息
        """
        thread = threading.current_thread()
        has_bot = hasattr(self._local, 'bot') and self._local.bot is not None
        
        return {
            "thread_id": thread.ident,
            "thread_name": thread.name,
            "has_bot_instance": has_bot,
            "bot_id": id(self._local.bot) if has_bot else None
        }

    def __str__(self) -> str:
        """字符串表示"""
        thread_info = self.get_thread_info()
        return (f"TelegramSender(thread={thread_info['thread_name']}, "
               f"has_bot={thread_info['has_bot_instance']}, "
               f"default_chat_id={self.default_chat_id})")

    def __repr__(self) -> str:
        """详细字符串表示"""
        return self.__str__()

# 创建全局实例
telegram_sender = TelegramSender(
    bot_token=config.BOT_TOKEN,
    max_retries=4,              # 微信转发建议4次重试
    retry_delay=1.5,            # 稍长的重试延迟
    pool_timeout=60.0,          # 1分钟连接池超时
    connection_pool_size=30     # 30个连接池大小
)
