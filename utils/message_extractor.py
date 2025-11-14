import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Callable, Union

from api.qq_api import qq_api
from config import locale

logger = logging.getLogger(__name__)


@dataclass
class ParsedMessage:
    """消息解析结果数据类"""
    text_parts: List[str] = field(default_factory=list)
    images: List[Dict[str, Any]] = field(default_factory=list)
    media_items: List[Dict[str, Any]] = field(default_factory=list)
    has_reply: bool = False
    reply_segments: List[Dict] = field(default_factory=list)
    has_forward: bool = False
    forward_data: Optional[Dict[str, str]] = None
    has_at: bool = False
    at_segments: List[Dict] = field(default_factory=list)
    
    @property
    def text_content(self) -> str:
        """获取合并后的文本内容"""
        return ''.join(self.text_parts)


class MessageContentExtractor:
    """消息内容提取器"""
    
    # 媒体类型配置
    MEDIA_CONFIGS = {
        'video': {'url_key': 'url', 'file_key': 'file', 'display': locale.type('video')},
        'record': {'url_key': 'url', 'file_key': 'file', 'display': locale.type('voice'), 'type': 'voice'},
        'file': {'url_key': 'url', 'file_key': 'file', 'display': locale.type('file')}
    }
    
    # 特殊消息段格式化器（移除 at，因为需要专门处理）
    SPECIAL_FORMATTERS = {
        'share': lambda d: f"[{locale.type('share')}: {d.get('title', '')}]",
        'music': lambda d: f"[{locale.type('music')}: {d.get('title', '')}]",
        'location': lambda d: f"[{locale.type('location')}: {d.get('title', '')}]",
        'face': lambda d: (
            f"[{d.get('raw', {}).get('faceText', '').lstrip('/')}]"
            if isinstance(d.get('raw'), dict) and d.get('raw', {}).get('faceText')
            else f"[{locale.type('emoji')}]"
        ),
    }
    
    def __init__(self, logger=None):
        """
        初始化
        
        Args:
            logger: 日志记录器
        """
        self.logger = logger
        
        # 消息段处理器映射
        self._segment_handlers = {
            'text': self._handle_text,
            'image': self._handle_image,
            'reply': self._handle_reply,
            'forward': self._handle_forward,
            'at': self._handle_at,
            'json': self._handle_json
        }
    
    # ==================== 异步入口====================
    
    async def extract(self, callback_message: Dict) -> Dict[str, Any]:
        """
        异步提取消息内容（支持 at 用户信息查询）
        
        使用示例:
            result = await extractor.extract(callback_message)
        """
        message_array = callback_message.get('message', [])
        
        try:
            if not isinstance(message_array, list):
                return self._create_result('text', str(message_array) if message_array else '[空消息]')
            
            # 解析所有消息段（异步模式）
            parsed_data = await self._parse_all_segments(message_array, callback_message)
            
            # 根据解析结果决定消息类型
            return self._determine_message_type(parsed_data, message_array)
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 提取消息内容失败: {e}", exc_info=True)
            return self._create_result('text', f'[消息解析失败: {str(message_array)[:100]}...]')
    
    # ==================== 异步解析 ====================
    
    async def _parse_all_segments(self, message_array: List[Dict], callback_message: Optional[Dict] = None) -> ParsedMessage:
        """解析所有消息段（按顺序处理，保持原始顺序）"""
        result = ParsedMessage()
        
        for i, segment in enumerate(message_array):
            if not isinstance(segment, dict):
                continue
                
            seg_type = segment.get('type', 'unknown')
            seg_data = segment.get('data', {})
            
            if self.logger:
                self.logger.debug(f"  处理消息段 {i+1}: {seg_type} - {seg_data}")
            
            try:
                # 根据类型处理消息段
                if seg_type == 'at':
                    await self._handle_at(seg_data, result, segment, callback_message)
                elif seg_type in self._segment_handlers:
                    handler = self._segment_handlers[seg_type]
                    if asyncio.iscoroutinefunction(handler):
                        await handler(seg_data, result, segment, callback_message)
                    else:
                        handler(seg_data, result, segment, callback_message)
                elif seg_type in self.MEDIA_CONFIGS:
                    self._handle_media(seg_type, seg_data, result)
                elif seg_type in self.SPECIAL_FORMATTERS:
                    self._handle_special(seg_type, seg_data, result)
                else:
                    result.text_parts.append(f'[{seg_type}]')
                    if self.logger:
                        self.logger.debug(f"   未知消息段类型: {seg_type}")
            
            except Exception as e:
                if self.logger:
                    self.logger.error(f"   处理消息段 {seg_type} 失败: {e}")
                result.text_parts.append(f'[{seg_type}处理失败]')
        
        return result
    
    # ==================== at 消息段处理器 ====================    
    async def _handle_at(self, seg_data: Dict, result: ParsedMessage, segment: Dict, callback_message):
        """处理 at 段（异步完整版本）"""
        result.has_at = True
        result.at_segments.append(segment)
        
        group = callback_message.get('group_id', 0)
        qq = seg_data.get('qq', '')
        
        # @全体成员
        if qq == 'all':
            result.text_parts.append(f"[@{locale.common('all')}]")
            if self.logger:
                self.logger.debug("   @全体成员")
            return
        
        # 查询用户信息
        user_info = None

        try:
            user_info = await self.user_info_fetcher(group, qq)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"   查询用户 {qq} 信息失败: {e}")
        
        # 格式化 at 文本
        if user_info:
            # 优先使用群名片，其次昵称
            display_name = user_info.get('card') or user_info.get('nickname') or qq
            result.text_parts.append(f'[@{display_name}]')
            
            if self.logger:
                self.logger.debug(f"   @用户: {display_name} (QQ: {qq})")
        else:
            result.text_parts.append(f'[@{qq}]')
            
            if self.logger:
                self.logger.debug(f"   @用户: {qq} (未获取到详细信息)")
    
    def _handle_json(self, seg_data: Dict, result: ParsedMessage, *args):
        """处理 JSON 类型消息（如分享卡片）"""
        try:
            import json
            json_data = json.loads(seg_data.get('data', '{}'))
            news = json_data.get('meta', {}).get('news', {})
            
            tag = news.get('tag', '')
            title = news.get('title', json_data.get('prompt', '[JSON消息]'))
            url = news.get('jumpUrl', '')
            
            # 构建 Telegram HTML 格式
            parts = []
            if tag:
                parts.append(f'<blockquote>{tag}</blockquote>')
            
            if url:
                parts.append(f'<a href="{url}">{title}</a>')
            else:
                parts.append(title)
            
            result.text_parts.append('\n'.join(parts))
            
            if self.logger:
                self.logger.debug(f"   JSON消息: {tag} - {title}")
        
        except Exception as e:
            if self.logger:
                self.logger.error(f"   处理 JSON 消息失败: {e}")
            result.text_parts.append('[JSON消息]')

    # ==================== 其他消息段处理器（保持不变）====================
    
    def _handle_text(self, seg_data: Dict, result: ParsedMessage, *args):
        """处理文本段"""
        text = seg_data.get('text', '')
        if text:
            result.text_parts.append(text)
    
    def _handle_image(self, seg_data: Dict, result: ParsedMessage, *args):
        """处理图片段"""
        url = seg_data.get('url', '')
        summary = seg_data.get('summary', '')
        sub_type = seg_data.get('sub_type', 0)

        if url:
            is_sticker = summary == '[动画表情]' or sub_type == 1 or '动画表情' in summary
            
            image_info = {
                'url': url,
                'file': seg_data.get('file', ''),
                'size': seg_data.get('file_size', ''),
                'is_sticker': is_sticker,
                'summary': summary
            }
            
            result.images.append(image_info)
            
            if self.logger:
                image_type = "贴纸表情" if is_sticker else "图片"
                self.logger.debug(f"   {image_type}URL: {url}")
        else:
            placeholder = '[贴纸表情]' if summary == '[动画表情]' or sub_type == 1 else '[图片]'
            result.text_parts.append(placeholder)
    
    def _handle_media(self, seg_type: str, seg_data: Dict, result: ParsedMessage):
        """处理媒体段"""
        config = self.MEDIA_CONFIGS[seg_type]
        url = seg_data.get(config['url_key'], '')
        file_name = seg_data.get(config['file_key'], '')
        
        if url:
            # ✅ 特殊处理文件URL
            if seg_type == 'file' and file_name:
                # 检查URL是否缺少文件名参数
                if url.endswith('?fname=') or '?fname=' not in url:
                    # 添加文件名到URL
                    import urllib.parse
                    if url.endswith('?fname='):
                        url = url + urllib.parse.quote(file_name)
                    elif '?fname=' not in url:
                        separator = '&' if '?' in url else '?'
                        url = url + f'{separator}fname=' + urllib.parse.quote(file_name)
            
            media_type = config.get('type', seg_type)
            result.media_items.append({
                'type': media_type,
                'url': url,  # ✅ 修复后的URL
                'file': file_name
            })
            
            if self.logger:
                self.logger.debug(f"   {config['display']}URL: {url}")
        else:
            display_text = f"[{config['display']}{f': {file_name}' if file_name else ''}]"
            result.text_parts.append(display_text)
    
    def _handle_reply(self, seg_data: Dict, result: ParsedMessage, segment: Dict, *args):
        """处理回复段"""
        result.has_reply = True
        result.reply_segments.append(segment)
    
    def _handle_forward(self, seg_data: Dict, result: ParsedMessage, segment: Dict, callback_message: Optional[Dict] = None):
        """处理合并转发消息"""
        result.has_forward = True
        forward_id = seg_data.get('id', '')
        
        if callback_message:
            message_id = callback_message.get('message_id', '')
            result.forward_data = {
                'forward_id': forward_id,
                'message_id': message_id
            }
            if self.logger:
                self.logger.debug(f"   合并转发消息ID: {message_id}, 转发ID: {forward_id}")
        else:
            result.forward_data = {'forward_id': forward_id, 'message_id': ''}
    
    def _handle_special(self, seg_type: str, seg_data: Dict, result: ParsedMessage):
        """处理特殊段"""
        formatter = self.SPECIAL_FORMATTERS.get(seg_type)
        if formatter:
            formatted_text = formatter(seg_data)
            result.text_parts.append(formatted_text)
            self._log_special_info(seg_type, seg_data)
        else:
            result.text_parts.append(f'[{seg_type}]')
    
    def _log_special_info(self, seg_type: str, seg_data: Dict):
        """记录特殊段的额外信息"""
        if not self.logger:
            return
            
        if seg_type == 'share' and seg_data.get('url'):
            self.logger.debug(f"   分享链接: {seg_data['url']}")
        elif seg_type == 'location' and seg_data.get('lat') and seg_data.get('lon'):
            self.logger.debug(f"   位置坐标: {seg_data['lat']}, {seg_data['lon']}")
    
    # ==================== 消息类型判断（保持不变）====================
    
    def _determine_message_type(self, parsed: ParsedMessage, original_message: List[Dict]) -> Dict[str, Any]:
        """根据解析数据决定消息类型"""
        
        # 优先级顺序检查
        type_checkers = [
            (self._check_forward, parsed, original_message),
            (self._check_reply, parsed, original_message),
            (self._check_at, parsed, original_message),  # ✅ 新增 at 检查
            (self._check_multiple_images, parsed),
            (self._check_single_image, parsed),
            (self._check_single_media, parsed),
            (self._check_mixed, parsed),
        ]
        
        for checker, *args in type_checkers:
            result = checker(*args)
            if result:
                return result
        
        # 默认：纯文本
        return self._check_text(parsed)
    
    def _check_forward(self, parsed: ParsedMessage, original_message: List[Dict]) -> Optional[Dict]:
        """检查转发消息"""
        if not parsed.has_forward:
            return None
            
        if self.logger:
            self.logger.debug("   检测到合并转发消息类型")
        
        forward_data = parsed.forward_data or {}
        return self._create_result(
            'forward', 
            forward_data.get('message_id', ''),
            forward_id=forward_data.get('forward_id', ''),
            message=original_message
        )
    
    def _check_reply(self, parsed: ParsedMessage, original_message: List[Dict]) -> Optional[Dict]:
        """检查回复消息"""
        if not parsed.has_reply:
            return None
            
        if self.logger:
            self.logger.debug("   检测到引用消息类型")
        
        return self._create_result('reply', locale.type('reply'), message=original_message)
    
    def _check_at(self, parsed: ParsedMessage, original_message: List[Dict]) -> Optional[Dict]:
        """检查 at 消息（✅ 新增）"""
        # 如果消息只包含 at 和少量文本，可以作为独立类型
        # 这里的逻辑可以根据需求调整
        if parsed.has_at and not parsed.images and not parsed.media_items:
            # 如果只有 at 没有其他内容，可以返回特殊类型
            if len(parsed.text_content.strip()) < 50:  # 文本较短
                if self.logger:
                    self.logger.debug("   检测到 at 消息类型")
                
                return self._create_result(
                    'at', 
                    parsed.text_content,
                    at_list=[seg.get('data', {}).get('qq') for seg in parsed.at_segments],
                    message=original_message
                )
        
        return None
    
    def _check_multiple_images(self, parsed: ParsedMessage) -> Optional[Dict]:
        """检查多图消息"""
        if len(parsed.images) <= 1:
            return None
        
        return self._create_result('images', parsed.images, text=parsed.text_content)
    
    def _check_single_image(self, parsed: ParsedMessage) -> Optional[Dict]:
        """检查单图消息"""
        if len(parsed.images) != 1 or parsed.media_items:
            return None
        
        img = parsed.images[0]
        msg_type = 'sticker' if img.get('is_sticker') else 'image'
        
        return self._create_result(
            msg_type, 
            img['url'],
            file=img.get('file', ''),
            size=img.get('size', ''),
            summary=img.get('summary', ''),
            text=parsed.text_content
        )
    
    def _check_single_media(self, parsed: ParsedMessage) -> Optional[Dict]:
        """检查单媒体消息"""
        if len(parsed.media_items) != 1 or parsed.images:
            return None
        
        media = parsed.media_items[0]
        return self._create_result(
            media['type'], 
            media['url'],
            file=media.get('file', ''),
            text=parsed.text_content
        )
    
    def _check_mixed(self, parsed: ParsedMessage) -> Optional[Dict]:
        """检查混合消息"""
        if not (parsed.images or parsed.media_items):
            return None
        
        mixed_content = self._build_mixed_content(parsed)
        return self._create_result('mixed', mixed_content)
    
    def _check_text(self, parsed: ParsedMessage) -> Dict:
        """纯文本消息"""
        final_text = parsed.text_content if parsed.text_content.strip() else '[空消息]'
        return self._create_result('text', final_text)
    
    # ==================== 辅助方法（保持不变）====================
    # 示例：用户信息获取函数（需要你实现）
    async def user_info_fetcher(self, group, qq) -> Dict[str, any]:
        """
        从你的其他模块获取用户信息
        
        Args:
            group: 群号
            qq: QQ号
            
        Returns:
            {'nickname': '昵称', 'card': '群名片', ...}
        """
        if not group:
            logger.warning(f"❌ 获取用户 {qq} 信息失败：未提供群组ID")
            return {}
        
        payload = {
            "group_id": group,
            "user_id": qq,
            "no_cache": False
        }

        try:
            logger.debug(f"🔍 查询用户信息: QQ={qq}, Group={group}")
            
            # 调用 QQ API
            response = await qq_api("GET_MEMBER_INFO", payload)
            
            # 检查响应状态
            if not isinstance(response, dict):
                logger.warning(f"❌ API响应格式错误: {type(response)}")
                return {}
            
            status = response.get('status')
            retcode = response.get('retcode')
            
            if status != 'ok' or retcode != 0:
                logger.warning(f"❌ API调用失败: status={status}, retcode={retcode}, message={response.get('message', '')}")
                return {}
            
            # 提取用户数据
            data = response.get('data', {})
            if not data:
                logger.warning(f"❌ API返回空数据: QQ={qq}")
                return {}
            
            # 格式化返回数据
            user_info = {
                'nickname': data.get('nickname', ''),
                'card': data.get('card', ''),
                'role': data.get('role', 'member'),
                'sex': data.get('sex', ''),
                'age': data.get('age', 0),
                'level': data.get('level', ''),
                'qq_level': data.get('qq_level', 0),
                'join_time': data.get('join_time', 0),
                'last_sent_time': data.get('last_sent_time', 0),
                'is_robot': data.get('is_robot', False),
                'shut_up_timestamp': data.get('shut_up_timestamp', 0),
                'title': data.get('title', ''),
                'raw_data': data  # 保存原始数据用于调试
            }
            
            logger.debug(f"✅ 成功获取用户信息: {user_info['card'] or user_info['nickname']} (QQ: {qq})")
            return user_info
            
        except Exception as e:
            logger.error(f"❌ 查询用户 {qq} 信息异常: {e}", exc_info=True)
            return {}

    def _build_mixed_content(self, parsed: ParsedMessage) -> str:
        """构建混合消息内容"""
        parts = []
        
        if parsed.text_parts:
            parts.append(parsed.text_content)
        
        for img in parsed.images:
            parts.append(self._format_image_description(img))
        
        for media in parsed.media_items:
            parts.append(self._format_media_description(media))
        
        return '\n'.join(parts)
    
    def _format_image_description(self, img: Dict) -> str:
        """格式化图片描述"""
        url = img['url']
        file_name = img.get('file', '')
        size = img.get('size', '')
        
        if size:
            try:
                size_str = self._format_file_size(int(size))
                return f'[图片: {file_name}, {size_str}]\n{url}'
            except (ValueError, TypeError):
                pass
        
        file_part = f': {file_name}' if file_name else ''
        return f'[图片{file_part}]\n{url}'
    
    def _format_media_description(self, media: Dict) -> str:
        """格式化媒体描述"""
        type_names = {'video': locale.type('video'), 'voice': locale.type('voice'), 'file': locale.type('file')}
        type_name = type_names.get(media['type'], media['type'])
        file_part = f': {media["file"]}' if media.get('file') else ''
        return f'[{type_name}{file_part}]\n{media["url"]}'
    
    @staticmethod
    def _format_file_size(size_bytes: int) -> str:
        """格式化文件大小"""
        if size_bytes < 1024:
            return f'{size_bytes}B'
        elif size_bytes < 1024 * 1024:
            return f'{size_bytes/1024:.1f}KB'
        else:
            return f'{size_bytes/(1024*1024):.1f}MB'
    
    @staticmethod
    def _create_result(msg_type: str, content: Any, **kwargs) -> Dict[str, Any]:
        """创建统一的返回结果"""
        result = {'type': msg_type, 'content': content}
        result.update(kwargs)
        return result

# 全局实例（异步模式）
message_extractor= MessageContentExtractor(logger)
