import asyncio
import logging
import os
import re
import threading
from asyncio import Queue
from io import BytesIO
from typing import Any, Dict, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError

import config
from api import qq_contacts
from api.qq_api import qq_api
from api.telegram_sender import telegram_sender
from config import LOCALE as locale
from service.telethon_client import get_client, get_user_id
from utils import tools
from utils.contact_manager import contact_manager
from utils.file_processor import async_file_processor
from utils.message_extractor import message_extractor
from utils.message_mapper import msgid_mapping

logger = logging.getLogger(__name__)

tg_user_id = get_user_id()

black_list = ['open_chat', 'bizlivenotify', 'qy_chat_update', 74, 'paymsg', 87, 'secmsg', 'NewXmlShowChatRoomAnnouncement']

message_types = {
    'private': '私聊消息',
    'group': '群聊消息',
    'temp': '临时消息'
}

role_names = {
    'owner': '群主',
    'admin': '管理员',
    'member': '普通成员'
}

notice_types = {
    'group_increase': '群成员增加',
    'group_decrease': '群成员减少',
    'group_recall': '群消息撤回',
    'friend_recall': '好友消息撤回',
    'group_admin': '群管理员变动',
    'group_ban': '群禁言',
    'friend_add': '好友添加'
}

async def is_blacklisted(contact_name: str, sender_name: str, content: str, push_content: str = "") -> bool:
    """
    检查消息是否在黑名单中（智能检测正则表达式）
    """
    if not getattr(config, 'ENABLE_BLACKLIST', True):
        return False
    
    blacklist_keywords = getattr(config, 'BLACKLIST_KEYWORDS', [])
    if not blacklist_keywords:
        return False
    
    check_texts = [
        contact_name or "",
        sender_name or "",
        push_content or "",
    ]
    
    if isinstance(content, str):
        check_texts.append(content)
    
    for keyword in blacklist_keywords:
        if not keyword or not keyword.strip():
            continue
            
        keyword = keyword.strip()
        
        # 先尝试作为正则表达式
        try:
            pattern = re.compile(keyword, re.IGNORECASE)
            
            # 检查是否为"简单"正则（只是普通字符串）
            # 如果正则和原字符串完全一样，说明没有特殊字符
            is_simple_string = (keyword == re.escape(keyword))
            
            for text in check_texts:
                if not text:
                    continue
                    
                if is_simple_string:
                    # 简单字符串，使用包含匹配
                    if keyword.lower() in text.lower():
                        logger.info(f"🚫 消息被黑名单过滤(字符串): 关键词='{keyword}', 发送者='{sender_name}'")
                        return True
                else:
                    # 复杂正则，使用正则匹配
                    if pattern.search(text):
                        logger.info(f"🚫 消息被黑名单过滤(正则): 模式='{keyword}', 匹配文本='{text[:50]}...', 发送者='{sender_name}'")
                        return True
                        
        except re.error:
            # 正则编译失败，作为普通字符串处理
            keyword_lower = keyword.lower()
            for text in check_texts:
                if text and keyword_lower in text.lower():
                    logger.info(f"🚫 消息被黑名单过滤(字符串): 关键词='{keyword}', 发送者='{sender_name}'")
                    return True
    
    return False

def _get_message_handlers():
    """返回消息类型处理器映射"""
    return {
        "text": _forward_text,
        "image": _forward_image,
        "images": _forward_images,
        "sticker": _forward_sticker,
        "voice": _forward_voice,
        "video": _forward_video,
        "file": _forward_file,
        "reply": _forward_reply,
        "forward": _forward_forward,
        "mixed": _forward_mixed
    }

async def _forward_text(chat_id: int, sender_info: str, message_data: Dict[str, Any]) -> None:
    """转发文本消息"""
    try:
        message_content = message_data.get('content', f"[{locale.type('text')}]")
        send_text = f"{sender_info}\n{message_content}"
        return await telegram_sender.send_text(chat_id, send_text)
    except Exception as e:
        logger.error(f"❌ 转发文本消息失败: {e}")
        raise

async def _forward_image(chat_id: int, sender_info: str, message_data: Dict[str, Any]) -> None:
    """转发单张图片消息"""
    try:
        image_url = message_data.get('content', '')
        text_content = message_data.get('text', '')

        # 构建 caption
        caption = sender_info.strip()
        if text_content:
            caption += f"\n{text_content}"

        return await async_file_processor.send_with_placeholder(
            'photo', f"[{locale.type('image')}]",
            chat_id, caption,
            tools.download_file_to_bytesio,
            image_url, "photo"
        )
            
    except Exception as e:
        logger.error(f"❌ 转发图片消息失败: {e}")
        # 失败时发送文本提示
        image_url = message_data.get('content', '')
        text_content = message_data.get('text', '')
        send_text = f"{sender_info}\n[転送失敗]\n{image_url}"
        if text_content:
            send_text = f"{sender_info}\n{text_content}\n[転送失敗]\n{image_url}"
        return await telegram_sender.send_text(chat_id, send_text)

async def _forward_sticker(chat_id: int, sender_info: str, message_data: Dict[str, Any]) -> None:
    """转发单张图片消息"""
    try:
        image_url = message_data.get('content', '')
        text_content = message_data.get('text', '')

        # 构建 caption
        caption = sender_info.strip()
        if text_content:
            caption += f"\n{text_content}"

        return await async_file_processor.send_with_placeholder(
            'animation', f"[{locale.type('sticker')}].gif",
            chat_id, caption,
            tools.download_file_to_bytesio,
            image_url, "sticker"
        )
            
    except Exception as e:
        logger.error(f"❌ 转发贴纸消息失败: {e}")
        # 失败时发送文本提示
        image_url = message_data.get('content', '')
        text_content = message_data.get('text', '')
        send_text = f"{sender_info}\n[転送失敗]\n{image_url}"
        if text_content:
            send_text = f"{sender_info}\n{text_content}\n[転送失敗]\n{image_url}"
        return await telegram_sender.send_text(chat_id, send_text)

async def _forward_images(chat_id: int, sender_info: str, message_data: Dict[str, Any]) -> None:
    """转发多张图片消息（相册）"""
    try:
        image_list = message_data.get('content', [])
        text_content = message_data.get('text', '')
        
        # 下载所有图片
        from telegram import InputMediaPhoto
        media_group = []
        
        for i, img_info in enumerate(image_list):
            image_url = img_info['url']
            logger.debug(f"   下载第 {i+1}/{len(image_list)} 张图片: {image_url}")
            
            # 从URL下载图片
            image_bytesio, file_name = await tools.download_file_to_bytesio(image_url, "photo")
            
            if image_bytesio:
                # 第一张图片添加caption（包含发送者信息和文本）
                if i == 0:
                    caption = sender_info.strip()
                    if text_content:
                        caption += f"\n{text_content}"
                    media_group.append(InputMediaPhoto(
                        media=image_bytesio,
                        caption=caption
                    ))
                else:
                    media_group.append(InputMediaPhoto(media=image_bytesio))
            else:
                logger.warning(f"下载第 {i+1} 张图片失败: {image_url}")
        
        # 发送媒体组
        if media_group:
            if len(media_group) == 1:
                # 如果只成功下载了一张，用send_photo发送
                return await telegram_sender.send_photo(
                    chat_id,
                    media_group[0].media,
                    media_group[0].caption or sender_info.strip()
                )
            else:
                # 发送媒体组
                return await telegram_sender.send_media_group(
                    chat_id,
                    media_group
                )
        else:
            # 所有图片都下载失败
            logger.error("所有图片下载失败")
            urls_text = '\n'.join([img['url'] for img in image_list])
            send_text = f"{sender_info}\n[{len(image_list)}张图片下载失败]\n{urls_text}"
            if text_content:
                send_text = f"{sender_info}\n{text_content}\n[{len(image_list)}张图片下载失败]\n{urls_text}"
            return await telegram_sender.send_text(chat_id, send_text)
            
    except Exception as e:
        logger.error(f"❌ 转发图片组失败: {e}")
        # 失败时发送文本提示
        image_list = message_data.get('images', [])
        text_content = message_data.get('text', '')
        urls_text = '\n'.join([img['url'] for img in image_list])
        send_text = f"{sender_info}\n[{len(image_list)}张图片发送失败]\n{urls_text}"
        if text_content:
            send_text = f"{sender_info}\n{text_content}\n[{len(image_list)}张图片发送失败]\n{urls_text}"
        return await telegram_sender.send_text(chat_id, send_text)

async def _forward_video(chat_id: int, sender_info: str, message_data: Dict[str, Any]) -> None:
    """转发视频消息"""
    try:
        video_url = message_data.get('content', '')
        text_content = message_data.get('text', '')
        
        # 构建 caption
        caption = sender_info.strip()
        if text_content:
            caption += f"\n{text_content}"

        return await async_file_processor.send_with_placeholder(
            'video', f"[{locale.type('video')}]",
            chat_id, caption,  # ✅ 使用包含文字的 caption
            tools.download_file_to_bytesio,
            video_url, "video"
        )

    except Exception as e:
        logger.error(f"❌ 转发视频消息失败: {e}")
        video_url = message_data.get('content', '')
        text_content = message_data.get('text', '')
        send_text = f"{sender_info}\n[视频] {video_url}"
        if text_content:
            send_text = f"{sender_info}\n{text_content}\n[视频] {video_url}"
        await telegram_sender.send_text(chat_id, send_text)

async def _forward_voice(chat_id: int, sender_info: str, message_data: Dict[str, Any]) -> None:
    """转发语音消息"""
    try:
        voice_url = message_data.get('content', '')
        text_content = message_data.get('text', '')
        
        # 构建说明文字
        caption = sender_info.strip()
        if text_content:
            caption += f"\n{text_content}"
        
        await telegram_sender.send_voice(
            chat_id,
            voice_url,
            caption  # ✅ 语音说明包含文字
        )
    except Exception as e:
        logger.error(f"❌ 转发语音消息失败: {e}")
        voice_url = message_data.get('content', '')
        text_content = message_data.get('text', '')
        send_text = f"{sender_info}\n[语音] {voice_url}"
        if text_content:
            send_text = f"{sender_info}\n{text_content}\n[语音] {voice_url}"
        await telegram_sender.send_text(chat_id, send_text)

async def _forward_file(chat_id: int, sender_info: str, message_data: Dict[str, Any]) -> None:
    """转发文件消息"""
    try:
        file_url = message_data.get('content', '')
        text_content = message_data.get('text', '')
        
        # 构建 caption
        caption = sender_info.strip()
        if text_content:
            caption += f"\n{text_content}"

        return await async_file_processor.send_with_placeholder(
            'document', f"[{locale.type('file')}]",
            chat_id, caption,  # ✅ 使用包含文字的 caption
            tools.download_file_to_bytesio,
            file_url, "file"
        )
    
    except Exception as e:
        logger.error(f"❌ 转发文件消息失败: {e}")
        file_url = message_data.get('content', '')
        text_content = message_data.get('text', '')
        send_text = f"{sender_info}\n[文件] {file_url}"
        if text_content:
            send_text = f"{sender_info}\n{text_content}\n[文件] {file_url}"
        await telegram_sender.send_text(chat_id, send_text)

async def _forward_reply(chat_id: int, sender_info: str, message_data: Dict[str, Any]) -> None:
    """处理QQ引用/回复消息"""
    try:
        message_array = message_data.get('message', [])
        # 提取引用的消息ID
        reply_id = None
        text_content = ""
        
        for msg_item in message_array:
            msg_type = msg_item.get('type')
            msg_data = msg_item.get('data', {})
            
            if msg_type == 'reply':
                # 获取被引用的消息ID
                reply_id = msg_data.get('id')
            elif msg_type == 'text':
                # 拼接文本内容（跳过@和空格）
                text = msg_data.get('text', '')
                if text.strip():  # 只添加非空文本
                    text_content += text
        
        # 查询被引用消息对应的TG消息ID
        reply_tg_msgid = 0
        if reply_id:
            reply_tg_msgid = await msgid_mapping.qq_to_tg(reply_id) or 0
            logger.debug(f"   引用消息: QQ={reply_id} -> TG={reply_tg_msgid}")
        
        # 构建发送文本
        send_text = sender_info.strip()
        if text_content:
            send_text += f"\n{text_content.strip()}"
        
        # 发送消息（带引用）
        return await telegram_sender.send_text(
            chat_id,
            send_text,
            reply_to_message_id=reply_tg_msgid if reply_tg_msgid else None
        )
        
    except Exception as e:
        logger.error(f"❌ 转发引用消息失败: {e}")
        # 降级处理：发送不带引用的消息
        try:
            text_content = message_data.get('raw_message', '')
            # 移除CQ码
            text_content = re.sub(r'\[CQ:.*?\]', '', text_content).strip()
            send_text = f"{sender_info}\n{text_content}" if text_content else sender_info
            return await telegram_sender.send_text(chat_id, send_text)
        except Exception as fallback_error:
            logger.error(f"❌ 降级发送也失败: {fallback_error}")
            return None

async def _forward_mixed(chat_id: int, sender_info: str, message_data: Dict[str, Any]) -> None:
    """转发混合类型消息"""
    try:
        message_content = message_data.get('content', f"[{locale.type('unknown')}]")
        send_text = f"{sender_info}\n{message_content}"
        await telegram_sender.send_text(chat_id, send_text)
    except Exception as e:
        logger.error(f"❌ 转发混合消息失败: {e}")
        raise

async def _forward_forward(chat_id: int, sender_info: str, message_data: Dict[str, Any]) -> None:
    """处理QQ转发消息（支持嵌套转发）"""
    try:
        # 获取转发消息的内容数组
        msg_id = message_data.get('content', 0)

        payload = {
            "message_id": int(msg_id)
        }

        forward_json = await qq_api("GET_FORWARD", payload)
        forward_content = forward_json.get("data", {}).get("messages", [])
        
        if not forward_content:
            logger.warning("转发消息内容为空")
            return await telegram_sender.send_text(
                chat_id, 
                f"{sender_info}\n[{locale.type('forward')}]"
            )
        
        # 递归处理转发内容
        return await _process_forward_content(chat_id, sender_info, forward_content, depth=0)
        
    except Exception as e:
        logger.error(f"❌ 转发消息处理失败: {e}", exc_info=True)
        fallback_text = f"{sender_info}\n[转发消息处理失败]"
        return await telegram_sender.send_text(chat_id, fallback_text)

async def _process_forward_content(chat_id: int, sender_info: str, forward_content: list, depth: int = 0) -> None:
    """递归处理转发内容"""
    try:
        # 限制递归深度，防止无限嵌套
        MAX_DEPTH = 5
        if depth > MAX_DEPTH:
            logger.warning(f"转发嵌套深度超过限制 ({MAX_DEPTH})，停止递归")
            return await telegram_sender.send_text(
                chat_id, 
                f"[合并转发嵌套过深，已省略 (深度: {depth})]"
            )
        
        # 构建预览文本和收集媒体文件
        preview_title = []
        preview_lines = []
        
        # 根据嵌套深度调整标题
        indent = "  " * depth  # 缩进表示嵌套层级
        depth_tip = f" (层级: {depth + 1})" if depth > 0 else ""
        preview_title.append(f"{indent}[{locale.type('forward')}]{depth_tip}")
        preview_title.append(f"{indent}件数: {len(forward_content)}")
        
        all_media = []  # 收集所有媒体文件（图片和视频）
        media_counter = 0  # 媒体文件计数器
        nested_forwards = []  # 收集嵌套的转发消息
        
        # 遍历所有转发的消息，生成预览
        for idx, forwarded_msg in enumerate(forward_content, 1):
            try:
                # 获取原始发送者信息
                original_sender = forwarded_msg.get('sender', {})
                original_nickname = original_sender.get('nickname', 'QQ用户')
                original_card = original_sender.get('card', '')
                display_name = original_card if original_card else original_nickname
                
                # 提取消息内容和类型
                forwarded_message_data = await message_extractor.extract(forwarded_msg)
                content_type = forwarded_message_data['type']
                
                # 根据消息类型生成预览文本
                if content_type == 'forward':
                    # 嵌套转发 - 收集起来稍后递归处理
                    nested_forward_id = forwarded_message_data.get('content', 0)
                    preview_lines.append(f"{indent}👤{display_name}: ")
                    preview_lines.append(f"{indent}[{locale.type('forward')}] (嵌套)")
                    
                    # 收集嵌套转发信息
                    nested_forwards.append({
                        'msg_id': nested_forward_id,
                        'sender': display_name,
                        'depth': depth + 1
                    })
                    
                elif content_type == 'image':
                    # 单张图片
                    image_url = forwarded_message_data.get('content', '')
                    text_content = forwarded_message_data.get('text', '')
                    
                    preview_lines.append(f"{indent}👤{display_name}: ")
                    if text_content:
                        preview_lines.append(f"{indent}{text_content}")
                    
                    media_counter += 1
                    preview_lines.append(f"{indent}[写真]{media_counter}")
                    
                    # 收集图片URL
                    if image_url:
                        all_media.append({
                            'type': 'photo',
                            'url': image_url,
                            'sender': display_name,
                            'text': text_content,
                            'depth': depth
                        })
                        
                elif content_type == 'images':
                    # 多张图片
                    image_list = forwarded_message_data.get('content', [])
                    text_content = forwarded_message_data.get('text', '')
                    
                    preview_lines.append(f"{indent}👤{display_name}: ")
                    if text_content:
                        preview_lines.append(f"{indent}{text_content}")
                    
                    # 为每张图片添加预览和收集URL
                    for img_info in image_list:
                        media_counter += 1
                        preview_lines.append(f"{indent}[{locale.type('image')}]{media_counter}")
                        all_media.append({
                            'type': 'photo',
                            'url': img_info.get('url', ''),
                            'sender': display_name,
                            'text': text_content if len(image_list) == 1 else '',
                            'depth': depth
                        })
                        
                elif content_type == 'video':
                    # 视频消息
                    video_url = forwarded_message_data.get('content', '')
                    text_content = forwarded_message_data.get('text', '')
                    
                    preview_lines.append(f"{indent}👤{display_name}: ")
                    if text_content:
                        preview_lines.append(f"{indent}{text_content}")
                    
                    media_counter += 1
                    preview_lines.append(f"{indent}[{locale.type('video')}]{media_counter}")
                    
                    # 收集视频URL
                    if video_url:
                        all_media.append({
                            'type': 'video',
                            'url': video_url,
                            'sender': display_name,
                            'text': text_content,
                            'depth': depth
                        })
                        
                elif content_type == 'text':
                    # 文本消息
                    message_content = forwarded_message_data.get('content', '')
                    preview_lines.append(f"{indent}👤{display_name}: ")
                    preview_lines.append(f"{indent}{message_content}")
                    
                elif content_type == 'sticker':
                    # 表情包
                    preview_lines.append(f"{indent}👤{display_name}: ")
                    preview_lines.append(f"{indent}[{locale.type('sticker')}]")
                    
                elif content_type == 'voice':
                    # 语音消息
                    preview_lines.append(f"{indent}👤{display_name}: ")
                    preview_lines.append(f"{indent}[{locale.type('voice')}]")
                    
                elif content_type == 'file':
                    # 文件消息
                    preview_lines.append(f"{indent}👤{display_name}: ")
                    preview_lines.append(f"{indent}[{locale.type('file')}]")
                    
                elif content_type == 'reply':
                    # 回复消息
                    text_content = forwarded_message_data.get('content', '')
                    preview_lines.append(f"{indent}👤{display_name}: ")
                    preview_lines.append(f"{indent}[{locale.type('reply')}] {text_content}")
                    
                else:
                    # 其他类型消息
                    message_content = forwarded_message_data.get('content', f"[{locale.type('unknown')}]")
                    preview_lines.append(f"{indent}👤{display_name}: ")
                    preview_lines.append(f"{indent}{message_content}")
                    
            except Exception as e:
                logger.error(f"❌ 处理第{idx}条转发消息预览失败: {e}")
                preview_lines.append(f"{indent}👤未知用户: ")
                preview_lines.append(f"{indent}[第{idx}条消息处理失败]")
        
        # 构建完整的预览文本
        preview_title.append(f"{indent}媒体: {media_counter}")
        if nested_forwards:
            preview_title.append(f"{indent}嵌套转发: {len(nested_forwards)}")
        
        preview_text = "\n".join(preview_title + preview_lines)
        
        # 发送预览消息（使用折叠引用块）
        if depth == 0:
            # 顶层转发包含发送者信息
            forward_preview = f"{sender_info}\n<blockquote expandable>{preview_text}\n</blockquote>"
        else:
            # 嵌套转发不重复发送者信息
            forward_preview = f"<blockquote expandable>{preview_text}\n</blockquote>"
        
        preview_response = None
        
        # 如果有媒体文件，分批发送媒体组
        if all_media:
            try:
                from telegram import InputMediaPhoto, InputMediaVideo
                
                # 统计媒体类型
                photo_count = sum(1 for media in all_media if media['type'] == 'photo')
                video_count = sum(1 for media in all_media if media['type'] == 'video')
                
                logger.info(f"开始下载 {len(all_media)} 个媒体文件 (图片: {photo_count}, 视频: {video_count}) [深度: {depth}]...")
                
                # 分批处理媒体文件（每批最多10个）
                BATCH_SIZE = 10
                total_batches = (len(all_media) + BATCH_SIZE - 1) // BATCH_SIZE
                
                for batch_idx in range(total_batches):
                    start_idx = batch_idx * BATCH_SIZE
                    end_idx = min(start_idx + BATCH_SIZE, len(all_media))
                    batch_media = all_media[start_idx:end_idx]
                    
                    logger.info(f"处理第 {batch_idx + 1}/{total_batches} 批媒体文件 ({len(batch_media)} 个) [深度: {depth}]")
                    
                    media_group = []
                    
                    for i, media_info in enumerate(batch_media):
                        media_url = media_info['url']
                        media_type = media_info['type']
                        sender_name = media_info['sender']
                        text_content = media_info['text']
                        
                        global_idx = start_idx + i + 1  # 全局索引
                        logger.debug(f"   下载第 {global_idx}/{len(all_media)} 个{media_type}: {media_url}")
                        
                        # 根据类型确定文件类型参数
                        file_type = "photo" if media_type == 'photo' else "video"
                        
                        # 从URL下载媒体文件
                        media_bytesio, file_name = await tools.download_file_to_bytesio(media_url, file_type)
                        
                        if media_bytesio:
                            # 第一批的第一个媒体文件添加完整caption，其他批次添加批次信息
                            caption = None
                            if batch_idx == 0 and i == 0:
                                # 第一批第一个文件：完整预览
                                caption = forward_preview
                            elif i == 0:
                                # 其他批次第一个文件：批次信息
                                start_num = batch_idx * 10 + 1
                                end_num = min(batch_idx * 10 + 10, media_counter)
                                depth_info = f" (层级: {depth + 1})" if depth > 0 else ""
                                caption = f"<blockquote>[{locale.type('forward')}]{depth_info} ({start_num} ~ {end_num})</blockquote>"
                            
                            # 根据类型创建对应的InputMedia对象
                            if media_type == 'photo':
                                media_group.append(InputMediaPhoto(
                                    media=media_bytesio,
                                    caption=caption
                                ))
                            else:  # video
                                media_group.append(InputMediaVideo(
                                    media=media_bytesio,
                                    caption=caption
                                ))
                        else:
                            logger.warning(f"下载第 {global_idx} 个{media_type}失败: {media_url}")
                    
                    # 发送当前批次的媒体组
                    if media_group:
                        if len(media_group) == 1:
                            # 如果只有一个文件，根据类型单独发送
                            media_item = media_group[0]
                            depth_info = f" (层级: {depth + 1})" if depth > 0 else ""
                            if isinstance(media_item, InputMediaPhoto):
                                batch_response = await telegram_sender.send_photo(
                                    chat_id,
                                    media_item.media,
                                    media_item.caption or f"📋 转发消息中的图片{depth_info} (第 {batch_idx + 1} 批)"
                                )
                            else:  # InputMediaVideo
                                batch_response = await telegram_sender.send_video(
                                    chat_id,
                                    media_item.media,
                                    media_item.caption or f"📋 转发消息中的视频{depth_info} (第 {batch_idx + 1} 批)"
                                )
                        else:
                            # 发送媒体组
                            batch_response = await telegram_sender.send_media_group(
                                chat_id,
                                media_group
                            )
                        
                        # 保存第一批的响应用于消息映射
                        if batch_idx == 0 and depth == 0:
                            preview_response = batch_response
                        
                        logger.info(f"✅ 成功发送第 {batch_idx + 1} 批 {len(media_group)} 个媒体文件 [深度: {depth}]")
                        
                        # 批次间添加小延迟，避免触发限制
                        if batch_idx < total_batches - 1:
                            await asyncio.sleep(1)
                    else:
                        logger.warning(f"第 {batch_idx + 1} 批媒体文件全部下载失败 [深度: {depth}]")
                
                logger.info(f"✅ 媒体文件发送完成，共 {total_batches} 批 [深度: {depth}]")
                
            except Exception as e:
                logger.error(f"❌ 发送转发消息媒体文件失败 [深度: {depth}]: {e}")
                error_text = f"❌ 转发消息中的媒体文件发送失败 [深度: {depth}]: {str(e)}"
                if depth == 0:
                    preview_response = await telegram_sender.send_text(chat_id, error_text)
        else:
            # 没有媒体文件，只发送预览文本
            if depth == 0:
                preview_response = await telegram_sender.send_text(chat_id, forward_preview)
        
        # 递归处理嵌套转发
        for nested_forward in nested_forwards:
            try:
                logger.info(f"处理嵌套转发 [深度: {nested_forward['depth']}]: {nested_forward['msg_id']}")
                
                # 获取嵌套转发内容
                payload = {
                    "message_id": int(nested_forward['msg_id'])
                }
                
                nested_forward_json = await qq_api("GET_FORWARD", payload)
                nested_forward_content = nested_forward_json.get("data", {}).get("messages", [])
                
                if nested_forward_content:
                    # 递归处理嵌套转发
                    nested_sender_info = f"🔄 嵌套转发 (来自: {nested_forward['sender']})"
                    await _process_forward_content(
                        chat_id, 
                        nested_sender_info, 
                        nested_forward_content, 
                        depth=nested_forward['depth']
                    )
                else:
                    logger.warning(f"嵌套转发内容为空: {nested_forward['msg_id']}")
                    
            except Exception as e:
                logger.error(f"❌ 处理嵌套转发失败 [深度: {nested_forward['depth']}]: {e}")
                error_text = f"❌ 嵌套转发处理失败 (来自: {nested_forward['sender']}): {str(e)}"
                await telegram_sender.send_text(chat_id, error_text)
        
        logger.info(f"✅ 转发消息处理完成 [深度: {depth}]，共{len(forward_content)}条消息，{len(all_media)}个媒体文件，{len(nested_forwards)}个嵌套转发")
        
        # 返回预览消息的响应（用于消息映射，只有顶层转发才返回）
        if depth == 0:
            return preview_response
        
    except Exception as e:
        logger.error(f"❌ 转发内容处理失败 [深度: {depth}]: {e}", exc_info=True)
        if depth == 0:
            fallback_text = f"{sender_info}\n[转发消息处理失败]"
            return await telegram_sender.send_text(chat_id, fallback_text)

async def _get_sender_info(data: Dict[str, Any], is_self_sent: bool = False) -> str:
    """
    获取发送者信息字符串
    
    Args:
        data: 消息数据
        is_self_sent: 是否为自己发送的消息
        
    Returns:
        str: 格式化的发送者信息
    """
    try:
        message_type = data.get('message_type', 'unknown')
        
        # 统一获取发送者ID
        if is_self_sent:
            sender_id = data.get('self_id', data.get('user_id', 'unknown'))
        else:
            sender_id = data.get('user_id', 'unknown')
        
        # 获取发送者信息
        sender = data.get('sender', {})
        nickname = sender.get('nickname', f'用户{sender_id}')
        card = sender.get('card', '')
        role = sender.get('role', 'unknown')
        
        if message_type == 'group':
            group_id = data.get('group_id', 'unknown')
            group_name = data.get('group_name', '未知群组')
            
            # 构建发送者显示名称
            display_name = card if card else nickname
            
            # 区分自己发送和他人发送的显示格式
            if is_self_sent:
                return f"<blockquote>{display_name}: </blockquote>"
            else:
                return f"<blockquote>{display_name}: </blockquote>"
                
        elif message_type == 'private':
            # 构建发送者显示名称
            if is_self_sent:
                return f"<blockquote>{nickname} (我): </blockquote>"
            else:
                return f""
                
        else:
            # 其他类型消息
            type_name = message_types.get(message_type, f'未知消息({message_type})')
            return f"📱 QQ消息 ({type_name}): {nickname}{'(我)' if is_self_sent else ''}\n"
            
    except Exception as e:
        logger.error(f"❌ 获取发送者信息失败: {e}")
        return locale.common('unknown')

async def _create_group_for_contact(qqid: str, contact_name: str, avatar_url: str = None, is_group: bool = False) -> Optional[int]:
    """异步创建群组"""
    try:
        if not qqid or not contact_name:
            logger.error(f"参数无效: qqid={qqid}, contact_name={contact_name}")
            return None
        
        result = await contact_manager.create_group_for_contact_async(
            qqid=qqid,
            contact_name=contact_name,
            avatar_url=avatar_url,
            is_group=is_group
        )
        
        if result and result.get('success'):
            chat_id = result['chat_id']
            return chat_id
        else:
            error_msg = result.get('error', '未知错误') if result else '返回结果为空'
            logger.error(f"群组创建失败: {qqid}, 错误: {error_msg}")
            return None
            
    except Exception as e:
        logger.error(f"创建群组异常: {e}", exc_info=True)
        return None

async def _get_or_create_chat(target_qq_id: str, sender_name: str, avatar_url: str, is_group: bool = False, message_for_log = None) -> Optional[int]:
    """获取或创建聊天群组"""
    # 读取contact映射
    contact_dic = await contact_manager.get_contact(target_qq_id)
    
    if contact_dic and not contact_dic.is_receive:
        return None

    # 检查是否已有有效的chatId
    if contact_dic and contact_dic.is_receive and contact_dic.chat_id != -9999999999:
        return contact_dic.chat_id
    
    # 检查是否允许自动创建群组
    auto_create = getattr(config, 'AUTO_CREATE_GROUPS', True)

    # 指定不创建群组的情况
    if not auto_create or target_qq_id == config.MY_QQ_ID:
        return None
    
    # 创建群组
    logger.warning(f"触发新建群组：{target_qq_id}")
    chat_id = await _create_group_for_contact(target_qq_id, sender_name, avatar_url, is_group)
    if not chat_id:
        logger.warning(f"无法创建聊天群组: {target_qq_id}")
        await telegram_sender.send_text(tg_user_id, f"{locale.common('failed_to_create_group')}")
        return None
    
    return chat_id 

async def _process_message_async(message: Dict[str, Any]) -> None:
    """异步处理单条消息"""
    try:
        is_group = False
        post_type = message.get('post_type', 'unknown')
        group_id = message.get('group_id')
        if group_id:
            is_group = True
            target_qq_id = group_id
            group_name = message.get('group_name')
        else:
            private_id = message.get('target_id') or message.get('user_id')
            target_qq_id = private_id
            group_name = None
        
        user_info = await qq_contacts.get_user_info(target_qq_id, is_group, group_name)
        
        logger.info(f"📨 调试: {message}")
        
        # 不转发自己
        if target_qq_id == int(config.MY_QQ_ID): return
        
        # 匹配或新建tg群组并返回chat_id
        target_chat_id = await _get_or_create_chat(target_qq_id, user_info.name, user_info.avatar_url, is_group)
        if not target_chat_id:
            return

        # 统一处理接收和发送的消息
        if post_type == 'message' or post_type == 'message_sent':
            
            # 不转发自己
            if post_type == 'message_sent': return
                
            await _handle_message_event(target_chat_id, message)
            
        elif post_type == 'notice':
            await _handle_notice_event(target_chat_id, message)
            
        elif post_type == 'request':
            await _handle_request_event(get_user_id(), message)
            
        elif post_type == 'meta_event':
            _log_meta_event(message)
            
        else:
            logger.warning(f"❓ 未知事件类型: {post_type}")
            
    except Exception as e:
        logger.error(f"❌ 异步处理QQ回调消息失败: {e}")

async def _handle_message_event(chat_id: int, data: Dict[str, Any]):
    """处理消息事件并转发到Telegram（统一处理接收和发送）"""
    try:
        # 检查是否配置了目标chat_id
        if not chat_id:
            logger.debug("未配置目标chat_id，跳过消息转发")
            return
        
        # 判断是接收消息还是发送消息
        post_type = data.get('post_type', 'message')
        send_id = data.get('user_id', '未知')
        to_id = data.get('group_id') or data.get('target_id') or data.get('user_id')
        
        msg_id = data.get('message_id', 0)
        is_self_sent = (post_type == 'message_sent')
        
        # 获取发送者信息
        sender_info = await _get_sender_info(data, is_self_sent)
        
        # 提取消息内容和类型
        message_data = await message_extractor.extract(data)
        content_type = message_data['type']  # text, image, images, video, voice, etc.
        
        # 获取消息处理器
        handlers = _get_message_handlers()
        handler = handlers.get(content_type, _forward_mixed)
        
        # 使用对应的处理器转发消息
        response = await handler(chat_id, sender_info, message_data)

        # 存储消息映射
        tg_msgid = response.message_id

        # 获取Telethon消息ID
        telethon_msg_id = 0

        await msgid_mapping.add(
            tg_msg_id=tg_msgid,
            from_qq_id=send_id,
            to_qq_id=to_id,
            qq_msg_id=msg_id,
            telethon_msg_id=telethon_msg_id
        )
        
        # 记录原始消息（调试用）
        raw_message = data.get('raw_message', '')
        if raw_message:
            logger.debug(f"原始消息: {raw_message}")
            
    except Exception as e:
        logger.error(f"❌ 处理并转发消息失败: {e}", exc_info=True)

async def _handle_notice_event(chat_id: int, data: Dict[str, Any]):
    """处理通知事件并转发到Telegram"""
    try:
        # 检查是否配置了目标chat_id
        if not chat_id:
            logger.debug("未配置目标chat_id，跳过通知转发")
            return
        logger.warning(f"调试：{data}")
        notice_type = data.get('notice_type', 'unknown')
        type_name = notice_types.get(notice_type, f'未知通知({notice_type})')
        
        logger.info(f"🔔 {type_name}")
        
        send_text = None
        
        if notice_type == 'group_recall' or notice_type == 'friend_recall':
            group_id = data.get('group_id', 'unknown')
            user_id = data.get('user_id', 'unknown')
            operator_id = data.get('operator_id', 'unknown')
            message_id = data.get('message_id', 'unknown')
            
            if operator_id != int(config.MY_QQ_ID):
                quote_tgmsgid = await msgid_mapping.qq_to_tg(message_id)
                send_text = f"<blockquote>{locale.common('revoke_message')}</blockquote>"
                if quote_tgmsgid:
                    return await telegram_sender.send_text(chat_id, send_text, reply_to_message_id=quote_tgmsgid)
            
        elif notice_type == 'group_increase':
            group_id = data.get('group_id', 'unknown')
            user_id = data.get('user_id', 'unknown')
            operator_id = data.get('operator_id', 'unknown')
            
            if operator_id != user_id:
                logger.info(f"   邀请者: {operator_id}")
                send_text = f"<blockquote>🔔 QQ群成员增加</blockquote>\n新成员: {user_id}\n邀请者: {operator_id}"
            else:
                send_text = f"<blockquote>🔔 QQ群成员增加</blockquote>\n新成员: {user_id}"
                
        elif notice_type == 'group_decrease':
            group_id = data.get('group_id', 'unknown')
            user_id = data.get('user_id', 'unknown')
            operator_id = data.get('operator_id', 'unknown')
            sub_type = data.get('sub_type', 'unknown')
            action = "主动退群" if sub_type == "leave" else "被踢出群" if sub_type == "kick" else f"操作类型({sub_type})"
            
            if operator_id and operator_id != user_id:
                logger.info(f"   操作者: {operator_id}")
                send_text = f"<blockquote>🔔 QQ群成员减少</blockquote>\n成员: {user_id}\n操作: {action}\n操作者: {operator_id}"
            else:
                send_text = f"<blockquote>🔔 QQ群成员减少</blockquote>\n成员: {user_id}\n操作: {action}"
        
        else:
            # 其他通知类型，显示关键字段
            info_parts = [f"<blockquote>🔔 {type_name}</blockquote>"]
            important_fields = ['group_id', 'user_id', 'operator_id', 'sub_type', 'duration']
            for field in important_fields:
                if field in data:
                    logger.info(f"   {field}: {data[field]}")
                    info_parts.append(f"{field}: {data[field]}")
            send_text = "\n".join(info_parts)
        
        # 发送到Telegram
        if send_text:
            await telegram_sender.send_text(chat_id, send_text)
                    
    except Exception as e:
        logger.error(f"❌ 处理并转发通知事件失败: {e}", exc_info=True)

async def _handle_request_event(chat_id: int, data: Dict[str, Any]):
    """处理请求事件并转发到Telegram"""
    try:
        # 检查是否配置了目标chat_id
        if not chat_id:
            logger.debug("未配置目标chat_id，跳过请求转发")
            return
        
        request_type = data.get('request_type', 'unknown')
        type_name = f"好友请求" if request_type == "friend" else "群请求" if request_type == "group" else f"未知请求({request_type})"
        
        logger.info(f"📋 {type_name}")
        
        send_text = None
        
        if request_type == 'friend':
            user_id = data.get('user_id', 'unknown')
            comment = data.get('comment', '')
            flag = data.get('flag', 'unknown')
            
            logger.info(f"   申请者: {user_id}")
            logger.info(f"   验证消息: {comment}")
            logger.info(f"   标识: {flag}")
            
            send_text = f"📋 QQ好友请求\n申请者: {user_id}\n验证消息: {comment}"
            
        elif request_type == 'group':
            group_id = data.get('group_id', 'unknown')
            user_id = data.get('user_id', 'unknown')
            comment = data.get('comment', '')
            sub_type = data.get('sub_type', 'unknown')
            flag = data.get('flag', 'unknown')
            action = "加群申请" if sub_type == "add" else "群邀请" if sub_type == "invite" else f"操作类型({sub_type})"
            
            logger.info(f"   操作: {action}")
            logger.info(f"   群组ID: {group_id}")
            logger.info(f"   用户: {user_id}")
            logger.info(f"   消息: {comment}")
            logger.info(f"   标识: {flag}")
            
            send_text = f"📋 QQ群请求\n操作: {action}\n群组: {group_id}\n用户: {user_id}\n消息: {comment}"
        
        # 发送到Telegram
        if send_text:
            await telegram_sender.send_text(chat_id, send_text)
            logger.info(f"✅ 请求已转发到Telegram (chat_id: {chat_id})")
            
    except Exception as e:
        logger.error(f"❌ 处理并转发请求事件失败: {e}", exc_info=True)

def _log_meta_event(data: Dict[str, Any]):
    """记录元事件（心跳等）"""
    try:
        meta_event_type = data.get('meta_event_type', 'unknown')
        
        if meta_event_type == 'heartbeat':
            # 心跳包不需要详细记录，只在debug级别显示
            status = data.get('status', {})
            online = status.get('online', False)
            logger.debug(f"💓 心跳包 - 在线状态: {online}")
        else:
            logger.info(f"🔄 元事件: {meta_event_type}")
            # 显示其他重要字段
            important_fields = ['interval', 'status', 'self_id']
            for field in important_fields:
                if field in data:
                    logger.info(f"   {field}: {data[field]}")
                    
    except Exception as e:
        logger.error(f"❌ 记录元事件失败: {e}")

async def process_callback_message(message_data: Dict[str, Any]) -> None:
    """处理QQ回调消息"""
    try:
        if not message_data:
            logger.error("提取消息信息失败")
            return
        
        await message_processor.add_message_async(message_data)
            
    except Exception as e:
        logger.error(f"消息处理失败: {e}", exc_info=True)

class MessageProcessor:
    def __init__(self):
        self.queue = None
        self.loop = None
        self._shutdown = False
        self._task = None
        self._init_complete = asyncio.Event()
        self._initialized = False
        
        self._init_async_env()
    
    def ensure_initialized(self):
        """确保处理器已初始化"""
        self._init_async_env()
    
    def _init_async_env(self):
        """在后台线程中初始化异步环境"""
        if self._initialized:  # 防止重复初始化
            return
            
        def run_async():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.queue = Queue(maxsize=1000)
            
            # 启动队列处理器
            self._task = self.loop.create_task(self._process_queue())
            logger.info("消息处理器已启动 (callback模式)")
            
            # 标记初始化完成
            self.loop.call_soon_threadsafe(self._init_complete.set)
            
            # 运行事件循环
            try:
                self.loop.run_forever()
            except Exception as e:
                logger.error(f"消息处理器事件循环异常: {e}")
        
        thread = threading.Thread(target=run_async, daemon=True)
        thread.start()
        self._initialized = True
    
    async def _process_queue(self):
        """处理队列中的消息"""
        while not self._shutdown:
            try:
                # 等待消息
                message = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                
                # 处理消息
                await _process_message_async(message)
                self.queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"处理消息失败: {e}", exc_info=True)
    
    async def add_message_async(self, message_info: Dict[str, Any]):
        """添加消息到队列"""
        self.ensure_initialized()  # 确保初始化
        
        # 等待初始化完成
        if not self._init_complete.is_set():
            await asyncio.wait_for(self._init_complete.wait(), timeout=5.0)
        
        if not self.queue:
            logger.error("处理器未就绪")
            return
        
        try:
            # 如果在同一个事件循环中，直接添加
            if asyncio.get_event_loop() == self.loop:
                await self.queue.put(message_info)
            else:
                # 跨线程调用
                future = asyncio.run_coroutine_threadsafe(
                    self.queue.put(message_info), self.loop
                )
                await asyncio.wrap_future(future)
        except Exception as e:
            logger.error(f"异步添加消息到队列失败: {e}")
    
    async def shutdown(self):
        """优雅关闭处理器"""
        if not self._initialized:
            return
            
        logger.info("正在关闭消息处理器...")
        self._shutdown = True
        
        if self.queue:
            # 等待队列处理完成
            try:
                await asyncio.wait_for(self.queue.join(), timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("等待队列处理完成超时")
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
        
        logger.info("消息处理器已关闭")
    
    def get_queue_size(self) -> int:
        """获取队列大小"""
        if self.queue:
            return self.queue.qsize()
        return 0

# 全局实例
message_processor = MessageProcessor()

# 优雅关闭函数
async def shutdown_message_processor():
    """关闭消息处理器"""
    await message_processor.shutdown()
