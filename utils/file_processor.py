import asyncio
import logging
from io import BytesIO
from typing import Callable, Any

from api.telegram_sender import telegram_sender

logger = logging.getLogger(__name__)

class AsyncFileProcessor:
    def __init__(self, telegram_sender):
        self.telegram_sender = telegram_sender
    
    def _create_placeholder_file(self, file_name: str) -> BytesIO:
        """创建1字节的占位符文件，文件名为file_name"""
        placeholder = BytesIO(b'\x00')  # 1字节的空数据
        placeholder.name = file_name
        placeholder.seek(0)
        return placeholder
    
    async def send_with_placeholder(self, file_type: str, file_name: str, 
        chat_id: int, sender_name: str, 
                                  download_func, *download_args, **download_kwargs) -> dict:
        """
        先发送占位符，然后异步下载并更新真实文件
        """
        # 1. 先发送1B临时文件，文件名为file_name
        placeholder_caption = f"{sender_name}"
        placeholder_file = self._create_placeholder_file(file_name)
        
        # 统一发送为document，文件名为file_name
        response = await self.telegram_sender.send_document(
            chat_id, placeholder_file, placeholder_caption, filename=file_name
        )
        
        # 2. 异步下载并更新
        if response:
            message_id = response.message_id
            # 创建异步任务来处理文件下载和更新
            asyncio.create_task(
                self._download_and_update(
                    file_type,
                    chat_id, message_id,  sender_name,
                    download_func, download_args, download_kwargs
                )
            )
        
        return response
    
    async def _download_and_update(self, file_type: str, 
        chat_id: int, message_id: int, sender_name: str, 
        download_func, args, kwargs):
        """异步下载文件并更新消息"""
        try:
            # 执行下载
            file_data, filename = await download_func(*args, **kwargs)
            
            if file_data:
                # 使用edit_message_media方法，只替换媒体内容，不修改caption
                await self.telegram_sender.edit_message_media(
                    chat_id=chat_id,
                    message_id=message_id,
                    media=file_data,
                    media_type=file_type,
                    filename=filename,
                    caption=sender_name
                )
                
            else:
                if filename != "企微图片":
                    # 下载失败，更新为错误消息
                    logger.warning(f"⚠️ 文件下载失败")
                
        except Exception as e:
            logger.error(f"❌ 异步下载或更新过程中出错: {e}", exc_info=True)

# 全局实例
async_file_processor = AsyncFileProcessor(telegram_sender)
