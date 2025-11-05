import base64
import datetime
import logging
import math
import os
from io import BytesIO
from typing import Tuple

import aiohttp
import aiofiles
import aiofiles.os

import config
from config import LOCALE as locale
from api.qq_api import qq_api

# 获取模块专用的日志记录器
logger = logging.getLogger(__name__)

# 常量定义
WXID = config.MY_WXID
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "download")
IMAGE_DIR = os.path.join(DOWNLOAD_DIR, "image")
VIDEO_DIR = os.path.join(DOWNLOAD_DIR, "video")
EMOJI_DIR = os.path.join(DOWNLOAD_DIR, "sticker")
FILE_DIR = os.path.join(DOWNLOAD_DIR, "file")
VOICE_DIR = os.path.join(DOWNLOAD_DIR, "voice")

async def get_image(msg_id: str, from_wxid: str, data_json) -> Tuple[bool, str, str]:
    return await chunked_download(
        api_path="GET_IMAGE",
        msg_id=msg_id,
        from_wxid=from_wxid,
        data_json=data_json,
        file_key="img",
        file_extension="png"
    )

async def get_video(msg_id: str, from_wxid: str, data_json) -> Tuple[bool, str, str]:
    return await chunked_download(
        api_path="GET_VIDEO",
        msg_id=msg_id,
        from_wxid=from_wxid,
        data_json=data_json,
        file_key="videomsg",
        file_extension="mp4"
    )

async def get_file(msg_id: str, from_wxid: str, data_json) -> Tuple[bool, str, str]:
    return await chunked_download(
        api_path="GET_FILE",
        msg_id=msg_id,
        from_wxid=from_wxid,
        data_json=data_json,
        file_key="appmsg",
        file_extension=""
    )
    
async def get_emoji(data_json) -> Tuple[bool, str, str]:
    try:
        md5 = data_json["msg"]["emoji"]["md5"]
        data_length = int(data_json["msg"]["emoji"]["len"])
        url = data_json["msg"]["emoji"]["cdnurl"]

        # 文件名和路径
        filename = f"{md5}.gif"
        filepath = os.path.join(EMOJI_DIR, filename)

        # ✅ 异步文件检查
        try:
            await aiofiles.os.stat(filepath)
            # 文件存在，读取到BytesIO
            async with aiofiles.open(filepath, 'rb') as f:
                data = await f.read()
                file_buffer = BytesIO(data)
                file_buffer.seek(0)
                return True, file_buffer, filename  # 统一返回BytesIO
        except FileNotFoundError:
            pass
        
        # 利用API请求下载
        async def get_url_by_api():
            # 构建请求参数
            payload = {
                "Md5": md5,
                "Wxid": WXID
            }

            # 发送请求
            response_data = await qq_api("GET_EMOJI", payload)
            
            # 检查响应数据结构
            if (response_data and "Data" in response_data):
                # 检查是否有直接的url
                if "url" in response_data["Data"]:
                    url = response_data["Data"]["url"]
                    return url
                # 检查是否有emojiList结构
                elif "emojiList" in response_data["Data"] and response_data["Data"]["emojiList"] and len(response_data["Data"]["emojiList"]) > 0:
                    if "url" in response_data["Data"]["emojiList"][0]:
                        url = response_data["Data"]["emojiList"][0]["url"]
                        return url
                    else:
                        logger.error("emojiList中找不到url字段")
                        return None
                else:
                    logger.error("响应数据中找不到url")
                    return None
            else:
                logger.error("响应数据格式不正确")
                return None
        
        # 如果原始URL为空，尝试通过API获取
        if not url or url == "":
            api_url = await get_url_by_api()
            if api_url:
                url = api_url
            else:
                logger.error("无法获取有效的URL")
                return False, "无法获取有效的URL", ""
        
        url = str(url) if url else ""
        if not url or not url.startswith('http'):
            raise ValueError("Empty URL provided")

        # 下载文件到内存并备份到磁盘
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as file_response:
                if file_response.status == 200:
                    # 读取所有数据到内存
                    data = await file_response.read()
                    
                    # 创建BytesIO对象
                    file_buffer = BytesIO(data)
                    file_buffer.seek(0)  # 重置指针到开头
                    
                    # ✅ 异步备份到磁盘
                    try:
                        await aiofiles.os.makedirs(os.path.dirname(filepath), exist_ok=True)
                        
                        async with aiofiles.open(filepath, 'wb') as f:
                            await f.write(data)
                        
                        logger.debug(f"文件已备份到: {filepath}")
                    except Exception as e:
                        logger.warning(f"备份文件失败: {e}")
                    
                    return True, file_buffer, f"{locale.type(47)}.gif"
                    
    except Exception as e:
        logger.exception(f"下载失败: {str(e)}")
        return False, f"下载失败: {str(e)}", ""

async def get_voice(msg_id, from_user_name, data_json) -> Tuple[bool, str]:
    try:
        md5 = data_json["msg"]["voicemsg"]["aeskey"]
        data_length = int(data_json["msg"]["voicemsg"]["length"])
        bufid = data_json["msg"]["voicemsg"]["bufid"]

        # 文件名和路径
        filename = f"{md5}.silk"
        filepath = os.path.join(VOICE_DIR, filename)

        # ✅ 异步文件检查
        try:
            await aiofiles.os.stat(filepath)
            return True, filepath
        except FileNotFoundError:
            pass
        
        # 构建请求参数
        payload = {
            "Bufid": str(bufid),
            "FromUserName": str(from_user_name),
            "Length": int(data_length),
            "MsgId": int(msg_id),
            "Wxid": str(WXID)
        }

        # 发送请求
        response_data = await qq_api("GET_VOICE", payload)
        
        # 检查响应数据结构
        if (response_data and "Data" in response_data):
            # 检查是否有直接的url
            if "data" in response_data["Data"] and "buffer" in response_data["Data"]["data"]:
                voice_base64 = response_data["Data"]["data"]["buffer"]
            else:
                logger.error("响应数据中找不到buffer")
                return False, "响应数据中找不到buffer"
        else:
            logger.error("响应数据格式不正确")
            return False, "响应数据格式不正确"
        
        # ✅ 异步目录创建和文件写入
        await aiofiles.os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # 下载文件
        all_binary_data = bytearray()
        if voice_base64:
            # 移除可能的base64头部
            if ',' in voice_base64:
                voice_base64 = voice_base64.split(',', 1)[1]
            # 解码base64为二进制数据
            voice_binary_data = base64.b64decode(voice_base64)
            all_binary_data.extend(voice_binary_data)

            # ✅ 异步文件写入
            async with aiofiles.open(filepath, 'wb') as f:
                await f.write(all_binary_data)
            return True, filepath
                
    except Exception as e:
        logger.exception(f"下载失败: {str(e)}")
        return False, f"下载失败: {str(e)}"

# 分段下载函数
async def chunked_download(api_path: str, msg_id: str, from_wxid: str, data_json: dict, file_key: str, file_extension: str, save_dir: str = None) -> Tuple[bool, str, str]:
    try:
        # 提取文件信息
        file_info = data_json["msg"][file_key]
        md5 = file_info.get("md5")
        if not md5:
            # 生成 8位日期 + 6位时间 的替代值
            now = datetime.datetime.now()
            md5 = now.strftime("%Y%m%d%H%M%S")
        data_length = int(file_info.get("length") or file_info.get("appattach").get("totallen"))
        file_title = (file_info.get("title") or "")

        # 发送者为企业微信时不尝试下载图片和视频
        if file_key in ['img', 'videomsg'] and data_length == 0:
            return False, "发送者为企业微信，终止下载", "企微图片"
        
        # 文件名和路径
        if not file_title:
            filename = f"{md5}.{file_extension}"
        else:
            filename = f"{file_title}"
            
        if save_dir:
            filepath = os.path.join(save_dir, filename)
        
            # ✅ 异步文件检查
            try:
                await aiofiles.os.stat(filepath)
                return True, filepath, filename  # 文件已存在
            except FileNotFoundError:
                pass  # 文件不存在，继续下载
            
            # ✅ 异步目录创建
            await aiofiles.os.makedirs(save_dir, exist_ok=True)
            
        # 用于存储所有分段的二进制数据
        all_binary_data = bytearray()
        cdn_success = False

        # 优先使用cdn下载（仅对图片）
        if file_key == "img":
            try:
                aeskey = data_json["msg"]["img"]["aeskey"]
                # 按优先级顺序获取第一个非空的CDN URL
                cdnurl = (data_json["msg"]["img"].get("cdnbigimgurl") or 
                        data_json["msg"]["img"].get("cdnmidimgurl") or 
                        data_json["msg"]["img"].get("cdnthumburl") or 
                        "")
                                
                if cdnurl:
                    cdn_body = {
                        "FileAesKey": aeskey,
                        "FileNo": cdnurl,
                        "Wxid": WXID
                    }
                    response_data = await qq_api("GET_IMAGE_CDN", cdn_body)
                
                    # 检查响应数据结构
                    if (response_data and "Data" in response_data and "Image" in response_data["Data"]):
                        cdn_base64 = response_data["Data"]["Image"]

                    if cdn_base64:
                        # 移除可能的base64头部
                        if ',' in cdn_base64:
                            cdn_base64 = cdn_base64.split(',', 1)[1]
                        # 解码base64为二进制数据
                        cdn_binary_data = base64.b64decode(cdn_base64)
                        all_binary_data.extend(cdn_binary_data)
                        cdn_success = True
            except Exception as e:
                cdn_success = False

        # 如果CDN下载失败或不是图片，使用分段下载
        if not cdn_success:
            # 配置分段大小
            chunk_size = 256 * 256
            total_chunks = math.ceil(data_length / chunk_size)
            
            # 初始化分段参数
            chunk_index = 1
            next_start_pos = 0
            next_chunk_size = min(chunk_size, data_length)
            
            # 循环下载所有分段
            retry_attempted = False  # 添加重试标志
            while True:
                logger.debug(f"下载分段 {chunk_index}/{total_chunks}, 起始位置: {next_start_pos}, 大小: {next_chunk_size}")
                
                # 构建请求参数
                if file_key == "appmsg":
                    payload = {
                        "AppID": file_info["appid"],
                        "AttachId": file_info["appattach"]["attachid"],
                        "DataLen": data_length,
                        "Section": {
                            "DataLen": next_chunk_size,
                            "StartPos": next_start_pos
                        },
                        "UserName": "",
                        "Wxid": WXID
                    }
                else:
                    payload = {
                        "CompressType": 0,
                        "DataLen": data_length,
                        "MsgId": msg_id,
                        "Section": {
                            "DataLen": next_chunk_size,
                            "StartPos": next_start_pos
                        },
                        "Wxid": WXID,
                        "ToWxid": from_wxid
                    }
                
                # 发送请求
                response_data = await qq_api(api_path, payload)
                
                # 解析响应JSON
                try:
                    if 'Data' in response_data and 'data' in response_data['Data'] and 'buffer' in response_data['Data']['data']:
                        # 获取base64字符串
                        base64_data = response_data['Data']['data']['buffer']
                        
                        # 移除可能存在的base64头部
                        if ',' in base64_data:
                            base64_data = base64_data.split(',', 1)[1]
                        
                        # 解码base64为二进制
                        binary_chunk = base64.b64decode(base64_data)
                        
                        # 添加到总数据中
                        all_binary_data.extend(binary_chunk)
                        logger.debug(f"成功接收分段 {chunk_index}, 大小: {len(binary_chunk)} 字节")
                    else:
                        # 当第一次请求获取不到buffer时，尝试更改payload重新请求
                        if chunk_index == 1 and not retry_attempted:
                            retry_attempted = True  # 标记已尝试重试
                            logger.warning("第一次请求未获取到buffer，尝试更改请求参数...")
                            
                            # 临时更改payload
                            if file_key == "appmsg":
                                temp_payload = {
                                    "AppID": file_info["appid"],
                                    "AttachId": file_info["appattach"]["attachid"],
                                    "Section": {
                                        "DataLen": chunk_size,
                                        "StartPos": 0
                                    },
                                    "UserName": "",
                                    "Wxid": WXID
                                }
                            else:
                                temp_payload = {
                                    "CompressType": 0,
                                    "MsgId": msg_id,
                                    "Section": {
                                        "DataLen": chunk_size,
                                        "StartPos": 0
                                    },
                                    "Wxid": WXID,
                                    "ToWxid": from_wxid
                                }
                            
                            # 发送临时请求
                            temp_data = await qq_api(api_path, temp_payload)
                            
                            # 尝试获取totalLen
                            if 'Data' in temp_data and 'totalLen' in temp_data['Data']:
                                # 更新data_length
                                new_data_length = temp_data['Data']['totalLen']
                                
                                # 使用新的data_length重新计算参数
                                data_length = new_data_length
                                total_chunks = math.ceil(data_length / chunk_size)
                                next_chunk_size = min(chunk_size, data_length)
                                
                                # 重新开始下载
                                continue
                            else:
                                logger.error("临时请求未能获取到totalLen，终止下载")
                                return False, "临时请求未能获取到totalLen", ""
                        else:
                            if chunk_index == 1:
                                logger.error("重试后仍无法获取buffer，终止下载")
                                return False, "重试后仍无法获取buffer", ""
                            else:
                                logger.error(f"响应格式错误: 找不到Data.data.buffer字段")
                                return False, f"响应格式错误: 找不到Data.data.buffer字段", ""
                except Exception as e:
                    logger.error(f"处理响应数据时出错: {str(e)}")
                    return False, f"处理响应数据时出错: {str(e)}", ""
                
                # 检查是否已下载完所有分段
                if chunk_index == total_chunks:
                    break
                    
                # 准备下一个请求
                chunk_index += 1
                next_start_pos = chunk_size * (chunk_index - 1)
                remaining_data = data_length - next_start_pos
                next_chunk_size = min(chunk_size, remaining_data)
        
        if save_dir:
            # ✅ 异步文件写入
            async with aiofiles.open(filepath, 'wb') as f:
                await f.write(all_binary_data)
            return True, filepath, filename
        else:
            # 转换为 BytesIO（推荐）
            file_buffer = BytesIO(all_binary_data)
            return True, file_buffer, filename
        
    except Exception as e:
        logger.exception(f"下载失败: {str(e)}")
        return False, f"下载失败: {str(e)}", ""
