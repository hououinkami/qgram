import logging

import config
from api.qq_api import qq_api
from api.telegram_sender import telegram_sender
from utils import tools

logger = logging.getLogger(__name__)

# 获取用户信息
class UserInfo:
    def __init__(self, name, avatar_url):
        self.name = name
        self.avatar_url = avatar_url

async def get_user_info(qqid, is_group, group_name = None):    
    try:
        if is_group:
            if group_name:
                name = group_name
            else:
                group_payload = {
                    "group_id": qqid
                }
                group_data = await qq_api("GET_GROUP_INFO", group_payload)
                name = group_data.get('data', {}).get('group_name', '未知群聊')
            avatar_url = f"https://p.qlogo.cn/gh/{qqid}/{qqid}/0"
        else:
            private_payload = {
                "user_id": qqid
            }
            private_data = await qq_api("GET_FRIEND_INFO", private_payload)
            name = private_data.get('data', {}).get('remark') or private_data.get('data', {}).get('nick') or ''
            avatar_url = f"https://q1.qlogo.cn/g?b=qq&nk={qqid}&s=0"
        return UserInfo(name, avatar_url)
    except (KeyError, IndexError) as e:
        logger.error(f"解析联系人信息时出错: {str(e)}")
    
    # 返回对应的None值
    return None

# 更新群组信息
async def update_info(chat_id, title=None, photo_url=None):
    """更新群组信息（标题和头像）"""
    results = {}
    
    # 更新群组名称
    if title:
        try:
            result = await telegram_sender.set_chat_title(chat_id, title)
            results['title_update'] = {
                'ok': True, 
                'description': 'Title updated successfully',
                'result': result
            }
        except Exception as e:
            results['title_update'] = {
                'ok': False, 
                'error': str(e)
            }
    
    # 更新群组头像
    if photo_url:
        try:            
            # 处理图片尺寸
            processed_photo_content = await tools.process_avatar_from_url(photo_url)
            
            result = await telegram_sender.set_chat_photo(chat_id, processed_photo_content)
            results['photo_update'] = {
                'ok': True, 
                'description': 'Photo updated successfully',
                'result': result
            }
            
        except Exception as e:
            results['photo_update'] = {
                'ok': False, 
                'error': str(e)
            }
    
    return results

# 修改后的函数
async def get_friends():
    """
    获取所有好友联系人并分类
    
    Returns:
        tuple: (friend_contacts, gh_contacts, chatroom_contacts)
            - friend_contacts: 私聊联系人ID列表（个人用户）
            - gh_contacts: 以gh_开头的联系人ID列表（公众号）
            - chatroom_contacts: 以@chatroom结尾的联系人ID列表（群聊）
    """
    
    # 初始化变量
    current_wx_seq = 0
    current_chatroom_seq = 0
    continue_flag = 1
    
    # 存储所有联系人
    all_contacts = []
    page_count = 0
    
    # 循环获取直到没有更多数据
    while continue_flag == 1:
        page_count += 1
        logger.debug(f"正在获取第 {page_count} 页数据...")
        
        # 构建请求体
        body = {
            "CurrentChatRoomContactSeq": current_chatroom_seq,
            "CurrentWxcontactSeq": current_wx_seq,
            "Wxid": config.MY_WXID
        }
        
        try:
            # 调用API
            response = await qq_api("USER_LIST", body)
            
            # 检查响应是否成功
            if not response.get('Success', False):
                error_msg = response.get('Message', '未知错误')
                logger.info(f"API调用失败: {error_msg}")
                break
            
            # 获取数据
            data = response.get('Data', {})
            
            # 更新分页参数
            continue_flag = data.get('CountinueFlag', 0)
            current_wx_seq = data.get('CurrentWxcontactSeq', 0)
            current_chatroom_seq = data.get('CurrentChatRoomContactSeq', 0)
            
            # 获取当前页的联系人列表
            contact_list = data.get('ContactUsernameList', [])
            contact_count = len(contact_list)
            
            logger.debug(f"第 {page_count} 页获取到 {contact_count} 个联系人")
            
            # 添加到总列表
            all_contacts.extend(contact_list)
            
            # 如果没有更多数据，退出循环
            if continue_flag == 0:
                logger.debug("已获取所有联系人数据")
                break
                
        except Exception as e:
            logger.error(f"请求第 {page_count} 页时发生错误: {str(e)}")
            break
    
    # 分类联系人
    gh_contacts = []         # 公众号（以gh_开头）
    chatroom_contacts = []   # 群聊（以@chatroom结尾）
    friend_contacts = []     # 私聊联系人（其余）
    
    for contact in all_contacts:
        if contact.startswith('gh_'):
            gh_contacts.append(contact)
        elif contact.endswith('@chatroom'):
            chatroom_contacts.append(contact)
        else:
            friend_contacts.append(contact)
    
    # 统计信息
    total_count = len(all_contacts)
    gh_count = len(gh_contacts)
    chatroom_count = len(chatroom_contacts)
    friend_count = len(friend_contacts)
    
    logger.debug("=" * 50)
    logger.debug("获取完成！")
    logger.debug(f"总页数: {page_count}")
    logger.debug(f"总联系人数: {total_count}")
    logger.debug(f"🏢 公众号数量 (gh_开头): {gh_count}")
    logger.debug(f"👥 群聊数量 (@chatroom结尾): {chatroom_count}")
    logger.debug(f"👤 私聊联系人数量: {friend_count}")
    logger.debug("=" * 50)
    
    return friend_contacts, chatroom_contacts, gh_contacts