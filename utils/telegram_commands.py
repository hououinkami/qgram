import base64
import logging
import os
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from io import BytesIO

from api import qq_contacts
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram import Update
from telegram.ext import ContextTypes

import config
from config import LOCALE as locale
from api import wechat_login
from api.telegram_sender import telegram_sender
from api.qq_api import qq_api
from service.telethon_client import get_user_id
from utils import tools
from utils.contact_manager import contact_manager, Contact
from utils.telegram_callbacks import create_callback_data
from utils.telegram_to_qq import revoke_by_telegram_bot_command

logger = logging.getLogger(__name__)

class CommandScope(Enum):
    BOT_ONLY = "bot_only"
    GROUP_ONLY = "group_only"
    CHAT_ONLY = "chat_only"
    NOT_BOT = "not_bot"
    ALL = "all"

def command_scope(scope: CommandScope):
    """装饰器：限制命令使用范围"""
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
            chat_id = update.effective_chat.id
            chat_type = update.effective_chat.type
            
            # 如果是 BOT_ONLY 或 NOT_BOT，不需要查询 qqid
            if scope == CommandScope.BOT_ONLY:
                if chat_id != get_user_id():
                    await telegram_sender.send_text(chat_id, locale.command("only_in_bot"))
                    return
            elif scope == CommandScope.NOT_BOT:
                if chat_id == get_user_id():
                    await telegram_sender.send_text(chat_id, locale.command("not_in_bot"))
                    return
            elif scope in [CommandScope.GROUP_ONLY, CommandScope.CHAT_ONLY]:
                # 只有需要区分微信群聊/私聊时才查询 qqid
                try:
                    qqid = await contact_manager.get_qqid_by_chatid(chat_id)
                    if not qqid:
                        await telegram_sender.send_text(chat_id, locale.command("no_binding"))
                        return
                    
                    if scope == CommandScope.GROUP_ONLY and not qqid.endswith('@chatroom'):
                        await telegram_sender.send_text(chat_id, locale.command("only_in_group"))
                        return
                    elif scope == CommandScope.CHAT_ONLY and qqid.endswith('@chatroom'):
                        await telegram_sender.send_text(chat_id, locale.command("only_in_chat"))
                        return
                        
                except Exception as e:
                    await telegram_sender.send_text(chat_id, locale.common("failed") + f": {str(e)}")
                    return
            
            # CommandScope.ALL 不需要检查
            return await func(update, context)
        return wrapper
    return decorator

def delete_command_message(func):
    """装饰器：自动删除命令消息"""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        message = update.message
        
        try:
            # 执行原始命令
            result = await func(update, context)
            return result
        finally:
            # 无论成功还是失败都删除命令消息
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message.message_id)
            except Exception:
                pass  # 忽略删除失败的错误
    
    return wrapper

class BotCommands:
    """机器人命令处理类"""

    @staticmethod
    @delete_command_message
    @command_scope(CommandScope.NOT_BOT)
    async def update_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """更新联系人信息"""
        chat_id = update.effective_chat.id
        
        try:
            to_qqid = await contact_manager.get_qqid_by_chatid(chat_id)
            if not to_qqid:
                await telegram_sender.send_text(chat_id, locale.command("no_binding"))
                return
            contact_saved = await contact_manager.get_contact(to_qqid)

            user_info = await qq_contacts.get_user_info(to_qqid, contact_saved.is_group)
            
            # 更新映射文件
            await contact_manager.update_contact_by_chatid(chat_id, {
                "name": user_info.name,
                "avatar_url": user_info.avatar_url
            })

            chat_info = await telegram_sender.get_chat(chat_id)
            name_to_use = user_info.name if contact_saved.name != user_info.name else None
            avatar_to_use = user_info.avatar_url if contact_saved.avatar_url != user_info.avatar_url else None
            # 若无头像则设置新头像
            if not hasattr(chat_info, 'photo') or chat_info.photo is None:
                avatar_to_use = user_info.avatar_url
                
            # 更新TG群组
            await qq_contacts.update_info(chat_id, name_to_use, avatar_to_use)
            
        except Exception as e:
            await telegram_sender.send_text(chat_id, f"{locale.common('failed')}: {str(e)}")

    @staticmethod
    @delete_command_message
    @command_scope(CommandScope.NOT_BOT)
    async def receive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """切换接收消息状态"""
        chat_id = update.effective_chat.id
        
        try:
            contact = await contact_manager.get_contact_by_chatid(chat_id)
            if not contact.chat_id:
                await telegram_sender.send_text(chat_id, locale.command("no_binding"))
                return
            
            await contact_manager.update_contact_by_chatid(chat_id, {"is_receive": "toggle"})
            contact_now = await contact_manager.get_contact_by_chatid(chat_id)
            
            if contact_now and contact_now.is_receive:
                await telegram_sender.send_text(chat_id, locale.command("receive_on"))
            else:
                await telegram_sender.send_text(chat_id, locale.command("receive_off"))
                
        except Exception as e:
            await telegram_sender.send_text(chat_id, f"{locale.common('failed')}: {str(e)}")

    @staticmethod
    @delete_command_message
    @command_scope(CommandScope.NOT_BOT)
    async def unbind_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """删除联系人数据"""
        chat_id = update.effective_chat.id
        
        try:
            to_qqid = await contact_manager.get_qqid_by_chatid(chat_id)
            if not to_qqid:
                await telegram_sender.send_text(chat_id, locale.command("no_binding"))
                return
            
            # 获取命令参数
            args = context.args if context.args else []

            if args and args[0].lower() == "del":   # 直接删除
                unbind_result = await contact_manager.delete_contact(to_qqid)
            else:   # 解绑但不删除
                unbind_result = await contact_manager.update_contact_by_chatid(chat_id, {"chat_id": -9999999999})

            if unbind_result:
                await telegram_sender.send_text(chat_id, locale.command("unbind_successed"))
            else:
                await telegram_sender.send_text(chat_id, locale.common('failed'))
                
        except Exception as e:
            await telegram_sender.send_text(chat_id, f"{locale.common('failed')}: {str(e)}")

    @staticmethod
    @command_scope(CommandScope.BOT_ONLY)
    async def friend_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """处理friend命令，支持update参数"""
        chat_id = update.effective_chat.id
        
        try:
            # 获取命令参数
            args = context.args if context.args else []
            
            if args and args[0].lower() == 'import':
                json_name = args[1] if len(args) > 1 else "contact"
                json_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                    "database", 
                    f"{json_name}.json"
                )
                # 导入json
                imported_count = await contact_manager.import_from_json(json_path)
                if imported_count > 0:
                    await telegram_sender.send_text(get_user_id(), f"{imported_count}の連絡先をインポートしました")
            elif args and args[0].lower() == 'export':
                json_name = args[1] if len(args) > 1 else "contact"
                json_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                    "database", 
                    f"{json_name}.json"
                )
                # 导出json
                exported_count = await contact_manager.export_to_json(json_path)
                if exported_count > 0:
                    await telegram_sender.send_text(get_user_id(), f"{exported_count}の連絡先をエクスポートしました")
            elif args and args[0].lower() == 'update':
                # 执行更新功能
                await contact_manager.update_contacts_and_sync_to_db(chat_id, update = False)
            elif args and args[0].lower() != 'update':
                # 执行搜索
                await BotCommands.list_contacts(chat_id, args[0])
            else:
                # 执行联系人列表显示功能
                await BotCommands.list_contacts(chat_id)
                
        except Exception as e:
            error_msg = f"{locale.common('failed')}: {str(e)}"
            await telegram_sender.send_text(chat_id, error_msg)
            logger.error(f"friend_command执行失败: {str(e)}")
    
    @staticmethod
    @delete_command_message
    @command_scope(CommandScope.BOT_ONLY)
    async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """添加联系人"""
        chat_id = update.effective_chat.id        
        scene_list = {"id": 3, "qq": 4, "group": 8, "phone": 15, "card": 17, "qr": 30}

        # 获取命令后的参数
        args = context.args  # 这是一个列表，包含命令后的所有参数
        if len(args) > 0:
            user_id = args[0]
            add_message = args[1] if len(args) > 1 else ""
            add_scene = scene_list.get(args[2], 0) if len(args) > 2 else 15
        else:
            await telegram_sender.send_text(chat_id, locale.command("no_phone"))
            return

        try:           
            search_payload = {
                "FromScene": add_scene,
                "SearchScene": 1,
                "ToUserName": str(user_id),
                "qqid": config.MY_qqid
            }
            search_result = await qq_api("USER_SEARCH", search_payload)

            search_data = search_result.get("Data", {})

            # 用户不存在
            if search_data.get('BaseResponse', {}).get('ret') == -4:
                await telegram_sender.send_text(chat_id, locale.command("no_user"))
                return
            
            # 用户存在
            nickname = search_data.get('NickName', {}).get('string', '')
            username = search_data.get('UserName', {}).get('string', '')
            ticket = search_data.get('AntispamTicket')
            avatar_url = search_data.get('BigHeadImgUrl') or search_data.get('SmallHeadImgUrl')

            # 已经是好友
            if not ticket:
                await telegram_sender.send_text(chat_id, locale.command("user_added"))
                return
            
            # 发送搜索结果
            if avatar_url:
                processed_photo_content = await tools.get_image_from_url(avatar_url)

            callback_data = {
                "Opcode": 2,
                "Scene": add_scene,
                "V1": username,
                "V2": ticket,
                "VerifyContent": add_message,
                "qqid": config.MY_qqid
            }

            keyboard = [
                [InlineKeyboardButton(
                    f"{locale.common('add_contact')}", 
                    callback_data=create_callback_data("add_contact", callback_data)
                )]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            send_text = f"<blockquote>{nickname}</blockquote>"
            await telegram_sender.send_photo(chat_id, processed_photo_content, send_text, reply_markup=reply_markup)
                
        except Exception as e:
            await telegram_sender.send_text(chat_id, f"{locale.common('failed')}: {str(e)}")

    @staticmethod
    @delete_command_message
    @command_scope(CommandScope.NOT_BOT)
    async def remark_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """设置好友备注"""
        chat_id = update.effective_chat.id
        
        # 添加参数检查
        if not context.args or len(context.args) == 0:
            await telegram_sender.send_text(chat_id, locale.command("no_remark_name"))
            return
        
        to_qqid = await contact_manager.get_qqid_by_chatid(chat_id)
        if not to_qqid:
            await telegram_sender.send_text(chat_id, locale.command("no_binding"))
            return
    
        # 将所有参数用空格连接，支持带空格的备注名
        remark_name = " ".join(context.args)

        try:
            if not to_qqid.endswith("@openim"):
                payload = {
                    "Remarks": remark_name,
                    "Toqqid": to_qqid,
                    "qqid": config.MY_qqid
                }
                
                await qq_api("USER_REMARK", payload)
            else:
                # 更新企业微信联系人文件
                await contact_manager.update_contact_by_chatid(chat_id, {
                    "name": remark_name
                })

            # 设置完成后更新群组信息
            await BotCommands.update_command(update, context)
            
        except Exception as e:
            await telegram_sender.send_text(chat_id, f"{locale.common('failed')}: {str(e)}")
    
    @staticmethod
    @delete_command_message
    @command_scope(CommandScope.GROUP_ONLY)
    async def quit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """退出群聊"""
        chat_id = update.effective_chat.id
        
        to_qqid = await contact_manager.get_qqid_by_chatid(chat_id)
        if not to_qqid:
            await telegram_sender.send_text(chat_id, locale.command("no_binding"))
            return

        try:
            payload = {
                "QID": to_qqid,
                "qqid": config.MY_qqid
            }
            
            await qq_api("GROUP_QUIT", payload)

            # 更新联系人文件
            await contact_manager.delete_contact(to_qqid)
            
        except Exception as e:
            await telegram_sender.send_text(chat_id, f"{locale.common('failed')}: {str(e)}")
    
    @staticmethod
    @delete_command_message
    @command_scope(CommandScope.NOT_BOT)
    async def revoke_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """撤回消息"""
        chat_id = update.effective_chat.id
        message = update.message
        
        try:
            if not message.reply_to_message:
                await telegram_sender.send_text(chat_id, locale.command("no_reply"))
                return
            
            await revoke_by_telegram_bot_command(chat_id, message)
            
        except Exception as e:
            await telegram_sender.send_text(chat_id, f"{locale.common('failed')}: {str(e)}")
    
    @staticmethod
    async def list_contacts(chat_id: int, search_word: str = ""):
        """显示联系人列表 - 简化版本，直接跳转到分页处理器"""
        try:

            # 搜索联系人
            contacts = await contact_manager.search_contacts_by_name(search_word)
            
            if not contacts:
                await telegram_sender.send_text(chat_id, locale.command('no_contacts'))
                return
            
            # 转换 Contact 对象为字典格式（为了兼容现有的显示逻辑）
            contacts_dict = [contact.to_dict() for contact in contacts]
            
            # 如果有联系人，直接显示第一页
            await BotCommands._show_contacts_page(chat_id, contacts_dict, 0, search_word)
            
        except Exception as e:
            logger.error(f"显示联系人列表失败: {e}")
            await telegram_sender.send_text(
                chat_id=chat_id,
                text=f"❌ 获取联系人列表失败: {str(e)}"
            )
    
    @staticmethod
    async def _show_contacts_page(chat_id: int, contacts: list, page: int = 0, search_word: str = ""):
        """显示联系人分页 - 发送新消息版本"""
        try:
            # 使用共享的构建方法
            message_text, reply_markup = await BotCommands.build_contacts_page_data(contacts, page, search_word)
            
            if reply_markup is None:
                await telegram_sender.send_text(chat_id, message_text)
            else:
                await telegram_sender.send_text(chat_id, message_text, reply_markup=reply_markup)
                
        except Exception as e:
            logger.error(f"显示联系人分页失败: {e}")
            await telegram_sender.send_text(
                chat_id=chat_id,
                text=f"❌ 显示联系人列表失败: {str(e)}"
            )
            
    @staticmethod
    async def build_contacts_page_data(contacts: list, page: int = 0, search_word: str = ""):
        """构建联系人页面数据 - 供回调处理器使用"""
        try:            
            if not contacts:
                return None, None
            
            # 分页设置
            items_per_page = 10
            total_contacts = len(contacts)
            total_pages = (total_contacts + items_per_page - 1) // items_per_page
            
            # 确保页码有效
            if page < 0:
                page = 0
            elif page >= total_pages:
                page = total_pages - 1
            
            # 获取当前页的联系人
            start_index = page * items_per_page
            end_index = min(start_index + items_per_page, total_contacts)
            current_page_contacts = contacts[start_index:end_index]
            
            # 构建键盘布局
            keyboard = []
            
            # 每行2个按钮，显示联系人
            for i in range(0, len(current_page_contacts), 2):
                row = []
                
                # 第一个联系人
                contact1 = current_page_contacts[i]
                contact1_name = contact1.get('name', '未知')
                if len(contact1_name) > 8:
                    contact1_name = contact1_name[:8] + "..."
                
                contact1_obj = Contact.from_dict(contact1)
                contact1_type = contact_manager.get_contact_type_icon(contact1_obj)
                contact1_receive = contact_manager.get_contact_receive_icon(contact1_obj)
                
                contact1_data = {
                    "qqid": contact1.get('qqid', ''),
                    "name": contact1.get('name', ''),
                    "chat_id": contact1.get('chat_id', ''),
                    "is_group": contact1.get('is_group', False),
                    "is_receive": contact1.get('is_receive', True),
                    "wx_name": contact1.get('wx_name', ''),
                    "avatar_url": contact1.get('avatar_url', ''),
                    'source_page': page,
                    'search_word': search_word
                }
                
                row.append(InlineKeyboardButton(
                    f"{contact1_type}{contact1_receive} {contact1_name}",
                    callback_data=create_callback_data("contact_info", contact1_data)
                ))
                
                # 第二个联系人（如果存在）
                if i + 1 < len(current_page_contacts):
                    contact2 = current_page_contacts[i + 1]
                    contact2_name = contact2.get('name', '未知')
                    if len(contact2_name) > 8:
                        contact2_name = contact2_name[:8] + "..."
                    
                    contact2_obj = Contact.from_dict(contact2)
                    contact2_type = contact_manager.get_contact_type_icon(contact2_obj)
                    contact2_receive = contact_manager.get_contact_receive_icon(contact2_obj)
                    
                    contact2_data = {
                        "qqid": contact2.get('qqid', ''),
                        "name": contact2.get('name', ''),
                        "chat_id": contact2.get('chat_id', ''),
                        "is_group": contact2.get('is_group', False),
                        "is_receive": contact2.get('is_receive', True),
                        "wx_name": contact2.get('wx_name', ''),
                        "avatar_url": contact2.get('avatar_url', ''),
                        'source_page': page,
                        'search_word': search_word
                    }
                    
                    row.append(InlineKeyboardButton(
                        f"{contact2_type}{contact2_receive} {contact2_name}",
                        callback_data=create_callback_data("contact_info", contact2_data)
                    ))
                
                keyboard.append(row)
            
            # 添加分页按钮
            if total_pages > 1:
                pagination_row = []
                
                if page > 0:
                    pagination_row.append(InlineKeyboardButton(
                        f"{locale.command('previous_page')}",
                        callback_data=create_callback_data("contact_page", {"contacts": contacts, "source_page": page - 1, "search_word": search_word})
                    ))
                
                pagination_row.append(InlineKeyboardButton(
                    f"📄 {page + 1}/{total_pages}",
                    callback_data="page_info"
                ))
                
                if page < total_pages - 1:
                    pagination_row.append(InlineKeyboardButton(
                        f"{locale.command('next_page')}",
                        callback_data=create_callback_data("contact_page", {"contacts": contacts, "source_page": page + 1, "search_word": search_word})
                    ))
                
                keyboard.append(pagination_row)
            
            # 构建消息文本
            offical_count = len([c for c in contacts if c.get('qqid', '').startswith('gh_')])
            friends_count = len([c for c in contacts if not c.get('is_group', False)])
            groups_count = len([c for c in contacts if c.get('is_group', False)])
            active_count = len([c for c in contacts if c.get('is_receive', True)])
            
            message_text = f"""📋 **{locale.command('contact_list')}** (第 {page + 1}/{total_pages} {locale.command('page')})

  • {locale.command('total_contacts')}: {total_contacts}
  • {locale.common('chat_account')}: {friends_count - offical_count} | {locale.common('group_account')}: {groups_count} | {locale.common('offical_account')}: {offical_count}
  • {locale.command('receive_yes')}: {active_count} | {locale.command('receive_no')}: {total_contacts - active_count}
"""
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            return message_text, reply_markup
            
        except Exception as e:
            logger.error(f"构建联系人页面数据失败: {e}")
            return f"❌ 构建联系人页面失败: {str(e)}", None

    # 命令配置
    @classmethod
    def get_command_config(cls):
        """获取命令配置"""
        return [
            ["update", locale.command("update")],
            ["receive", locale.command("receive")], 
            ["unbind", locale.command("unbind")],
            ["friend", locale.command("friend")],
            ["add", locale.command("add")],
            ["remark", locale.command("remark")],
            ["quit", locale.command("quit")],
            ["rm", locale.command("revoke")]
        ]
    
    # 命令处理器映射
    @classmethod
    def get_command_handlers(cls):
        """获取命令处理器映射"""
        return {
            "update": cls.update_command,
            "receive": cls.receive_command,
            "unbind": cls.unbind_command,
            "friend": cls.friend_command,
            "add": cls.add_command,
            "remark": cls.remark_command,
            "quit": cls.quit_command,
            "rm": cls.revoke_command
        }
