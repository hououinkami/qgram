class LocaleConfig:
    LOCALES = {
        'ja': {
            'message_types': {
                'text': 'テキスト',
                'image': '写真',
                'voice': '音声',
                'request': '友人登録リクエスト',
                'video': '動画',
                'sticker': 'ステッカー',
                'location': '位置',
                'file': 'ファイル',
                'forward': 'チャット履歴',
                'reply': '引用',
                'revoke': '撤回',
                'unknown': '不明',
                'share': '共有',
                'music': '音楽',
                'emoji': '絵文字'
            },
            'common': {
                'all': 'すべて',
                'online': '🟢 WeChatがオンラインしました',
                'offline': '🔴 WeChatがオフラインしました',
                'auto_heartbeat_success': '✅ 自動心拍成功',
                'auto_heartbeat_fail': '❌ 自動心拍失敗',
                'twice_login_success': '✅ 二次ログイン成功',
                'twice_login_fail': '❌ 二次ログイン失敗',
                'successed': '✅ 成功',
                'failed': '❌ 失敗',
                'trans_to_text': '文字に転換',
                'add_contact': '連絡先に追加',
                'request_successed': '✅ 友人要請を送信しました',
                'agree_accept': '承認',
                'accept_successed': '✅ 承認しました',
                'transfer_out': 'を受領された',
                'transfer_in': 'を受信',
                'chat_account': 'チャット',
                'group_account': 'グループ',
                'offical_account': '公式アカ',
                'group_announcememt': 'グループ通知',
                'failed_to_create_group': '❌ グループ作成失敗！'
            },
            'command': {
                'update': '連絡先を更新',
                'receive': 'メッセージの受信',
                'receive_on': '✅ 転送オン',
                'receive_off': '❌ 転送オフ',
                'unbind': 'バインドを解除',
                'unbind_successed': '✅ バインド解除成功',
                'no_binding': '⚠️ まだ連絡先とバインドされません',
                'friend': '友人リストを取得',
                'no_contacts': '⚠️ 友人なし',
                'contact_list': '連絡先リスト',
                'page': 'ページ',
                'previous_page': '⬅️ 前へ',
                'next_page': '次へ ➡️',
                'total_contacts': 'すべて',
                'receive_yes': '受信',
                'receive_no': '非受信',
                'group_binding': '🔗 バインド',
                'group_unbind': '🔗 バインド解除',
                'update_contact': '🔄 更新',
                'delete_contact': '🗑️ 連絡先を削除',
                'back': '🔙 戻る',
                'ok': '✅ 確認',
                'cancel': '❌ キャンセル',
                'add': '連絡先を追加',
                'no_phone': '⚠️ 携帯番号、IDまたはQQが必要',
                'no_user': '⚠️ ユーザーは存在していません',
                'user_added': '✅ この友人はすでに登録しています',
                'remark': 'コメントを設定',
                'no_remark_name': '⚠️ コメントが必要',
                'quit': 'グループから退出',
                'revoke': 'メッセージの撤回',
                'revoke_failed': '❌ 撤回失敗',
                'no_reply': '⚠️ 撤回したいメッセージを引用',
                'no_message': '⚠️ 送信したいメッセージが必要',
                'timer_successed': '✅ 定時メッセージ送信成功',
                'timer_failed': '❌ 定時メッセージ送信失敗',
                'only_in_bot': '⚠️ このコマンドはボットのみで有効',
                'only_in_group': '⚠️ このコマンドはグループのみで有効',
                'only_in_chat': '⚠️ このコマンドはチャットのみで有効',
                'not_in_bot': '⚠️ このコマンドはボット以外で有効'
            }
        },
        'zh': {
            'message_types': {
                'text': '文本',
                'image': '图片',
                'voice': '语音',
                'request': '好友请求',
                'video': '视频',
                'sticker': '贴纸',
                'location': '定位',
                'file': '文件',
                'forward': '合并转发消息',
                'reply': '引用',
                'revoke': '撤回',
                'unknown': '未知',
                'share': '分享',
                'music': '音乐',
                'emoji': '表情'
            },
            'common': {
                'all': '全体成员',
                'moments': '朋友圈',
                'online': '🟢 WeChat已上线',
                'offline': '🔴 WeChat已离线',
                'twice_login_success': '✅ 二次登录成功',
                'twice_login_fail': '❌ 二次登录失敗',
                'successed': '✅ 成功',
                'failed': '❌ 失败',
                'trans_to_text': '转换成文字',
                'add_to_contact': '添加到联系人',
                'request_successed': '✅ 已发送好友申请',
                'agree_accept': '同意',
                'accept_successed': '✅ 已通过',
                'transfer_out': '已接受',
                'transfer_in': '',
                'chat_account': '私聊',
                'group_account': '群聊',
                'offical_account': '公众号',
                'group_announcememt': '群通知',
                'failed_to_create_group': '❌ 新建群组失败！'
            },
            'command': {
                'update': '更新联系人',
                'receive': '信息接收开关',
                'receive_on': '✅ 转发开启',
                'receive_off': '❌ 转发关闭',
                'unbind': '解除绑定',
                'unbind_successed': '⚠️ 从联系人文件中删除成功',
                'no_binding': '⚠️ 尚未绑定联系人',
                'friend': '获取好友列表',
                'no_contacts': '⚠️ 无好友',
                'contact_list': '好友列表',
                'page': '页',
                'previous_page': '⬅️ 上一页',
                'next_page': '下一页 ➡️',
                'total_contacts': '全部',
                'receive_yes': '接收',
                'receive_no': '不接收',
                'group_binding': '🔗 绑定',
                'group_unbind': '🔗 解绑',
                'update_contact': '🔄 更新',
                'delete_contact': '🗑️ 删除联系人',
                'back': '🔙 返回',
                'ok': '✅ 确认',
                'cancel': '❌ 取消',
                'add': '添加联系人',
                'no_phone': '⚠️ 请在命令后面输入搜索变量',
                'no_user': '⚠️ 用户不存在',
                'user_added': '✅ 已经添加为好友',
                'remark': '设置备注名',
                'no_remark_name': '⚠️ 请输入备注名',
                'quit': '退出群聊',
                'revoke': '撤回消息',
                'revoke_failed': '❌ 撤回失败',
                'no_reply': '⚠️ 请回复要撤回的信息',
                'no_message': '⚠️ 无发送信息',
                'timer_successed': '✅ 定时消息发送成功',
                'timer_failed': '❌ 定时消息发送失败',
                'only_in_bot': '⚠️ 此命令仅在Bot中有效',
                'only_in_group': '⚠️ 此命令仅在群聊中有效',
                'only_in_chat': '⚠️ 此命令仅在私聊中有效',
                'not_in_bot': '⚠️ 此命令仅在Bot中无效'
            }
        }
    }
    
    @classmethod
    def get_message_types(cls, locale='ja'):
        return cls.LOCALES.get(locale, {}).get('message_types', {})
    
    @classmethod
    def get_common(cls, locale='ja'):
        return cls.LOCALES.get(locale, {}).get('common', {})
    
    @classmethod
    def get_command(cls, locale='ja'):
        return cls.LOCALES.get(locale, {}).get('command', {})

class Locale:
    def __init__(self, locale='ja'):
        self.locale = locale
        self.type_map = LocaleConfig.get_message_types(locale)
        self.common_map = LocaleConfig.get_common(locale)
        self.command_map = LocaleConfig.get_command(locale)
    
    def type(self, value):
        """获取消息类型"""
        return self.type_map.get(value)
    
    def common(self, key):
        """获取通用文本"""
        return self.common_map.get(key)
    
    def command(self, key):
        """获取命令文本"""
        return self.command_map.get(key)