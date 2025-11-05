#!/usr/bin/env python3
"""
Telegram 手动认证脚本
"""
import asyncio
from telethon import TelegramClient
import config
import os

async def main():
    session_dir = os.path.join(os.path.dirname(__file__), 'sessions')
    os.makedirs(session_dir, exist_ok=True)
    session_path = os.path.join(session_dir, 'tg_session')
    client = TelegramClient(
        session_path,
        config.API_ID,
        config.API_HASH,
        device_model="QGram",  # 设备型号
        system_version="Alpha",  # 系统版本
        # app_version="1.0.0",  # 应用版本
        # lang_code="zh",  # 语言代码
        # system_lang_code="zh"  # 系统语言
    )
    
    print("🔐 开始Telegram认证...")
    print(f"📱 手机号: {config.PHONE_NUMBER}")
    
    try:
        await client.start(phone=config.PHONE_NUMBER)
        me = await client.get_me()
        print(f"✅ 认证成功!")
        print(f"👤 用户: {me.first_name} {me.last_name or ''}")
        print(f"🆔 用户名: @{me.username}")
        print(f"🔢 用户ID: {me.id}")
        print("✅ 会话已保存，现在可以正常运行监控服务了")
    except Exception as e:
        print(f"❌ 认证失败: {e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())