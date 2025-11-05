import os

from utils.locales import Locale

# 语言
LANG = os.getenv("LANG", "zh")
LOCALE = Locale(LANG)

# Telegram Bot
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required")
API_ID = int(os.getenv("API_ID"))
if not API_ID:
    raise ValueError("API_ID environment variable is required")
API_HASH = os.getenv("API_HASH")
if not API_HASH:
    raise ValueError("API_HASH environment variable is required")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
if not PHONE_NUMBER:
    raise ValueError("PHONE_NUMBER environment variable is required")
DEVICE_MODEL = os.getenv("DEVICE_MODEL", "QGram")
QQ_CHAT_FOLDER = os.getenv("WECHAT_CHAT_FOLDER", "QQ")
POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "1"))
AUTO_CREATE_GROUPS = os.getenv("AUTO_CREATE_GROUPS", "True").lower() == "true"
WEBHOOK_DOMAIN = os.getenv("WEBHOOK_DOMAIN")
WEBHOOK_PORT = os.getenv("WEBHOOK_PORT", 8443)
SSL_CERT_NAME = os.getenv("SSL_CERT_NAME", "cert.pem")
SSL_KEY_NAME = os.getenv("SSL_KEY_NAME", "key.pem")

# NapCat
NAPCAT_CALLBACK_PATH = os.getenv("NAPCAT_CALLBACK_PORT", "/callback")
NAPCAT_CALLBACK_PORT = os.getenv("NAPCAT_CALLBACK_PORT", 3000)
NAPCAT_API_URL = os.getenv("NAPCAT_API_URL", "http://napcat:3001")
MY_QQ_ID = os.getenv("MY_QQ_ID")