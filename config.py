import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_TELEGRAM_IDS = [int(x) for x in os.getenv('CLIENT_TELEGRAM_IDS', '').split(',')]