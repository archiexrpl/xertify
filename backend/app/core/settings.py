import os
from dotenv import load_dotenv, find_dotenv

# Force-find the nearest .env and override any shell env with it
load_dotenv(find_dotenv(usecwd=True), override=True)

XUMM_API_KEY = os.getenv("XUMM_API_KEY") or os.getenv("XUMM_APIKEY", "")
XUMM_API_SECRET = os.getenv("XUMM_API_SECRET") or os.getenv("XUMM_APISECRET", "")
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:8000")
SESSION_SECRET = os.getenv("SESSION_SECRET", "change-me-please")
SESSION_COOKIE = os.getenv("SESSION_COOKIE", "prjxhub_session")
XRPL_RPC_URL = os.getenv("XRPL_RPC_URL", "https://s.altnet.rippletest.net:51234")
