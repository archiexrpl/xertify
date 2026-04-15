import os
from dotenv import load_dotenv
from xumm import XummSdk

load_dotenv()

key = os.getenv("XUMM_API_KEY") or os.getenv("XUMM_APIKEY")
sec = os.getenv("XUMM_API_SECRET") or os.getenv("XUMM_APISECRET")

print("KEY:", key[:4], "...", key[-4:], "len", len(key))
print("SEC:", sec[:4], "...", sec[-4:], "len", len(sec))

sdk = XummSdk(key.strip(), sec.strip())
pong = sdk.ping()
print("APP:", pong.application.name, "disabled:", pong.application.disabled)
