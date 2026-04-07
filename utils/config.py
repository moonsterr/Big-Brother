import os
from dotenv import load_dotenv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / ".env"

load_dotenv(dotenv_path=env_path)

if not env_path.exists():
    print(f"!!! CRITICAL: .env not found at {env_path}")
else:
    print(f"[*] .env detected at: {env_path}")
    print(f"proxy_url is {os.getenv("PROXY_URL")}")
REDDIT_CONFIG = {
    "client_id": "YOUR_ID",
    "client_secret": "YOUR_SECRET",
    "user_agent": "python:archiver.v1:1.0 (by /u/YourUser)",
    "username": "YourUser",
    "password": "YourPassword"
}

PROXY_CONFIG = {
    "url": os.getenv("PROXY_URL"),
    "enabled": os.getenv("PROXY_ENABLED", "False").lower() == "true"
}