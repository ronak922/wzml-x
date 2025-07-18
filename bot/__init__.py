# ruff: noqa: E402

from uvloop import install

install()

from subprocess import run as srun
from os import getcwd
from asyncio import Lock, new_event_loop, set_event_loop
from logging import (
    ERROR,
    INFO,
    WARNING,
    FileHandler,
    StreamHandler,
    basicConfig,
    getLogger,
)
from os import cpu_count
from time import time

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import utils as pyroutils

from .core.config_manager import BinConfig
from sabnzbdapi import SabnzbdClient

getLogger("requests").setLevel(WARNING)
getLogger("urllib3").setLevel(WARNING)
getLogger("pyrogram").setLevel(ERROR)
getLogger("aiohttp").setLevel(ERROR)
getLogger("apscheduler").setLevel(ERROR)
getLogger("httpx").setLevel(WARNING)
getLogger("pymongo").setLevel(WARNING)
getLogger("aiohttp").setLevel(WARNING)

pyroutils.MIN_CHAT_ID = -999999999999
pyroutils.MIN_CHANNEL_ID = -100999999999999
bot_start_time = time()

bot_loop = new_event_loop()
set_event_loop(bot_loop)

basicConfig(
    format="[%(asctime)s] [%(levelname)s] - %(message)s",  #  [%(filename)s:%(lineno)d]
    datefmt="%d-%b-%y %I:%M:%S %p",
    handlers=[FileHandler("log.txt"), StreamHandler()],
    level=INFO,
)

LOGGER = getLogger(__name__)
cpu_no = cpu_count()

bot_cache = {}
DOWNLOAD_DIR = "/usr/src/app/downloads/"
intervals = {"status": {}, "qb": "", "jd": "", "nzb": "", "stopAll": False}
qb_torrents = {}
jd_downloads = {}
nzb_jobs = {}
user_data = {}
aria2_options = {}
qbit_options = {}
nzb_options = {}
queued_dl = {}
queued_up = {}
status_dict = {}
task_dict = {}
rss_dict = {}
shortener_dict = {}
var_list = [
    "BOT_TOKEN",
    "TELEGRAM_API",
    "TELEGRAM_HASH",
    "OWNER_ID",
    "DATABASE_URL",
    "BASE_URL",
    "UPSTREAM_REPO",
    "UPSTREAM_BRANCH",
    "UPDATE_PKGS",
]
auth_chats = {}
excluded_extensions = ["aria2", "!qB"]
drives_names = []
drives_ids = []
index_urls = []
sudo_users = []
non_queued_dl = set()
non_queued_up = set()
multi_tags = set()
task_dict_lock = Lock()
queue_dict_lock = Lock()
qb_listener_lock = Lock()
nzb_listener_lock = Lock()
jd_listener_lock = Lock()
cpu_eater_lock = Lock()
same_directory_lock = Lock()

def load_shorteners_from_file():
    """Load shorteners from shortener.txt file"""
    try:
        with open("shortener.txt", "r") as f:
            content = f.read().strip()
            
        if not content:
            LOGGER.warning("shortener.txt is empty")
            return
            
        LOGGER.info("📄 Loading shorteners from shortener.txt...")
        
        # Your current format: vplink.in8fa658d2aed43901d0fbd8ce7868dc73ddd01820
        shortener_configs = {
            "vplink.in": "",
            "linkshortify.com": "",
            "ouo.io": "",
            "bitly.com": "",
            "cutt.ly": "",
            "shorte.st": ""
        }
        
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Check each known domain
            for domain in shortener_configs.keys():
                if domain in line:
                    api_key = line.replace(domain, "").strip()
                    if api_key:
                        shortener_dict[domain] = api_key
                        LOGGER.info(f"✅ Loaded shortener: {domain}")
                    break
                        
    except FileNotFoundError:
        LOGGER.warning("❌ shortener.txt file not found")
    except Exception as e:
        LOGGER.error(f"❌ Error loading shorteners: {e}")

# Load shorteners from file
load_shorteners_from_file()
LOGGER.info(f"📋 Total shorteners loaded: {len(shortener_dict)}")
if shortener_dict:
    LOGGER.info(f"🔗 Available shorteners: {list(shortener_dict.keys())}")
else:
    LOGGER.warning("⚠️ No shorteners configured! Verification links will be direct.")

sabnzbd_client = SabnzbdClient(
    host="http://localhost",
    api_key="admin",
    port="8070",
)
srun([BinConfig.QBIT_NAME, "-d", f"--profile={getcwd()}"], check=False)

scheduler = AsyncIOScheduler(event_loop=bot_loop)
