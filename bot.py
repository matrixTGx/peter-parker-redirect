import logging.config
import asyncio
import sys
import os
from pyrogram import Client, __version__ as pyrogram_version, utils as pyroutils
from pyrogram.raw.all import layer as pyrogram_layer
from database.users_chats_db import db
from info import SESSION, API_ID, API_HASH, BOT_TOKEN, LOG_STR, REQ_CHANNEL1, REQ_CHANNEL2, LOG_CHANNEL, OWNER_ID
from utils import temp, load_datas, delete_messages_loop
from typing import Union, Optional, AsyncGenerator
from pyrogram import types
from apscheduler.schedulers.asyncio import AsyncIOScheduler

pyroutils.MIN_CHAT_ID = -999999999999
pyroutils.MIN_CHANNEL_ID = -100999999999999

logging.config.fileConfig('logging.conf')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger('apscheduler').setLevel(logging.WARNING)

class PremiumLogFormatter(logging.Formatter):
    GREY = "\x1b[38;20m"
    BLUE = "\x1b[34;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: f"{GREY}[DEBUG] %(asctime)s - %(name)s - %(message)s{RESET}",
        logging.INFO: f"{BLUE}[INFO] %(asctime)s - %(name)s - %(message)s{RESET}",
        logging.WARNING: f"{YELLOW}[WARNING] %(asctime)s - %(name)s - %(message)s{RESET}",
        logging.ERROR: f"{RED}[ERROR] %(asctime)s - %(name)s - %(message)s{RESET}",
        logging.CRITICAL: f"{BOLD_RED}[CRITICAL] %(asctime)s - %(name)s - %(message)s{RESET}"
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)

for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(PremiumLogFormatter())

class Bot(Client):
    def __init__(self):
        super().__init__(
            name=SESSION,
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            workers=100,
            plugins={"root": "plugins"},
            sleep_threshold=5,
        )
        self.schedule = AsyncIOScheduler()
        self.username = None
        self.req_link1 = None
        self.req_link2 = None
        self.req_link3 = None

    async def start(self, **kwargs):
        startup_banner = r"""
██████╗  ██████╗ ████████╗
██╔══██╗██╔═══██╗╚══██╔══╝
██████╔╝██║   ██║   ██║
██╔══██╗██║   ██║   ██║
██████╔╝╚██████╔╝   ██║
╚═════╝  ╚═════╝    ╚═╝     🚀 Bot Initializing...
"""
        print(f"\n{startup_banner}\n")
        logger.info("🎬 Initializing bot components...")

        try:
            banned_users, banned_chats = await db.get_banned()
            temp.BANNED_USERS = banned_users
            temp.BANNED_CHATS = banned_chats
            logger.info("🔄 Loaded banned users and chats from database.")
        except Exception as e:
            logger.error(f"❌ Failed to load banned users/chats: {e}", exc_info=True)
            sys.exit(1)

        try:
            await super().start()
            me = await self.get_me()
            await load_datas(me.id)
            self.schedule.start()

            temp.ME = me.id
            temp.U_NAME = "NeelizBot"
            temp.B_NAME = me.first_name
            self.username = f'@{me.username}'

            logger.info(f"✨ Bot connected successfully! Name: {me.first_name} (@{me.username})")
            logger.info(f"Pyrogram Version: v{pyrogram_version} (Layer {pyrogram_layer})")

            if temp.REQ_CHANNEL1:
                try:
                    link = await self.create_chat_invite_link(chat_id=int(temp.REQ_CHANNEL1), creates_join_request=temp.REQ1)
                    self.req_link1 = link.invite_link
                    logger.info(f"🔗 Invite Link 1 set: {self.req_link1}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to create invite link for REQ_CHANNEL1 ({REQ_CHANNEL1}): {e}")
            else:
                logger.debug("REQ_CHANNEL1 not configured. Skipping invite link creation.")

            if temp.REQ_CHANNEL2:
                try:
                    link = await self.create_chat_invite_link(chat_id=int(temp.REQ_CHANNEL2), creates_join_request=temp.REQ2)
                    self.req_link2 = link.invite_link
                    logger.info(f"🔗 Invite Link 2 set: {self.req_link2}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to create invite link for REQ_CHANNEL2 ({REQ_CHANNEL2}): {e}")
            else:
                logger.debug("REQ_CHANNEL2 not configured. Skipping invite link creation.")

            if temp.REQ_CHANNEL3:
                try:
                    link = await self.create_chat_invite_link(chat_id=int(temp.REQ_CHANNEL3), creates_join_request=temp.REQ3)
                    self.req_link3 = link.invite_link
                    logger.info(f"🔗 Invite Link 3 set: {self.req_link3}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to create invite link for REQ_CHANNEL3 ({REQ_CHANNEL3}): {e}")
            else:
                logger.debug("REQ_CHANNEL3 not configured. Skipping invite link creation.")

            if temp.REQ_CHANNEL1_2:
                try:
                    link = await self.create_chat_invite_link(chat_id=int(temp.REQ_CHANNEL1_2), creates_join_request=temp.REQ1_2)
                    self.req_link1_2 = link.invite_link
                    logger.info(f"🔗 Invite Link 1_2 set: {self.req_link1_2}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to create invite link for REQ_CHANNEL1 ({REQ_CHANNEL1}): {e}")
            else:
                logger.debug("REQ_CHANNEL1_2 not configured. Skipping invite link creation.")

            if temp.REQ_CHANNEL2_2:
                try:
                    link = await self.create_chat_invite_link(chat_id=int(temp.REQ_CHANNEL2_2), creates_join_request=temp.REQ2_2)
                    self.req_link2_2 = link.invite_link
                    logger.info(f"🔗 Invite Link 2_2 set: {self.req_link2_2}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to create invite link for REQ_CHANNEL2_2 ({REQ_CHANNEL2}): {e}")
            else:
                logger.debug("REQ_CHANNEL2_2 not configured. Skipping invite link creation.")

            if temp.REQ_CHANNEL3_2:
                try:
                    link = await self.create_chat_invite_link(chat_id=int(temp.REQ_CHANNEL3_2), creates_join_request=temp.REQ3_2)
                    self.req_link3_2 = link.invite_link
                    logger.info(f"🔗 Invite Link 3_2 set: {self.req_link3_2}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to create invite link for REQ_CHANNEL3_2 ({REQ_CHANNEL3}): {e}")
            else:
                logger.debug("REQ_CHANNEL3_2 not configured. Skipping invite link creation.")

            asyncio.create_task(delete_messages_loop(self), name="delete_messages_loop_task")
            logger.info("⚙️ Started background task: Message Cleanup Loop.")

            from sql.db import migrate_to_sql
            await migrate_to_sql()
            logger.info("🗄️ Database migration check completed.")
            
            if OWNER_ID:
                try:
                    await self.send_message(chat_id=int(OWNER_ID), text="🟢 **Bot Restarted Successfully!**\n\n_All systems online._")
                    logger.info(f"✅ Sent restart notification to owner (ID: {OWNER_ID}).")
                except Exception as e:
                    logger.error(f"❌ Failed to send restart message to OWNER_ID ({OWNER_ID}): {e}")
            else:
                logger.warning("OWNER_ID not set. Cannot send restart notification.")        
        except Exception as e:
            logger.critical(f"❌ An unrecoverable error occurred during startup: {e}", exc_info=True)
            sys.exit(1)
            
            logger.info("✅ Bot startup process completed.")

            
    async def stop(self, *args):
        logger.info("🛑 Shutting down bot...")
        try:
            if self.schedule.running:
                self.schedule.shutdown()
                logger.info("⏰ APScheduler shut down.")
        except Exception as e:
            logger.warning(f"⚠️ Error shutting down APScheduler: {e}")

        try:
            await super().stop()
            logger.info("👋 Bot disconnected from Telegram API.")
        except Exception as e:
            logger.warning(f"⚠️ Error during Pyrogram client stop: {e}")

        logger.info("✅ Bot shutdown complete. Goodbye!")

app = Bot()
app.run()
