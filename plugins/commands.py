import os
import sys
import logging
import asyncio
import base64
from datetime import datetime, timedelta
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from Script import script
from database.ia_filterdb import Media2, Media3, get_file_details, unpack_new_file_id
from database.users_chats_db import db
from info import ADMINS, LOG_CHANNEL, CUSTOM_FILE_CAPTION, AUTO_DEL
from utils import get_size, is_requested_one, is_requested_two, is_requested_three, temp, check_loop_sub, check_loop_sub1, check_loop_sub2, check_loop_sub3
from sql.db import delete_all_files_sql, delete_file_sql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DELETE_TXT = "❗️This message will be auto-deleted in 5 minutes due to copyright issues. Please forward it to your saved messages."

async def send_log_report(client, user, item, report_type):
    """Sends a formatted report (bug/request) to the log channel."""
    header = "#BugReport" if report_type == "bug" else "#Request"
    item_label = "Description" if report_type == "bug" else "Requested Item"

    user_info = f"**User:** {user.mention}\n**User ID:** `{user.id}`"
    report_message = f"{header}\n\n**{item_label}:** {item}\n\n{user_info}"

    try:
        await client.send_message(chat_id=LOG_CHANNEL, text=report_message)
        return True
    except Exception as e:
        logger.error(f"Failed to send {report_type} to log channel: {e}")
        return False

async def auto_delete_message(message, delay_seconds):
    """Deletes a message after a specified delay."""
    await asyncio.sleep(delay_seconds)
    try:
        await message.delete()
    except Exception as e:
        logger.warning(f"Failed to auto-delete message {message.id}: {e}")
        
@Client.on_message(filters.command("start") & filters.incoming)
async def start(client, message):
    chat_type = message.chat.type
    user_id = message.from_user.id if message.from_user else None
    chat_id = message.chat.id

    if chat_type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        buttons = [[InlineKeyboardButton('🤖 Updates', url='https://t.me/Cinema_Kottaka_updates')],
                   [InlineKeyboardButton('ℹ️ Help', url=f"https://t.me/{temp.U_NAME}?start=help")]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await message.reply(script.START_TXT.format(message.from_user.mention if user_id else message.chat.title, temp.B_NAME), reply_markup=reply_markup)
        if not await db.get_chat(chat_id):
            total = await client.get_chat_members_count(chat_id)
            await client.send_message(LOG_CHANNEL, script.LOG_TEXT_G.format(message.chat.title, chat_id, total, "Unknown"))
            await db.add_chat(chat_id, message.chat.title)
        return

    if user_id and not await db.is_user_exist(user_id):
        await db.add_user(user_id, message.from_user.first_name)
        await client.send_message(LOG_CHANNEL, script.LOG_TEXT_P.format(user_id, message.from_user.mention))

    if len(message.command) != 2:
        buttons = [[
            InlineKeyboardButton('➕ 𝙰𝚍𝚍 𝙼𝚎 𝚃𝚘 𝚈𝚘𝚞𝚛 𝙶𝚛𝚘𝚞𝚙𝚜 ➕', url=f'http://t.me/{temp.U_NAME}?startgroup=true')
        ],[
            InlineKeyboardButton('💌 Updates Channel', url='https://t.me/Cinema_Kottaka_updates'),
            InlineKeyboardButton('📈 Bot Statistics', callback_data='stats')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await message.reply_text(
            text=script.START_TXT.format(message.from_user.mention if user_id else "User", temp.U_NAME, temp.B_NAME),
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
        return

    try:
        if temp.REQ_CHANNEL3:
            joined_ch1=True
            joined_ch2=True
            if temp.REQ_CHANNEL1:
                joined_ch1 = await is_requested_one(client, message)
            if temp.REQ_CHANNEL2:
                joined_ch2 = await is_requested_two(client, message)
            if joined_ch1 and joined_ch2:
                if not await is_requested_three(client, message):
                    btn = [[
                        InlineKeyboardButton("〄 Rᴇǫᴜᴇsᴛ Tᴏ Jᴏɪɴ Cʜᴀɴɴᴇʟ 3 〄", url=client.req_link3)
                    ]]
                    if message.command[1] != "subscribe":
                        try:
                            kk, file_id = message.command[1].split("_", 1)
                            pre = 'checksubp' if kk == 'filep' else 'checksub'
                            btn.append([InlineKeyboardButton("〄 Tʀʏ Aɢᴀɪɴ 〄", callback_data=f"{pre}#{file_id}")])
                        except (IndexError, ValueError):
                            btn.append([InlineKeyboardButton("〄 Tʀʏ Aɢᴀɪɴ 〄", url=f"https://t.me/{temp.U_NAME}?start={message.command[1]}")])

                    sh = await client.send_message(
                        chat_id=message.from_user.id,
                        text='📢 𝐑𝐞𝐪𝐮𝐞𝐬𝐭 𝐓𝐨 𝐉𝐨𝐢𝐧 𝐂𝐡𝐚𝐧𝐧𝐞𝐥 3 📢  ക്ലിക്ക് ചെയ്ത ശേഷം 🔄 𝐓𝐫𝐲 𝐀𝐠𝐚𝐢𝐧 🔄 എന്ന ബട്ടണിൽ അമർത്തിയാൽ നിങ്ങൾക്ക് ഞാൻ ആ സിനിമ അയച്ചു തരുന്നതാണ് 😍',
                        reply_markup=InlineKeyboardMarkup(btn),
                        parse_mode=enums.ParseMode.MARKDOWN
                    )

                    check = await check_loop_sub3(client, message)
                    if check:
                        await sh.delete()
                    else:
                        return

        should_run_check_loop_sub = False
        should_run_check_loop_sub1 = False

        if temp.REQ_CHANNEL1 and not await is_requested_one(client, message):
            btn = [[
                InlineKeyboardButton("〄 Rᴇǫᴜᴇsᴛ Tᴏ Jᴏɪɴ Cʜᴀɴɴᴇʟ 1 〄", url=client.req_link1)
            ]]
            should_run_check_loop_sub1 = True

            try:
                if temp.REQ_CHANNEL2 and not await is_requested_two(client, message):
                    btn.append([
                        InlineKeyboardButton("〄 Rᴇǫᴜᴇsᴛ Tᴏ Jᴏɪɴ Cʜᴀɴɴᴇʟ 2 〄", url=client.req_link2)
                    ])
                    should_run_check_loop_sub = True
            except Exception as e:
                print(e)

            if message.command[1] != "subscribe":
                try:
                    kk, file_id = message.command[1].split("_", 1)
                    pre = 'checksubp' if kk == 'filep' else 'checksub'
                    btn.append([InlineKeyboardButton("〄 Tʀʏ Aɢᴀɪɴ 〄", callback_data=f"{pre}#{file_id}")])
                except (IndexError, ValueError):
                    btn.append([InlineKeyboardButton("〄 Tʀʏ Aɢᴀɪɴ 〄", url=f"https://t.me/{temp.U_NAME}?start={message.command[1]}")])

            sh = await client.send_message(
                chat_id=message.from_user.id,
                text='📢 𝐑𝐞𝐪𝐮𝐞𝐬ᴛ 𝐓𝐨 𝐉𝐨𝐢𝐧 𝐂𝐡𝐚𝐧𝐧𝐞𝐥 📢  ക്ലിക്ക് ചെയ്ത ശേഷം 🔄 𝐓𝐫𝐲 𝐀𝐠ᴀ𝐢𝐧 🔄 എന്ന ബട്ടണിൽ അമർത്തിയാൽ നിങ്ങൾക്ക് ഞാൻ ആ സിനിമ അയച്ചു തരുന്നതാണ് 😍',
                reply_markup=InlineKeyboardMarkup(btn),
                parse_mode=enums.ParseMode.MARKDOWN
            )

            if should_run_check_loop_sub:
                check = await check_loop_sub(client, message)
            elif should_run_check_loop_sub1:
                check = await check_loop_sub1(client, message)

            if check:
                await sh.delete()
            else:
                return

    except Exception as e:
        return await message.reply(str(e))

    if temp.REQ_CHANNEL2 and not await is_requested_two(client, message):
        btn = [[
            InlineKeyboardButton("Join channel", url=client.req_link2)
        ]]
        if message.command[1] != "subscribe":
            try:
                kk, file_id = message.command[1].split("_", 1)
                pre = 'checksubp' if kk == 'filep' else 'checksub'
                btn.append([InlineKeyboardButton(" 🔄 Tʀʏ Aɢᴀɪɴ 🔄", callback_data=f"{pre}#{file_id}")])
            except (IndexError, ValueError):
                btn.append([InlineKeyboardButton(" 🔄 Tʀʏ Aɢᴀɪɴ 🔄", url=f"https://t.me/{temp.U_NAME}?start={message.command[1]}")])
        sh = await client.send_message(
            chat_id=message.from_user.id,
            text='📢 𝐑𝐞𝐪𝐮𝐞𝐬ᴛ 𝐓𝐨 𝐉𝐨𝐢𝐧 𝐂𝐡𝐚𝐧𝐧𝐞𝐥 📢  ക്ലിക്ക് ചെയ്ത ശേഷം 🔄 𝐓𝐫𝐲 𝐀𝐠ᴀ𝐢𝐧 🔄 എന്ന ബട്ടണിൽ അമർത്തിയാൽ നിങ്ങൾക്ക് ഞാൻ ആ സിനിമ അയച്ചു തരുന്നതാണ് 😍',
            reply_markup=InlineKeyboardMarkup(btn),
            parse_mode=enums.ParseMode.MARKDOWN
        )
        check = await check_loop_sub2(client, message)
        if check:
            await sh.delete()
        else:
            return

    if len(message.command) == 2 and message.command[1] in ["subscribe", "error", "okay", "help"]:
        buttons = [[
            InlineKeyboardButton('➕ 𝙰𝚍𝚍 𝙼𝚎 𝚃𝚘 𝚈ᴏ𝚞𝚛 𝙶𝚛𝚘𝚞𝚙𝚜 ➕', url=f'http://t.me/{temp.U_NAME}?startgroup=true')
        ],[
            InlineKeyboardButton('💌 Updates Channel', url='https://t.me/Cinema_Kottaka_updates'),
            InlineKeyboardButton('📈 Bot Statistics', callback_data='stats')
        ]]
        reply_markup = InlineKeyboardMarkup(buttons)
        await message.reply_text(
            text=script.START_TXT.format(message.from_user.mention if user_id else "User", temp.U_NAME, temp.B_NAME),
            reply_markup=reply_markup,
            parse_mode=enums.ParseMode.HTML
        )
        return

    data = message.command[1]
    try:
        prefix, file_id = data.split('_', 1)
    except ValueError:
        file_id = data
        prefix = ""

    files_ = await get_file_details(file_id)
    if not files_:
        try:
            decoded_data = (base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))).decode("ascii")
            prefix, file_id = decoded_data.split("_", 1)
            msg = await client.send_cached_media(
                chat_id=user_id,
                file_id=file_id,
                protect_content=True if prefix == 'filep' else False,
            )
            file_type = msg.media
            file = getattr(msg, file_type)
            title = file.file_name
            size = get_size(file.file_size)
            f_caption = f"<code>{title}</code>"
            if CUSTOM_FILE_CAPTION:
                try:
                    f_caption=CUSTOM_FILE_CAPTION.format(file_name= '' if title is None else title, file_size='' if size is None else size, file_caption='')
                except:
                    return
            await msg.edit_caption(f_caption)
            client.schedule.add_job(msg.delete, 'date', run_date=datetime.now() + timedelta(seconds=AUTO_DEL))
            return
        except Exception:
            return await message.reply('No such file exists.')

    files = files_[0]
    title = files.file_name
    size = get_size(files.file_size)
    f_caption=files.caption
    if CUSTOM_FILE_CAPTION:
        try:
            f_caption=CUSTOM_FILE_CAPTION.format(file_name= '' if title is None else title, file_size='' if size is None else size, file_caption='' if f_caption is None else f_caption)
        except Exception as e:
            logger.exception(e)
            f_caption=f_caption
    if f_caption is None:
        f_caption = f"{files.file_name}"

    try:
        ok = await client.send_cached_media(
            chat_id=user_id,
            file_id=file_id,
            caption=f_caption,
            protect_content=True if prefix == 'filep' else False,
        )
        delete_notice = await ok.reply(f"<u>{DELETE_TXT}</u>")
        asyncio.create_task(auto_delete_message(delete_notice, 30))
        client.schedule.add_job(ok.delete, 'date', run_date=datetime.now() + timedelta(seconds=AUTO_DEL))
        
    except Exception as e:
        logger.error(f"Error sending cached media: {e}")
