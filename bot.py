import os, sys, glob, importlib, logging, logging.config, pytz, asyncio
from pathlib import Path

# Get logging configurations
logging.config.fileConfig('logging.conf')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("imdbpy").setLevel(logging.ERROR)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

from requests import get as rget
from os import environ

CONFIG_FILE_URL = environ.get('CONFIG_FILE_URL')
try:
    if not CONFIG_FILE_URL:
        raise ValueError("CONFIG_FILE_URL is missing or empty")

    res = rget(CONFIG_FILE_URL)

    if res.status_code == 200:
        # Write the content to info.py file
        with open('info.py', 'wb+') as f:
            f.write(res.content)
        logging.info("info.py downloaded successfully!")
    else:
        logging.error(f"Failed to download info.py: {res.status_code}")
except Exception as e:
    logging.error(f"Error downloading CONFIG_FILE_URL: {e}")

# Import info.py after downloading
from info import *

from pyrogram import Client, idle
from database.ia_filterdb import Media
from database.users_chats_db import db
from utils import temp
from typing import Union, Optional, AsyncGenerator
from script import script
from datetime import date, datetime 
from aiohttp import web
from plugins import web_server
from plugins.clone import restart_bots

from stream.bot import KPSBot
from stream.util.keepalive import ping_server
from stream.bot.clients import initialize_clients


ppath = "plugins/*.py"
files = glob.glob(ppath)
KPSBot.start()
loop = asyncio.get_event_loop()

async def start():
    print('Initalizing Your Bot')
    bot_info = await KPSBot.get_me()
    await initialize_clients()
    for name in files:
        with open(name) as a:
            patt = Path(a.name)
            plugin_name = patt.stem.replace(".py", "")
            plugins_dir = Path(f"plugins/{plugin_name}.py")
            import_path = "plugins.{}".format(plugin_name)
            spec = importlib.util.spec_from_file_location(import_path, plugins_dir)
            load = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(load)
            sys.modules["plugins." + plugin_name] = load
            print("Imported => " + plugin_name)
    if ON_HEROKU:
        asyncio.create_task(ping_server())
    b_users, b_chats = await db.get_banned()
    temp.BANNED_USERS = b_users
    temp.BANNED_CHATS = b_chats
    await Media.ensure_indexes()
    me = await KPSBot.get_me()
    temp.BOT = KPSBot
    temp.ME = me.id
    temp.U_NAME = me.username
    temp.B_NAME = me.first_name
    logging.info(LOG_STR)
    logging.info(script.LOGO)
    tz = pytz.timezone('Asia/Kolkata')
    today = date.today()
    now = datetime.now(tz)
    time = now.strftime("%H:%M:%S %p")
    await KPSBot.send_message(chat_id=LOG_CHANNEL, text=script.RESTART_TXT.format(today, time))
    if CLONE_MODE == True:
        print("Restarting All Clone Bots.......")
        await restart_bots()
        print("Restarted All Clone Bots.")
    app = web.AppRunner(await web_server())
    await app.setup()
    bind_address = "0.0.0.0"
    await web.TCPSite(app, bind_address, PORT).start()
    await idle()

async def restart_loop():
    try:
        if RANDOM_UPDATES == True:
            try:
                await asyncio.wait_for(start(), timeout= TIMEOUT * 60)
                print(f'🔄 Next Restart for in {TIMEOUT} minutes..')
                
                #os.system("python3 get_config.py")
                os.execl(sys.executable, sys.executable, *sys.argv)
            except KeyboardInterrupt:
                logging.info('Service Stopped Bye 👋')
            except Exception as e:
                logging.error(f"Error occurred: {e}")
        else:
            await start()
    except:
            await start()


if __name__ == '__main__':
    try:
        loop.run_until_complete(restart_loop())
    except KeyboardInterrupt:
        logging.info('Service Stopped Bye 👋')
