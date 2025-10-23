import re
from pyrogram import Client, filters, enums

@Client.on_message(filters.group & filters.text)
async def group_filter_spam(client, message):
    user = message.from_user
    if not user:
        return
    # Check if user is admin or owner
    try:
        member = await client.get_chat_member(message.chat.id, user.id)
        if member.status in [enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER]:
            return
    except:
        return
    # 18+ spam words
    spam_words = re.compile(
        r"(desi\s+xxx|secret\s+cams|no\s+censorship|crystal\s+clear|steal\s+it|@\w+_bot)",
        re.IGNORECASE
    )
    # Links and usernames
    link_pattern = re.compile(
        r'(?im)(?:https?://|www\.|t\.me/|telegram\.dog/)\S+|@[a-z0-9_]{5,32}\b'
    )
    # If message has spam or link
    text = message.text or ""
    if spam_words.search(text):
        await message.delete()
        await message.reply("⚠️ 18+ content is not allowed. User has been removed.", quote=True)
    elif link_pattern.search(text):
        await message.delete()
        await message.reply("⚠️ Links are not allowed. User has been removed.", quote=True)
