import asyncio

import json

import os

import time

from pyrogram import Client

from pyrogram.errors import FloodWait



# ==================== CONFIGURATION ====================

ACCOUNTS = [

    {"phone": "919450524237", "api_id": 24534486, "api_hash": "929e4b47d89fb65f3123bfbb32fd7a93", "session": "account_1"},

    {"phone": "916393661032", "api_id": 29391397, "api_hash": "a2a8d15535da9fba51239f15855d43fa", "session": "account_2"},

    {"phone": "917232988890", "api_id": 25303943, "api_hash": "c60cf19c850848f6d342fcfb4ebe89e0", "session": "account_3"},

  #  {"phone": "916358898459", "api_id": 31481718, "api_hash": "b147759f5e74a6260a250ce556d08d78", "session": "account_4"} 

]



# SOURCE: https://t.me/c/3675228230/530936 (Movie's Hub Bulk)
# Channel ID format: -100 + channel_id
SOURCE_CHANNEL = "-1003675228230"  # Movie's Hub Bulk channel
TARGET_CHANNEL = "mymovie2"

# Starting message ID from the Telegram link
STARTING_MESSAGE_ID = 530841



# Speed controls

MESSAGES_PER_BATCH = 200     # Fetch 200 messages at a time (Keep this moderate to avoid API timeouts)

SPEED_PER_ACCOUNT = 50       # Files per minute per account

PROGRESS_FILE = "copy_progress.json"



progress_lock = asyncio.Lock()



# ==================== PROGRESS MANAGEMENT ====================

def load_progress():

    if os.path.exists(PROGRESS_FILE):

        try:

            with open(PROGRESS_FILE, 'r') as f:

                data = json.load(f)

                # Ensure we have the key we need

                if "last_processed_id" not in data:

                    data["last_processed_id"] = STARTING_MESSAGE_ID - 1

                return data

        except:

            pass

    # Default start point if file doesn't exist - start from message 530936
    return {"last_processed_id": STARTING_MESSAGE_ID - 1}



async def update_progress(last_id):

    async with progress_lock:

        # Load current to compare

        current = load_progress()

        # Only update if the new ID is higher (don't go backwards)

        if last_id > current['last_processed_id']:

            with open(PROGRESS_FILE, 'w') as f:

                json.dump({"last_processed_id": last_id}, f, indent=2)



# ==================== CHANNEL RESOLVER ====================

async def get_channel_by_name(client, channel_identifier):

    """
    Get channel by username or ID.
    For channel IDs (starting with -100), use them directly.
    For usernames, search in dialogs.
    ALWAYS returns integer ID, not Chat object.
    """
    # If it's a channel ID (starts with -100), return it as integer
    if isinstance(channel_identifier, str) and channel_identifier.startswith("-100"):
        try:
            return int(channel_identifier)
        except:
            pass
    
    # Otherwise search by title/username and return the ID
    async for dialog in client.get_dialogs():

        if dialog.chat and dialog.chat.title and dialog.chat.title == channel_identifier:

            return dialog.chat.id  # Return ID, not Chat object

    return None



# ==================== ID BASED SCANNER (NO HISTORY SEARCH) ====================

async def scan_next_batch(client, source_chat_id, start_id):

    """

    Mathematically generates the next list of IDs to check.

    Does not search history, it just grabs the specific IDs.

    """

    print(f"\n📊 Fetching next {MESSAGES_PER_BATCH} messages starting from ID {start_id + 1}...")

    

    # Create a list of IDs to fetch: [530936, 530937, 530938, ... 531136]

    ids_to_fetch = list(range(start_id + 1, start_id + 1 + MESSAGES_PER_BATCH))

    

    valid_files = []

    try:

        # Get messages by explicit ID list

        messages = await client.get_messages(source_chat_id, ids_to_fetch)

        

        # Determine strict range (some IDs might be empty/deleted)

        # We process the list returned by get_messages

        for msg in messages:

            if not msg or msg.empty:

                continue

                

            # Check for media

            media = msg.document or msg.video or msg.audio

            if media:

                file_name = getattr(media, 'file_name', 'Unknown')

                valid_files.append({

                    'msg_id': msg.id,

                    'file_name': file_name

                })

                

    except Exception as e:

        print(f"❌ Error scanning batch: {e}")

        return [], start_id # Return empty list and same ID to retry or stop



    print(f"✅ Found {len(valid_files)} valid files in this range.")

    

    # If no messages found, we still return the new 'end' ID so the loop can move forward

    # Otherwise we get stuck on empty ID ranges forever.

    last_scanned_id = ids_to_fetch[-1]

    

    return valid_files, last_scanned_id



# ==================== COPY WORKER ====================

async def copy_batch(clients, source_chat_ids, target_chat_ids, files):

    total = len(files)

    if total == 0:

        return



    num_clients = len(clients)

    

    # Distribute files

    chunk_size = (total + num_clients - 1) // num_clients

    chunks = [files[i:i + chunk_size] for i in range(0, total, chunk_size)]

    

    while len(chunks) < num_clients:

        chunks.append([])



    stop_flag = asyncio.Event()

    

    async def copy_worker(client, source_chat_id, target_chat_id, file_list, acc_num):

        # Calculate delay: 50 files/min = 60/50 = 1.2 seconds delay

        delay = 60.0 / SPEED_PER_ACCOUNT

        

        for file_info in file_list:

            if stop_flag.is_set():

                return

            

            msg_id = file_info['msg_id']

            file_name = file_info['file_name']

            

            retry_count = 0

            while True:

                if stop_flag.is_set(): return

                

                start_time = time.time()

                try:

                    # 1. Fetch Fresh Message

                    msg = await client.get_messages(source_chat_id, msg_id)

                    

                    if not msg or msg.empty:

                        print(f"⚠️ Acc {acc_num}: Message {msg_id} is unavailable (deleted/skipped).")

                        break



                    # 2. Send Message

                    if msg.document:

                        await client.send_document(target_chat_id, msg.document.file_id, caption=msg.caption, file_name=msg.document.file_name, force_document=True)

                    elif msg.video:

                        await client.send_video(target_chat_id, msg.video.file_id, caption=msg.caption, file_name=msg.video.file_name, duration=msg.video.duration, width=msg.video.width, height=msg.video.height)

                    elif msg.audio:

                        await client.send_audio(target_chat_id, msg.audio.file_id, caption=msg.caption, file_name=msg.audio.file_name)

                    

                    # 3. SUCCESS - Update Progress Immediately

                    print(f"   Acc {acc_num}: ✅ Sent {file_name} (ID: {msg_id})")

                    await update_progress(msg_id)

                    

                    # 4. Wait for Speed Limit

                    elapsed = time.time() - start_time

                    await asyncio.sleep(max(0, delay - elapsed))

                    break 



                except FloodWait as e:

                    print(f"⏳ Acc {acc_num} FloodWait: {e.value}s")

                    await asyncio.sleep(e.value + 2)

                

                except Exception as e:

                    retry_count += 1

                    print(f"⚠️ Acc {acc_num} Error on {msg_id}: {e}")

                    if retry_count > 5:

                        print(f"❌ Acc {acc_num} skipping {msg_id} after 5 fails.")

                        break

                    await asyncio.sleep(2)



    # Run workers

    tasks = []

    for i in range(num_clients):

        if chunks[i]:

            tasks.append(copy_worker(clients[i], source_chat_ids[i], target_chat_ids[i], chunks[i], i+1))

    

    if tasks:

        await asyncio.gather(*tasks)



# ==================== MAIN ====================

async def main():

    print("╔══════════════════════════════════════════════════════════╗")

    print("║      STRICT ID-BASED COPY BOT (NO SKIPPING)              ║")

    print("║      Starting from Message ID: 530936                    ║")

    print("║      Source: Movie's Hub Bulk (-1003675228230)           ║")

    print("╚══════════════════════════════════════════════════════════╝\n")

    

    # Initialize Progress

    progress = load_progress()

    current_id = progress['last_processed_id']

    print(f"📂 Starting Sequence from ID: {current_id + 1}")



    # Login

    print("🔐 Logging in...")

    clients = []

    for acc in ACCOUNTS:

        try:

            client = Client(acc["session"], api_id=acc["api_id"], api_hash=acc["api_hash"]) 

            await client.start()

            clients.append(client)

            print(f"✅ {acc['phone']} Connected")

        except Exception as e:

            print(f"❌ Failed to login {acc['phone']}: {e}")



    if not clients:

        print("❌ No clients connected.")

        return



    # Connect to Channels

    print("\n🔍 Connecting to channels...")

    source_chat_ids, target_chat_ids = [], []

    for client in clients:

        s = await get_channel_by_name(client, SOURCE_CHANNEL)

        t = await get_channel_by_name(client, TARGET_CHANNEL)

        if not s or not t:

            print(f"❌ Client cannot access channels!")

            print(f"   Source result: {s}")

            print(f"   Target result: {t}")

            return

        source_chat_ids.append(s)

        target_chat_ids.append(t)

    

    print(f"✅ Source Channel ID: {source_chat_ids[0]}")

    print(f"✅ Target Channel ID: {target_chat_ids[0]}")



    # MAIN LOOP

    consecutive_empty_batches = 0

    

    while True:

        # reload progress just in case

        progress = load_progress()

        current_id = progress['last_processed_id']

        

        # 1. Scan Next Batch of IDs

        files, next_scan_end_id = await scan_next_batch(clients[0], source_chat_ids[0], current_id)

        

        # 2. Copy Files if Found

        if files:

            consecutive_empty_batches = 0

            await copy_batch(clients, source_chat_ids, target_chat_ids, files)

            # Update internal counter to the highest ID found in this batch to be safe

            # (Though copy_worker updates file individually, this helps the loop logic)

            max_in_batch = max(f['msg_id'] for f in files)

            if max_in_batch > next_scan_end_id:

                next_scan_end_id = max_in_batch

        else:

            consecutive_empty_batches += 1

            print(f"   (Batch empty, moving range forward to {next_scan_end_id})")

            

            # Update the progress file even if empty, so we don't rescan empty IDs forever

            # This is CRITICAL for skipping gaps in message history

            await update_progress(next_scan_end_id)



        # 3. Stop condition (if we scan 5 empty batches in a row ~1000 messages empty, we are probably at the end)

        if consecutive_empty_batches >= 5:

            print("\n✅ End of Channel reached (1000 IDs checked with no content).")

            break



    for c in clients: 

        await c.stop()



if __name__ == "__main__":

    asyncio.run(main())

