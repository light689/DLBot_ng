import os
import json
import time
import asyncio
import websockets
import random
from http.server import BaseHTTPRequestHandler, HTTPServer
from aiohttp import web
import urllib.parse
import logging
import sqlite3
import sys

# 初始化数据库
def init_db():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS whoami
                 (trip TEXT PRIMARY KEY, description TEXT)''')
    conn.commit()
    conn.close()

# 保存 whoami 数据
def save_whoami(trip, description):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO whoami (trip, description) VALUES (?, ?)", (trip, description))
    conn.commit()
    conn.close()

# 获取 whoami 数据
def get_whoami(trip):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute("SELECT description FROM whoami WHERE trip = ?", (trip,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

# 全局变量用于存储WebSocket连接
whisper_history = {}
websocket = None

# 初始化日志状态文件
if not os.path.exists("log_status.txt"):
    with open("log_status.txt", "w", encoding="utf-8") as status_file:
        status_file.write("0")

def log_message(type, message):
    if not os.path.exists("log_status.txt"):
        with open("log_status.txt", "w", encoding="utf-8") as status_file:
            status_file.write("0")

    with open("log_status.txt", "r+", encoding="utf-8") as status_file:
        status = status_file.read()
        if status == "0":
            if not os.path.exists("log.log"):
                open("log.log", "w", encoding="utf-8").close()
            if not os.path.exists("msg.log"):
                open("msg.log", "w", encoding="utf-8").close()
            with open("log.log", "a", encoding="utf-8") as log_file:
                log_file.write(f"----------------- {time.strftime('%Y-%m-%d %H:%M:%S')} -----------------\n")
            with open("msg.log", "a", encoding="utf-8") as log_file:
                log_file.write(f"----------------- {time.strftime('%Y-%m-%d %H:%M:%S')} -----------------\n")
            status_file.seek(0)
            status_file.write("1")

    with open("log.log", "a", encoding="utf-8") as log_file:
        log_entry = f"{type}：{message}\n"
        log_file.write(log_entry)
        print(log_entry, end="")
    
    if type == "收到消息":
        try:
            message_json = json.loads(message)
            if message_json.get("cmd") == "chat":
                msg_entry = f"[{message_json.get('trip', '')}]{message_json.get('nick', '')}：{message_json.get('text', '')}\n"
            elif message_json.get("cmd") == "info":
                msg_entry = f"系统提示：{message_json.get('text', '')}\n"
            elif message_json.get("cmd") == "warn":
                msg_entry = f"系统警告：{message_json.get('text', '')}\n"
            else:
                return
            
            with open("msg.log", "a", encoding="utf-8") as msg_file:
                msg_file.write(msg_entry)
        except json.JSONDecodeError:
            # 如果消息不是有效的JSON，忽略或记录错误
            pass

# 新增变量和数据结构用于新功能
message_count = 0  # 记录自上次发送消息后收到的chat消息数量
received_messages = []  # 存储接收到的chat消息
last_sent_time = 0  # 记录上次发送消息的时间

# 初始化日志状态文件
if not os.path.exists("log_status.txt"):
    with open("log_status.txt", "w", encoding="utf-8") as status_file:
        status_file.write("0")

async def join_channel(nick, password, channel, ws_link):
    global websocket, message_count, received_messages, last_sent_time
    uri = ws_link
    current_nick = nick  # 使用 current_nick 代替 nick 进行修改
    full_nick = f"{current_nick}#{password}"  # 包含密码的完整昵称
    pause_until = None  # 新增变量，用于记录需要暂停到的时间

    async def send_color_message(ws):
        while True:
            color = f"{random.randint(0, 255):02x}{random.randint(0, 255):02x}{random.randint(0, 255):02x}"
            color_message = {"cmd": "chat", "text": f"/color #{color}", "customId": "0"}
            await ws.send(json.dumps(color_message))
            log_message("发送消息", json.dumps(color_message))
            await asyncio.sleep(10)

    async def handle_messages(ws):
        nonlocal current_nick, full_nick  # 声明 nonlocal 以便修改外部变量
        nonlocal pause_until  # 声明 nonlocal，以便在接收到特定警告时修改 pause_until
        nonlocal message_count, received_messages, last_sent_time  # 声明用于新功能的变量
        send_color_task = asyncio.create_task(send_color_message(ws))
        initial_join_time = time.time()
        while True:
            try:
                response = await ws.recv()
                log_message("收到消息", response)
                message = json.loads(response)
                
                if message.get("cmd") == "warn":
                    warn_text = message.get("text", "")
                    # 检查特定的警告信息
                    if "Your account is only allowed to connect" in warn_text:
                        log_message("系统日志", f"收到限频警告：{warn_text}。暂停60秒。")
                        pause_until = time.time() + 60  # 设置暂停时间
                        await ws.close()
                        send_color_task.cancel()
                        try:
                            await send_color_task
                        except asyncio.CancelledError:
                            pass
                        break  # 退出当前循环，等待重新连接
                    elif "Nickname must consist of up to 24 letters, numbers, and underscores" in warn_text:
                        log_message("系统日志", f"收到昵称格式警告：{warn_text}。移除昵称中的下划线并重新加入频道。")
                        current_nick = current_nick.replace("_", "")
                        full_nick = f"{current_nick}#{password}"
                        await ws.close()
                        send_color_task.cancel()
                        try:
                            await send_color_task
                        except asyncio.CancelledError:
                            pass
                        break  # 退出当前循环，等待重新连接
                    elif "You are joining channels too fast. Wait a moment and try again." in warn_text or \
                         "You are being rate-limited or blocked." in warn_text or \
                         "You are sending too much text. Wait a moment try again." in warn_text:
                        break

                if message.get("cmd") == "info":
                    info_text = message.get("text", "")
                    if "You have been denied access to that channel and have been moved somewhere else. Retry later or wait for a mod to move you." == info_text:
                        break

                if message.get("cmd") == "info" and message.get("type") == "whisper":
                    from_user = message.get("from")
                    trip = message.get("trip", "")
                    whisper_content = message.get("text", "")
                    
                    # 忽略以"You whispered to"开头的消息
                    if not whisper_content.startswith("You whispered to"):
                        # 记录私信历史
                        current_time = time.time()
                        if from_user not in whisper_history:
                            whisper_history[from_user] = []
                        whisper_history[from_user].append((current_time, whisper_content))
                        
                        # 检查最近1秒内的私信
                        recent_whispers = [w for w in whisper_history[from_user] if current_time - w[0] <= 1]
                        
                        if len(recent_whispers) >= 3 and all(w[1] == recent_whispers[0][1] for w in recent_whispers):
                            # 发送警告消息
                            warning_message = {
                                "cmd": "chat",
                                "text": f"管理员请注意：[{trip}]{from_user}正在反复向我私信，请注意是否有刷屏现象发生，必要时采取措施。",
                                "customId": "0"
                            }
                            await ws.send(json.dumps(warning_message))
                            log_message("发送消息", json.dumps(warning_message))
                        
                        # 清理旧的私信记录
                        whisper_history[from_user] = [w for w in whisper_history[from_user] if current_time - w[0] <= 10]
                        
                        # 固定的私信回复
                        reply = ".\n本bot目前不支持私信命令使用。"
                        
                        # 发送私信回复
                        whisper_reply = {
                            "cmd": "whisper",
                            "nick": from_user,
                            "text": reply
                        }
                        # await ws.send(json.dumps(whisper_reply))
                        # log_message("发送私信", json.dumps(whisper_reply))


                if message.get("cmd") == "warn" and "Nickname taken" in message.get("text", ""):
                    log_message("系统日志", "Nickname taken, modifying nickname and retrying...")
                    # 修改 nick 和 full_nick，添加下划线
                    current_nick += "_"
                    full_nick = f"{current_nick}#{password}"
                    break

                if message.get("cmd") == "info" and message.get("type") == "whisper" and message.get("trip") == "j156Wo" and "因为有一个相同名称的用户已经在线了" in message.get("text", ""):
                    log_message("系统日志", "Nickname taken, modifying nickname and retrying...")
                    # 修改 nick 和 full_nick，添加下划线
                    current_nick += "_"
                    full_nick = f"{current_nick}#{password}"
                    break
                
                if message.get("cmd") == "onlineSet":
                    startup_message = {"cmd": "chat", "text": "DLBot检测到异常退出，并且顺利重启。 使用`$help`查看帮助。", "customId": "0"}
                    await ws.send(json.dumps(startup_message))
                    log_message("发送消息", json.dumps(startup_message))
                    # 使用当前的 nick 而不是硬编码的 "DLBot"
                    cnick_message = {"cmd": "changenick", "nick": current_nick, "customId": "0"}
                    await ws.send(json.dumps(cnick_message))
                    log_message("系统提示", f"已修改昵称为 {current_nick}")


                if message.get("cmd") == "info" and message.get("type") == "whisper":
                    trip = message.get("trip")
                    if trip in trustedusers:
                        text = message.get("text")
                        if text and "whispered: $chat " in text:
                            msg = text.split("whispered: $chat ", 1)[1]
                            chat_message = {"cmd": "chat", "text": msg, "customId": "0"}
                            await ws.send(json.dumps(chat_message))
                            log_message("发送消息", json.dumps(chat_message))
                
                if message.get("channel") != true_channel and time.time() - initial_join_time > 10:
                    log_message("系统日志", "检测到被踢出，尝试重新加入...")
                    send_color_task.cancel()
                    try:
                        await send_color_task
                    except asyncio.CancelledError:
                        pass
                    break
                
                if message.get("cmd") == "chat" and message.get("text", "").startswith("$chat "):
                    trip = message.get("trip")
                    if trip in trustedusers:
                        msg = message.get("text").split("$chat ", 1)[1]
                        whisper_message = {"cmd": "chat", "text": msg, "customId": "0"}
                        await ws.send(json.dumps(whisper_message))
                        log_message("发送消息", json.dumps(whisper_message))
                
                if message.get("cmd") == "chat" and message.get("text", "").startswith("$whoami "):
                    trip = message.get("trip")
                    nick = message.get("nick", "Unknown")
                    if not trip:
                        error_message = {"cmd": "chat", "text": f"@{nick} 错误：空的识别码不得设置身份信息。请确保你已经使用密码登录。", "customId": "0"}
                        await ws.send(json.dumps(error_message))
                        log_message("发送消息", json.dumps(error_message))
                    else:
                        description = message.get("text").split("$whoami ", 1)[1]
                        save_whoami(trip, description)
                        confirm_message = {"cmd": "chat", "text": f"@{nick} 你的身份描述已设置。", "customId": "0"}
                        await ws.send(json.dumps(confirm_message))
                        log_message("发送消息", json.dumps(confirm_message))

                if message.get("cmd") == "onlineAdd":
                    trip = message.get("trip")
                    nick = message.get("nick")
                    description = get_whoami(trip)
                    if description:
                        welcome_message = {"cmd": "chat", "text": f"@{nick} 的身份： {description}", "customId": "0"}
                        await ws.send(json.dumps(welcome_message))
                        log_message("发送消息", json.dumps(welcome_message))

                if message.get("cmd") == "chat" and message.get("text") == "$help":
                    help_message = {
                        "cmd": "chat",
                        "text": r"""| 指令 | 用途 | 用法 | 需要的权限 |
| --- | --- | --- | --- |
| $help | 显示本页面 | `$help` | 无 |
| $whoami | 设置身份描述 | `$whoami <描述>` （清除： `$whoami<空格>`） | 需要有识别码（使用密码登录） |
| $reload | 重载代码 | `$reload` | 需要是受信任的用户 |""",
                        "customId": "0"
                    }
                    await ws.send(json.dumps(help_message))
                    log_message("发送消息", json.dumps(help_message))

                if message.get("cmd") == "chat" and message.get("text") == "$reload":
                    trip = message.get("trip")
                    if trip in trustedusers:
                        log_message("系统日志", "受信任用户发起重载，正在重载...")
                        try:
                            os.execl(sys.executable, sys.executable, *sys.argv)
                        except Exception as e:
                            error_message = {"cmd": "chat", "text": f"重载失败: {str(e)}", "customId": "0"}
                            await ws.send(json.dumps(error_message))
                            log_message("发送消息", json.dumps(error_message))
                    else:
                        error_message = {"cmd": "chat", "text": "访问被拒绝", "customId": "0"}
                        await ws.send(json.dumps(error_message))
                        log_message("发送消息", json.dumps(error_message))

                # 新增功能处理：记录chat消息并在条件满足时发送随机消息
                if message.get("cmd") == "chat":
                    message_count += 1
                    # 记录收到的消息
                    received_messages.append(message.get("text", ""))
                    
                    # 检查是否达到了100条消息
                    if message_count >= 100:
                        # 在30秒内随机选择一条合格的消息
                        eligible_messages = [msg for msg in received_messages if "\n" not in msg and len(msg) <= 500]
                        if eligible_messages:
                            random_message = random.choice(eligible_messages)
                            # 在前面加上空格
                            send_text = f" {random_message}"
                            await ws.send(json.dumps({"cmd": "chat", "text": send_text, "customId": "0"}))
                            log_message("发送消息", json.dumps({"cmd": "chat", "text": send_text, "customId": "0"}))
                            # 重置计数和消息列表
                            message_count = 0
                            received_messages = []
                            last_sent_time = time.time()
                        else:
                            # 如果没有符合条件的消息，重置计数和消息列表
                            message_count = 0
                            received_messages = []
                    
            except websockets.ConnectionClosed:
                log_message("系统日志", "连接中断，尝试重新连接...")
                send_color_task.cancel()
                try:
                    await send_color_task
                except asyncio.CancelledError:
                    pass
                break

            if message.get("cmd") == "onlineAdd":
                log_message("系统日志", f"{message.get('nick', 'Unknown')}加入了聊天室")
                msg_entry = f"[{message.get('trip', '')}]{message.get('nick', 'Unknown')} 加入了聊天室\n"
                with open("msg.log", "a", encoding="utf-8") as msg_file:
                    msg_file.write(msg_entry)
            elif message.get("cmd") == "onlineRemove":
                log_message("系统日志", f"{message.get('nick', 'Unknown')}退出了聊天室")
                msg_entry = f"[{message.get('trip', '')}]{message.get('nick', 'Unknown')} 退出了聊天室\n"
                with open("msg.log", "a", encoding="utf-8") as msg_file:
                    msg_file.write(msg_entry)

    while True:
        # 在尝试连接前，检查是否需要暂停
        if pause_until and time.time() < pause_until:
            wait_time = pause_until - time.time()
            log_message("系统日志", f"由于限频，暂停 {int(wait_time)} 秒后重试...")
            await asyncio.sleep(wait_time)
            pause_until = None  # 重置暂停时间

        try:
            async with websockets.connect(uri) as ws:
                websocket = ws  # 更新全局的 websocket 变量
                join_message = {"cmd": "join", "channel": channel, "nick": full_nick}  # 使用包含密码的完整昵称
                await ws.send(json.dumps(join_message))
                log_message("系统日志", f"Joined channel {channel} as {current_nick}")
                await handle_messages(ws)
        except (websockets.ConnectionClosed, websockets.InvalidHandshake, websockets.InvalidURI, OSError) as e:
            log_message("系统日志", f"连接错误: {e}, 10秒后重试...")
        log_message("系统日志", f"断开连接。10秒后重试...")
        await asyncio.sleep(10)

class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/chat/'):
            message = self.path.split('/chat/')[1]
            global websocket
            if websocket:
                asyncio.run(websocket.send(json.dumps({"cmd": "chat", "text": message, "customId": "0"})))
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "success", "message": "Message sent"}).encode('utf-8'))
            else:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "WebSocket connection not established"}).encode('utf-8'))
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": "Not found"}).encode('utf-8'))

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data)
        global websocket
        if websocket:
            asyncio.run(websocket.send(json.dumps(data)))
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "success", "message": "JSON message sent"}).encode('utf-8'))
        else:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": "WebSocket connection not established"}).encode('utf-8'))

async def handle_get_recent_messages(request):
    count = int(request.query.get('count', 10))
    try:
        with open("msg.log", "r", encoding="utf-8") as f:
            lines = f.readlines()
            total_lines = len(lines)
            start_line = max(0, total_lines - count)
            messages = lines[start_line:]
            content = ''.join(messages)
    except Exception as e:
        log_message("系统日志", f"读取 msg.log 时发生错误: {str(e)}")
        content = f"错误: {str(e)}"
    
    return web.Response(text=content, content_type='text/plain')

async def start_server():
    app = web.Application()
    app.router.add_get('/', handle_index)
    app.router.add_post('/send_message', handle_send_message)
    app.router.add_post('/send_json', handle_send_json)
    app.router.add_get('/get_recent_messages', handle_get_recent_messages)  
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 18896)
    await site.start()
    print(f"Starting server on http://0.0.0.0:18896")

async def handle_index(request):
    with open('index.html', 'r', encoding='utf-8') as f:
        content = f.read()
    return web.Response(text=content, content_type='text/html')

async def handle_send_message(request):
    data = await request.post()
    message = data['message']
    log_message("系统日志", f"Attempting to send message: {message}")
    success = await send_message(message)
    log_message("系统日志", f"Message sent successfully: {success}")
    return web.json_response({"success": success})

async def handle_send_json(request):
    global websocket
    data = await request.json()
    if websocket:
        await websocket.send(json.dumps(data))
        return web.json_response({"success": True})
    else:
        return web.json_response({"success": False, "error": "WebSocket not connected"})

async def handle_chat(request):
    query_params = request.query_string
    if query_params:
        # URL 解码
        decoded_message = urllib.parse.unquote(query_params)
        # 处理换行符
        decoded_message = decoded_message.replace('\\n', '\n')
    else:
        decoded_message = ""
    
    global websocket
    if websocket:
        chat_message = {"cmd": "chat", "text": decoded_message, "customId": "0"}
        await websocket.send(json.dumps(chat_message))
        return web.json_response({"status": "success", "message": "Message sent"})
    else:
        return web.json_response({"status": "error", "message": "WebSocket connection not established"}, status=500)

async def handle_post(request):
    data = await request.json()
    global websocket
    if websocket:
        if "text" in data:
            # 处理换行符
            data["text"] = data["text"].replace('\\n', '\n')
        await websocket.send(json.dumps(data))
        return web.json_response({"status": "success", "message": "JSON message sent"})
    else:
        return web.json_response({"status": "error", "message": "WebSocket connection not established"}, status=500)

async def send_message(message):
    global websocket
    if websocket:
        # 处理换行符
        message = message.replace('\\n', '\n')
        chat_message = {"cmd": "chat", "text": message, "customId": "0"}
        await websocket.send(json.dumps(chat_message))
        return True
    return False

if __name__ == '__main__':
    if os.path.exists("user.txt"):
        with open("user.txt", "r", encoding="utf-8") as user_file:
            lines = user_file.readlines()
            for line in lines:
                if line.startswith("username:"):
                    nick = line.replace("username:", "").strip()
                elif line.startswith("password:"):
                    password = line.replace("password:", "").strip()
                elif line.startswith("channel:"):
                    channel = line.replace("channel:", "").strip()
                elif line.startswith("true_channel:"):
                    true_channel = line.replace("true_channel:", "").strip()
                elif line.startswith("trustedusers:"):
                    trustedusers = json.loads(line.replace("trustedusers:", "").strip())
                elif line.startswith("ws_link:"):
                    ws_link = line.replace("ws_link:", "").strip()
        if "ws_link" not in locals():
            ws_link = "wss://hack.chat/chat-ws" # 仍有bug

        if "true_channel" not in locals():
            true_channel = channel

        async def main():
            init_db()
            server_task = asyncio.create_task(start_server())
            join_task = asyncio.create_task(join_channel(nick, password, channel, ws_link))
            await join_task

        asyncio.run(main())
    else:
        print("错误：未找到 'user.txt' 文件。请确保该文件存在。")
