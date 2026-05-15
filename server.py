import os, json, sqlite3
from datetime import datetime, timedelta
from aiohttp import web

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
USE_PG = bool(DATABASE_URL and psycopg2)
DB_PATH = os.path.join(BASE_DIR, "chat.db")
clients = {}       # username -> websocket
profiles = {}      # username -> avatar
last_seen = {}

def now_time(): return datetime.now().strftime("%H:%M")
def now_full(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def is_pg(): return USE_PG

def get_conn():
    if USE_PG:
        return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def ph(sql):
    return sql.replace("?", "%s") if USE_PG else sql

def exec_sql(con, sql, params=()):
    cur = con.cursor()
    cur.execute(ph(sql), params)
    return cur

def add_col_sqlite(con, table, col_def):
    try: con.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
    except Exception: pass

def init_db():
    with get_conn() as con:
        exec_sql(con, """
        CREATE TABLE IF NOT EXISTS users(
            username TEXT PRIMARY KEY,
            avatar TEXT,
            last_seen TEXT
        )""")
        exec_sql(con, """
        CREATE TABLE IF NOT EXISTS groups(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            avatar TEXT,
            created_by TEXT,
            created_at TEXT
        )""")
        exec_sql(con, """
        CREATE TABLE IF NOT EXISTS group_members(
            group_id TEXT NOT NULL,
            username TEXT NOT NULL,
            PRIMARY KEY(group_id, username)
        )""")
        exec_sql(con, """
        CREATE TABLE IF NOT EXISTS messages(
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            sender TEXT NOT NULL,
            receiver TEXT NOT NULL,
            text TEXT,
            image TEXT,
            voice TEXT,
            file_name TEXT,
            file_data TEXT,
            file_type TEXT,
            reply_id TEXT,
            reply_text TEXT,
            reply_sender TEXT,
            avatar TEXT,
            time TEXT,
            status TEXT DEFAULT 'sent',
            edited INTEGER DEFAULT 0,
            deleted INTEGER DEFAULT 0,
            created_at TEXT
        )""")
        exec_sql(con, """
        CREATE TABLE IF NOT EXISTS stories(
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            media TEXT,
            media_type TEXT,
            text TEXT,
            created_at TEXT
        )""")
        if not USE_PG:
            for c in ["file_name TEXT", "file_data TEXT", "file_type TEXT", "reply_id TEXT", "reply_text TEXT", "reply_sender TEXT"]:
                add_col_sqlite(con, "messages", c)

def rowdict(r):
    d = dict(r)
    for k in ["edited", "deleted"]: d[k] = bool(d.get(k))
    return d

async def send_to(user, data):
    ws = clients.get(user)
    if ws:
        try: await ws.send_str(json.dumps(data))
        except Exception: pass

async def send_many(users, data):
    for u in set([x for x in users if x]): await send_to(u, data)

def get_group_members(con, gid):
    rows = exec_sql(con, "SELECT username FROM group_members WHERE group_id=?", (gid,)).fetchall()
    return [r["username"] for r in rows]

async def broadcast_users():
    with get_conn() as con:
        rows = exec_sql(con, "SELECT username, avatar, last_seen FROM users ORDER BY username").fetchall()
    users = []
    for r in rows:
        rd = dict(r)
        name = rd["username"]
        users.append({"username": name, "avatar": profiles.get(name) or rd.get("avatar") or "", "online": name in clients, "last_seen": "online" if name in clients else (last_seen.get(name) or rd.get("last_seen") or "")})
    for u in list(clients): await send_to(u, {"type":"users", "users":users})

async def send_user_groups(username):
    with get_conn() as con:
        rows = exec_sql(con, """
            SELECT g.* FROM groups g JOIN group_members m ON g.id=m.group_id
            WHERE m.username=? ORDER BY g.created_at DESC
        """, (username,)).fetchall()
    await send_to(username, {"type":"groups", "groups":[dict(r) for r in rows]})

async def send_stories(username):
    cutoff = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as con:
        rows = exec_sql(con, "SELECT * FROM stories WHERE created_at>=? ORDER BY created_at DESC", (cutoff,)).fetchall()
    await send_to(username, {"type":"stories", "stories":[dict(r) for r in rows]})

def static_file(name): return os.path.join(BASE_DIR, name)
async def index(request): return web.FileResponse(static_file("index.html"))
async def manifest(request): return web.FileResponse(static_file("manifest.json"))
async def sw(request): return web.FileResponse(static_file("sw.js"))
async def icon_svg(request): return web.FileResponse(static_file("icon.svg"))
async def icon_png(request):
    size = request.match_info.get("size")
    p = static_file(f"icon-{size}.png")
    return web.FileResponse(p if os.path.exists(p) else static_file("icon.svg"))

async def ws_handler(request):
    ws = web.WebSocketResponse(max_msg_size=60*1024*1024)
    await ws.prepare(request)
    username = None
    async for msg in ws:
        if msg.type != web.WSMsgType.TEXT: continue
        try: data = json.loads(msg.data)
        except Exception: continue
        typ = data.get("type")

        if typ == "join":
            username = (data.get("username") or "").strip()
            avatar = data.get("avatar") or ""
            if not username: continue
            clients[username] = ws; profiles[username] = avatar
            with get_conn() as con:
                exec_sql(con, "INSERT INTO users(username,avatar,last_seen) VALUES(?,?,?) ON CONFLICT(username) DO UPDATE SET avatar=excluded.avatar,last_seen=excluded.last_seen" if USE_PG else "INSERT OR REPLACE INTO users(username,avatar,last_seen) VALUES(?,?,?)", (username, avatar, "online"))
                rows = exec_sql(con, """
                    SELECT * FROM messages WHERE sender=? OR receiver=? OR receiver IN
                    (SELECT 'group:' || group_id FROM group_members WHERE username=?)
                    ORDER BY created_at ASC LIMIT 600
                """, (username, username, username)).fetchall()
            await send_to(username, {"type":"history", "messages":[rowdict(r) for r in rows]})
            await send_user_groups(username); await send_stories(username); await broadcast_users()

        elif typ in ["message","image","voice","file"]:
            sender, receiver, mid = data.get("sender"), data.get("receiver"), data.get("id")
            if not sender or not receiver or not mid: continue
            recips = [sender]
            if str(receiver).startswith("group:"):
                gid = receiver.split(":",1)[1]
                with get_conn() as con: recips += get_group_members(con, gid)
                status = "sent"
            else:
                recips.append(receiver); status = "delivered" if receiver in clients else "sent"
            record = {"type":typ,"id":mid,"sender":sender,"receiver":receiver,"text":data.get("text",""),"image":data.get("image",""),"voice":data.get("voice",""),"file_name":data.get("file_name",""),"file_data":data.get("file_data",""),"file_type":data.get("file_type",""),"reply_id":data.get("reply_id",""),"reply_text":data.get("reply_text",""),"reply_sender":data.get("reply_sender",""),"avatar":data.get("avatar",""),"time":now_time(),"status":status,"edited":False,"deleted":False,"created_at":now_full()}
            with get_conn() as con:
                exec_sql(con, """INSERT INTO messages(id,type,sender,receiver,text,image,voice,file_name,file_data,file_type,reply_id,reply_text,reply_sender,avatar,time,status,edited,deleted,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", tuple(record[k] if k not in ["edited","deleted"] else int(record[k]) for k in ["id","type","sender","receiver","text","image","voice","file_name","file_data","file_type","reply_id","reply_text","reply_sender","avatar","time","status","edited","deleted","created_at"]))
            await send_many(recips, record)

        elif typ == "create_group":
            gid, name, members = data.get("id"), (data.get("name") or "").strip(), data.get("members") or []
            creator = data.get("creator")
            if not gid or not name or not creator: continue
            members = list(set([creator] + [m for m in members if m]))
            with get_conn() as con:
                exec_sql(con, "INSERT INTO groups(id,name,avatar,created_by,created_at) VALUES(?,?,?,?,?)", (gid, name, data.get("avatar",""), creator, now_full()))
                for m in members: exec_sql(con, "INSERT INTO group_members(group_id,username) VALUES(?,?) ON CONFLICT DO NOTHING" if USE_PG else "INSERT OR IGNORE INTO group_members(group_id,username) VALUES(?,?)", (gid, m))
            for m in members: await send_user_groups(m)

        elif typ == "story":
            sid, user = data.get("id"), data.get("username")
            if not sid or not user: continue
            story = {"type":"story","id":sid,"username":user,"media":data.get("media",""),"media_type":data.get("media_type",""),"text":data.get("text",""),"created_at":now_full()}
            with get_conn() as con:
                exec_sql(con, "INSERT INTO stories(id,username,media,media_type,text,created_at) VALUES(?,?,?,?,?,?)", (story["id"],story["username"],story["media"],story["media_type"],story["text"],story["created_at"]))
            for u in list(clients): await send_stories(u)

        elif typ == "typing":
            if data.get("receiver","").startswith("group:"):
                with get_conn() as con: members = get_group_members(con, data["receiver"].split(":",1)[1])
                await send_many([m for m in members if m != data.get("sender")], data)
            else: await send_to(data.get("receiver"), data)

        elif typ == "read":
            mid, reader = data.get("id"), data.get("reader")
            with get_conn() as con:
                row = exec_sql(con, "SELECT sender,receiver FROM messages WHERE id=?", (mid,)).fetchone()
                if row and row["receiver"] == reader:
                    exec_sql(con, "UPDATE messages SET status='read' WHERE id=?", (mid,))
                    await send_many([row["sender"], row["receiver"]], {"type":"status","id":mid,"status":"read"})

        elif typ == "edit":
            mid, new_text, sender = data.get("id"), data.get("text",""), data.get("sender")
            with get_conn() as con:
                row = exec_sql(con, "SELECT sender,receiver FROM messages WHERE id=?", (mid,)).fetchone()
                if row and row["sender"] == sender:
                    exec_sql(con, "UPDATE messages SET text=?, edited=1 WHERE id=?", (new_text, mid))
                    recips = [row["sender"], row["receiver"]]
                    if str(row["receiver"]).startswith("group:"): recips = get_group_members(con, row["receiver"].split(":",1)[1])
                    await send_many(recips, {"type":"edit","id":mid,"text":new_text})

        elif typ == "delete":
            mid, sender = data.get("id"), data.get("sender")
            with get_conn() as con:
                row = exec_sql(con, "SELECT sender,receiver FROM messages WHERE id=?", (mid,)).fetchone()
                if row and row["sender"] == sender:
                    exec_sql(con, "UPDATE messages SET deleted=1,text='',image='',voice='',file_data='' WHERE id=?", (mid,))
                    recips = [row["sender"], row["receiver"]]
                    if str(row["receiver"]).startswith("group:"): recips = get_group_members(con, row["receiver"].split(":",1)[1])
                    await send_many(recips, {"type":"delete","id":mid})

        elif typ in ["call-offer","call-answer","ice-candidate","call-end"]:
            receiver = data.get("receiver")
            if receiver: await send_to(receiver, data)

    if username:
        clients.pop(username, None); seen = now_full(); last_seen[username] = seen
        with get_conn() as con: exec_sql(con, "UPDATE users SET last_seen=? WHERE username=?", (seen, username))
        await broadcast_users()
    return ws

init_db()
app = web.Application(client_max_size=60*1024*1024)
app.router.add_get("/", index); app.router.add_get("/ws", ws_handler)
app.router.add_get("/manifest.json", manifest); app.router.add_get("/sw.js", sw)
app.router.add_get("/icon.svg", icon_svg); app.router.add_get("/icon-{size}.png", icon_png)
web.run_app(app, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
