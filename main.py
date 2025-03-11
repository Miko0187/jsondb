import os
import json
import asyncio
from datetime import datetime
from argon2 import PasswordHasher
from db import Database, remove_db
from utils import create_config, setup_data, get_user, db_exists


_db_cache: dict[str, Database] = {}
ph = PasswordHasher()
db_files: str = None

def log(addr: tuple[str, int], msg: str, db: str = None, collection: str = None):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"[{timestamp}] [{addr[0]}:{addr[1]}]{f'/{db}' if db != None else ''}{f'/{collection}' if collection != None else ''} {msg}")   

async def error(writer: asyncio.StreamWriter, details: str):
    writer.write(json.dumps({
        "error": details
    }).encode() + b"\n")
    await writer.drain()
    
async def operation(writer: asyncio.StreamWriter, op: str, d: dict = None):
    resp = {
        "op": op
    }
    
    if d != None:
        resp["d"] = d
        
    writer.write(json.dumps(resp).encode() + b"\n")
    await writer.drain()

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    is_authed = False
    db: Database = None
    
    log(addr, "opened")
    
    try:
        while True:
            if not is_authed:
                await operation(writer, "auth")
            
            try:
                data = await reader.readuntil(b"\n")
            except asyncio.IncompleteReadError:
                break
            
            if not data:
                break
            
            data = data.decode().strip()
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                await error(writer, "decoding")
                
                continue
            
            if not data.get("op"):
                await error(writer, "format")
                
                continue
            
            if is_authed:
                if data["op"] == "auth":
                    await error(writer, "already_authed")
                    
                    continue
                elif data["op"] == "open_db":
                    if not data.get("d"):
                        await error(writer, "format")
                        
                        continue
                    
                    name = data["d"].get("name")
                    
                    if db != None:
                        if db.db_name == name:
                            await error(writer, "already_opened")
                            
                            continue
                        
                    if not db_exists(db_files, name):
                        await error(writer, "doesnt_exist")
                        
                        continue
                    
                    if _db_cache.get(name):
                        db = _db_cache[name]
                    else:
                        db = Database(db_files, name)
                        _db_cache[name] = db
                                                
                    await operation(writer, "ok")
                    
                    log(addr, f"opened '{name}'")
                elif data["op"] == "create_db":
                    if not data.get("d"):
                        await error(writer, "format")
                        
                        continue
                    
                    name = data["d"].get("name")
                            
                    if db_exists(db_files, name):
                        await error(writer, "exists")
                        
                        continue
                    
                    os.mkdir(f"{db_files}/files/{name}")
                    
                    await operation(writer, "ok")
                    
                    log(addr, f"created db '{name}'")
                elif data["op"] == "list_db":
                    dbs = []
                    
                    for dir in os.scandir(f"{db_files}/files"):
                        if dir.is_dir():
                            dbs.append(dir.name)
                            
                    await operation(writer, "ok", {"result": dbs})
                elif data["op"] == "delete_db":
                    if not data.get("d"):
                        await error(writer, "format")
                        
                        continue
                    
                    name = data["d"].get("name")
                            
                    if not db_exists(db_files, name):
                        await error(writer, "doesnt_exist")
                        
                        continue
                    
                    os.rmdir(f"{db_files}/files/{name}")
                    
                    try:
                        del _db_cache[name]
                    except:
                        pass
                    
                    remove_db(name)
                    
                    await operation(writer, "ok")
                    
                    log(addr, f"removed db '{name}'")
                elif data["op"] == "create_collection":
                    if db == None:
                        await error(writer, "non_open")
                        
                        continue
                    
                    if not data.get("d"):
                        await error(writer, "format")
                        
                        continue
                    
                    name = data["d"].get("name")
                    
                    if db.create_collection(name):
                        await operation(writer, "ok")
                        
                        log(addr, f"created collection '{name}'", db.db_name)
                    else:
                        await error(writer, "doesnt_exist")
                        
                        continue
                elif data["op"] == "list_collections":
                    if db == None:
                        await error(writer, "non_open")
                        
                        continue
                    
                    await operation(writer, "ok", {"result": db.list_collections()})
                elif data["op"] == "delete_collection":
                    if db == None:
                        await error(writer, "non_open")
                        
                        continue
                    
                    if not data.get("d"):
                        await error(writer, "format")
                        
                        continue
                    
                    name = data["d"].get("name")
                    
                    if db.delete_collection(name):
                        await operation(writer, "ok")
                        
                        log(addr, f"deleted collection '{name}'", db.db_name)
                    else:
                        await error(writer, "doesnt_exist")
                        
                        continue
                elif data["op"] == "insert_doc":
                    if db == None:
                        await error(writer, "non_open")
                        
                        continue
                    
                    if not data.get("d"):
                        await error(writer, "format")
                        
                        continue
                    
                    coll = data["d"].get("collection")
                    
                    if not coll:
                        await error(writer, "format")
                        
                        continue
                    
                    if db.collection_exists(coll) == None:
                        await error(writer, "doesnt_exist")
                        
                        continue
                    
                    _data = data["d"].get("dict")
                    
                    if not _data:
                        await error(writer, "format")
                        
                        continue
                    
                    db.insert_doc(coll, _data)
                    
                    await operation(writer, "ok")
                    
                    log(addr, f"insert", db.db_name, coll)
                elif data["op"] == "find_one_doc":
                    if db == None:
                        await error(writer, "non_open")
                        
                        continue
                    
                    if not data.get("d"):
                        await error(writer, "format")
                        
                        continue
                    
                    coll = data["d"].get("collection")
                    
                    if not coll:
                        await error(writer, "format")
                        
                        continue
                    
                    if db.collection_exists(coll) == None:
                        await error(writer, "doesnt_exist")
                        
                        continue
                    
                    query = data["d"].get("query")
                    
                    if not query:
                        await error(writer, "format")
                        
                        continue
                    
                    await operation(writer, "ok", {"result": db.find_one(coll, query)})
                elif data["op"] == "find_all_doc":
                    if db == None:
                        await error(writer, "non_open")
                        
                        continue
                    
                    if not data.get("d"):
                        await error(writer, "format")
                        
                        continue
                    
                    coll = data["d"].get("collection")
                    
                    if not coll:
                        await error(writer, "format")
                        
                        continue
                    
                    if db.collection_exists(coll) == None:
                        await error(writer, "doesnt_exist")
                        
                        continue
                                            
                    query = data["d"].get("query")
                    
                    await operation(writer, "ok", {"result": db.find_all(coll, query)})
                elif data["op"] == "update_doc":
                    if db == None:
                        await error(writer, "non_open")
                        
                        continue
                    
                    if not data.get("d"):
                        await error(writer, "format")
                        
                        continue
                    
                    coll = data["d"].get("collection")
                    
                    if not coll:
                        await error(writer, "format")
                        
                        continue
                    
                    if db.collection_exists(coll) == None:
                        await error(writer, "doesnt_exist")
                        
                        continue
                                            
                    query = data["d"].get("query")
                    
                    if not query:
                        await error(writer, "format")
                        
                        continue
                    
                    update = data["d"].get("update")
                    
                    if not update:
                        await error(writer, "format")
                        
                        continue
                    
                    db.update(coll, query, update)
                    
                    await operation(writer, "ok")
                    
                    log(addr, f"update", db.db_name, coll)
                elif data["op"] == "delete_doc":
                    if db == None:
                        await error(writer, "non_open")
                        
                        continue
                    
                    if not data.get("d"):
                        await error(writer, "format")
                        
                        continue
                    
                    coll = data["d"].get("collection")
                    
                    if not coll:
                        await error(writer, "format")
                        
                        continue
                    
                    if db.collection_exists(coll) == None:
                        await error(writer, "doesnt_exist")
                        
                        continue
                                            
                    query = data["d"].get("query")
                    
                    if not query:
                        await error(writer, "format")
                        
                        continue
                    
                    db.delete(coll, query)
                    
                    await operation(writer, "ok")
                    
                    log(addr, f"delete", db.db_name, coll)
                else:
                    await error(writer, "unknown")
                    
                    continue
            else:
                if not data.get("d"):
                    await error(writer, "format")
                    
                    continue
                
                if data["op"] == "auth":
                    name = data["d"].get("name") or ""
                    password = data["d"].get("password") or ""
                    
                    user = get_user(db_files, name)
                    
                    if user == None:
                        await error(writer, "user")
                        
                        continue
                    
                    try:
                        ph.verify(user, password)
                    except:
                        await error(writer, "user")
                        
                        continue
                    
                    await operation(writer, "authed")
                    is_authed = True
                    
                    log(addr, "authed")
                else:
                    await error(writer, "unauthed")
                    
                    continue
                    
    except asyncio.CancelledError:
        pass
    
    log(addr, "closed")

async def main():
    try:
        with open("config.json", "r") as f:
            config: dict[str, any] = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        config = create_config()
    
    if not config.get("address"):
        print("'address' is missing from the config")
        return
    
    if not config.get("port"):
        print("'port' is missing from the config")
        return
    
    if not config.get("db_files"):
        print("'db_files' is missing from the config")
        return
    
    global db_files
    db_files = config["db_files"]
    
    setup_data(db_files)
    
    if not get_user(db_files, "root"):
        print("Root user doesnt exists. Consider deleting 'files'")
        return
    
    server = await asyncio.start_server(handle_client, config["address"], config["port"])
    
    addr = server.sockets[0].getsockname()
    print(f"Running on {addr[0]}:{addr[1]}")

    async with server:
        await server.serve_forever()
        
try:
    asyncio.run(main())
except:
    raise SystemExit
