import os
import json
import signal
import asyncio
from classes import Manager, Session
from utils import create_config, setup_data
from commands import *

manager: Manager = None
commands: list[Command] = [
    Auth,
    CreateCollection,
    CreateDb,
    DeleteCollection,
    DeleteDb,
    DeleteDoc,
    EventSub,
    EventUnsub,
    FindAllDoc,
    FindOneDoc,
    InsertDoc,
    ListCollections,
    ListDb,
    OpenDb,
    UpdateDoc
]

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    session = Session(reader, writer)

    await manager.log(addr, "connected")
    
    try:
        while True:          
            data = await session.read()
            if not data:
                break
            data = data.strip()
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                await session.error(writer, "decoding")
                continue

            if not session.authed:
                if not manager.ratelimiter.is_allowed(addr[0]):
                    await manager.log(addr, "rate limited")
                    await asyncio.sleep(manager.ratelimiter.delay)

            cmd = data.get("op")
            if not cmd:
                await session.error("format", data.get("id"))
                continue

            for command in commands:
                if command.name == cmd:
                    if await command.check_requirements(session, manager, data):
                        await command.run(addr, data, session, manager)
                    break
            else:
                await session.error("unknown", data.get("id"))
    except asyncio.CancelledError:
        pass

    try:
       manager.event_manager.subs.pop(session) 
    except:
        pass
    
    await manager.log(addr, "closed")

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
    
    setup_data(config["db_files"])

    global manager
    manager = Manager(config["db_files"])
    manager.init()

    if not manager.get_user("root"):
        print("Root user doesnt exists. Consider deleting 'files' and rerunning this program")
        return

    server = await asyncio.start_server(handle_client, config["address"], config["port"])
    
    addr = server.sockets[0].getsockname()
    await manager.log(addr, f"Server started on {addr[0]}:{addr[1]}")

    try:
        async with server:
            await server.serve_forever()
    except asyncio.CancelledError:
        server.close()

    manager.stop()
        
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    main_task = asyncio.ensure_future(main())

    for signal in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(signal, main_task.cancel)

    try:
        loop.run_until_complete(main_task)
    finally:
        loop.close()
