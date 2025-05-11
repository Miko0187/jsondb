import os
import json
import dotenv
import signal
import asyncio
from classes import Manager, Session
from utils import setup_data
from commands import *

dotenv.load_dotenv(dotenv_path=".env")

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
    if not os.environ.get("SERVER_ADDRESS"):
        print("'SERVER_ADDRESS' is not set as an enviroment variable")
        return
    address = os.environ["SERVER_ADDRESS"]
    
    if not os.environ.get("SERVER_PORT"):
        print("'SERVER_PORT' is not set as an enviroment variable")
        return
    port = int(os.environ["SERVER_PORT"])
    
    if not os.environ.get("SAVE_DIR"):
        print("'SAVE_DIR' is not set as an enviroment variable")
        return
    save_dir = os.environ["SAVE_DIR"]
    
    if error := setup_data(save_dir):
        print(error)
        return

    global manager
    manager = Manager(save_dir)
    manager.init()

    if not manager.get_user("root"):
        print("Root user doesnt exists. Consider deleting 'files' and rerunning this program")
        return

    server = await asyncio.start_server(handle_client, address, port)
    
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
