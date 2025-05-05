from .base import Command
from classes import Manager, Session

class CreateCollection(Command):
    name = "create_collection"
    requires_login = True
    requires_db = True
    requires_coll = False

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")    
        data_d = request.get("d")
        if not data_d:
            await session.error("format", data_id)
            return
        
        name = data_d.get("name")
        if session.db.create_collection(name):
            await session.operation("ok", data_id=data_id)
            manager.event_manager.emit("coll_create", {
                "name": name
            })

            await manager.log(addr, f"create", session.db.db_name, name)
        else:
            await session.error("exist", data_id)
            