from .base import Command
from classes import Manager, Session

class DeleteDb(Command):
    name = "delete_db"
    requires_login = True
    requires_db = False
    requires_coll = False

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")
        data_d = request.get("d")
        if not data_d:
            await session.error("format", data_id)
            return
        
        name = data_d.get("name")
        if not manager.get_db(name):
            await session.error("doesnt_exist", data_id)
            return
        
        for other_session in manager.event_manager.subs.keys():
            if other_session.db.db_name == name and other_session != session:
                await session.error("active", data_id)
                return
        
        manager.delete_db(name)
        
        await session.operation("ok", data_id=data_id)
        manager.event_manager.emit("db_delete", {
            "name": name
        })
        await manager.log(addr, f"delete", name)