from .base import Command
from classes import Manager, Session, Permissions

class OpenDb(Command):
    name = "open_db"
    requires_login = True
    requires_db = False
    requires_coll = False
    permission = Permissions.READ

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")
        data_d = request.get("d")
        if not data_d:
            await session.error("format", data_id)
            return
        
        name = data_d.get("name")
        
        if session.db != None:
            if session.db.db_name == name:
                await session.error("already_opened", data_id)
                return
            
        if not manager.get_db(name):
            await session.error("doesnt_exist", data_id)
            return
        
        session.db = manager.get_db(name)
                                    
        await session.operation("ok", data_id=data_id)
        await manager.log(addr, "open", name)
