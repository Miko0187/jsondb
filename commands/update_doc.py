from .base import Command
from classes import Manager, Session, Permissions

class UpdateDoc(Command):
    name = "update_doc"
    requires_login = True
    requires_db = True
    requires_coll = True
    permission = Permissions.WRITE

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")    
        data_d = request.get("d")
        coll = data_d.get("collection")
        if not coll:
            await session.error( "format", data_id)
            return
        
        if session.db.collection_exists(coll) == None:
            await session.error("doesnt_exist", data_id)
            return
        coll = session.db.get(coll)

        query = data_d.get("query")
        if not query:
            await session.error("format", data_id)
            return
        
        update = data_d.get("update")
        if not update:
            await session.error("format", data_id)
            return
        
        before, after = await coll.update(query, update)
        
        await session.operation("ok", data_id=data_id)
        manager.event_manager.emit("doc_update", {
            "before": before,
            "after": after
        })

        await manager.log(addr, f"update", session.db.db_name, coll.name)
