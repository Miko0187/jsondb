from .base import Command
from classes import Manager, Session

class InsertDoc(Command):
    name = "insert_doc"
    requires_login = True
    requires_db = True
    requires_coll = True

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")    
        data_d = request.get("d")
        coll = data_d.get("collection")
        if session.db.collection_exists(coll) == None:
            await session.error("doesnt_exist", data_id)
            return
        coll = session.db.get(coll)

        _data = data_d.get("dict")
        if not _data:
            await session.error("format", data_id)
            return

        await coll.insert(_data)

        await session.operation("ok", data_id=data_id)
        manager.event_manager.emit("doc_insert", {
            "doc": _data
        })

        await manager.log(addr, f"insert", session.db.db_name, coll.name)
            