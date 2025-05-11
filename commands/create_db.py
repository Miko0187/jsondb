from .base import Command
from classes import Manager, Session, Permissions

class CreateDb(Command):
    name = "create_db"
    requires_login = True
    requires_db = False
    requires_coll = False
    permission = Permissions.CREATE_DB

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")
        data_d = request.get("d")
        if not data_d:
            await session.error("format", data_id)
            return

        name = data_d.get("name")     
        if manager.get_db(name):
            await session.error("exists", data_id)
            return

        manager.create_db(name)

        await session.operation("ok", data_id=data_id)
        manager.event_manager.emit("db_create", {
            "name": name
        })
        await manager.log(addr, f"create", name, None)