from .base import Command
from classes import Manager, Session

class EventUnsub(Command):
    name = "event_unsub"
    requires_login = True
    requires_db = False
    requires_coll = False

    async def run(addr: tuple, request: dict, session: Session, manager: Manager):
        data_id = request.get("id")
        data_d = request.get("d")
        if not data_d:
            await session.error("format", data_id)
            return
        
        events = data_d.get("events")
        if not events or type(events) != list:
            await session.error("format", data_id)
            return

        manager.event_manager.unsub(session, events)

        await session.operation("ok", data_id=data_id)
