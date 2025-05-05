import typing
import asyncio
from .session import Session
if typing.TYPE_CHECKING:
    from .manager import Manager

class EventManager:
    def __init__(self, manager: "Manager"):
        self.manager = manager

        self.subs: dict[Session, list[str]] = {}
        self.valid_events = [
            "db_create",
            "db_delete",
            "coll_create",
            "coll_delete",
            "doc_insert",
            "doc_update",
            "doc_delete"
        ]

    def sub(self, session: Session, events: list[str]):
        for event in events:
            if event.lower() not in self.valid_events:
                continue
            if event.lower() in self.subs[session]:
                continue

            self.subs[session].append(event.lower())

    def unsub(self, session: Session, events: list[str]):
        for event in events:
            if event.lower() not in self.valid_events:
                continue
            if event.lower() not in self.subs[session]:
                continue

            self.subs[session].remove(event.lower())

    def emit(self, event: str, data: dict):
        asyncio.create_task(self._emit(event, data))

    async def _emit(self, event: str, data: dict):
        if event not in self.valid_events:
            await self.manager.log(("127.0.0.1", 0000), f"Warning: event to emit \"{event}\" is not a valid event, skipping")
            return
        
        for client, events in self.subs.items():
            if event in events:
                await client.operation("event", {
                    "ev": event,
                    "d": data
                })
