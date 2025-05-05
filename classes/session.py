import json
import asyncio
from .db import Database

class Session:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.authed = False
        self.db: Database = None

    async def error(self, details: str, data_id = None):
        resp = {
            "error": details
        }
        if data_id != None:
            resp["id"] = data_id

        self.writer.write(json.dumps(resp).encode() + b"\n\r\n\r")
        await self.writer.drain()
        
    async def operation(self, op: str, d: dict = None, data_id: str = None):
        resp = {
            "op": op
        }
        if data_id != None:
            resp["id"] = data_id
        if d != None:
            resp["d"] = d
            
        self.writer.write(json.dumps(resp).encode() + b"\n\r\n\r")
        await self.writer.drain()
