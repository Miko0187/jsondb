import json
import zstd
import struct
import asyncio
from .db import Database

HEADER_SIZE = 4 # Bytes

class Session:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

        self.authed = False
        self.zstd = False
        self.db: Database = None

    async def send(self, body: str):
        encoded = body.encode()
        if self.zstd:
            encoded = zstd.compress(encoded, 1)
        header = struct.pack(">I", len(encoded))

        body = header + encoded
        self.writer.write(body)
        await self.writer.drain()

    async def read(self):
        try:
            header = await self.reader.readexactly(HEADER_SIZE)
        except asyncio.IncompleteReadError:
            return None
        
        msg_len = struct.unpack(">I", header)[0]
        data = await self.reader.readexactly(msg_len)
        if self.zstd:
            data = zstd.uncompress(data)
        return data.decode()

    async def error(self, details: str, data_id = None):
        resp = {
            "error": details
        }
        if data_id != None:
            resp["id"] = data_id

        await self.send(json.dumps(resp))
        
    async def operation(self, op: str, d: dict = None, data_id: str = None):
        resp = {
            "op": op
        }
        if data_id != None:
            resp["id"] = data_id
        if d != None:
            resp["d"] = d
            
        await self.send(json.dumps(resp))
