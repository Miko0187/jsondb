import asyncio
import argparse
import json
from connection import Connection
from error import (    
    DatabaseAlreadyOpenException,
    DoesntExistException,
    ExistException,
    FormatException,
    InvalidUser,
    NoDbOpenException
)

parser = argparse.ArgumentParser()
parser.add_argument("-ip", "--host", required=True, help="Server Host")
parser.add_argument("-p", "--port", required=True, type=int, help="Server Port")
parser.add_argument("-n", "--name", required=True, help="Username")
parser.add_argument("-pw", "--password", required=True, help="Password")

async def main():
    args = parser.parse_args()
    conn = Connection(args.host, args.port, args.name, args.password)
    
    await conn.connect()
    
    while True:
        raw = input(f"{args.name}@{args.host}:{args.port}{f'/{conn.database.name}' if conn.database else ''} > ")
        
        if not raw.strip():
            continue
        
        parts = raw.split()
        command = parts[0].lower()
        params = parts[1:] if len(parts) > 1 else []
        
        try:
            match command.lower():
                case "exit":
                    break
                case "help":
                    print("Available commands:")
                    print("  exit - Close the connection")
                    print("  list_db - List all databases")
                    print("  open_db <name> - Open a database")
                    print("  create_db <name> - Create a new database")
                    print("  delete_db <name> - Delete a database")
                    print("  list_collections - List collections in the current database")
                    print("  create_collection <name> - Create a new collection")
                    print("  delete_collection <name> - Delete a collection")
                    print("  insert_doc <collection> <json> - Insert a document into a collection")
                    print("  find_doc <collection> <json> - Find a single document")
                    #print("  find_all_doc <collection> <json> - Find all documents matching a query")
                    print("  update_doc <collection> <json_query> <json_update> - Update a document")
                    print("  delete_doc <collection> <json> - Delete a document")
                case "list_db":
                    print(", ".join(await conn.list_databases()))
                case "open_db":
                    if params:
                        await conn.open_database(params[0])
                case "create_db":
                    if params:
                        await conn.create_database(params[0])
                case "delete_db":
                    if params:
                        await conn.delete_database(params[0])
                case "list_collections":
                    db = conn.database
                    print(", ".join(await db.list_collections()))
                case "create_collection":
                    if params:
                        db = conn.database
                        await db.create_collection(params[0])
                case "delete_collection":
                    if params:
                        db = conn.database
                        await db.delete_collection(params[0])
                case "insert_doc":
                    if len(params) >= 2:
                        try:
                            doc = json.loads(params[1])
                            await conn.send({"op": "insert_doc", "d": {"collection": params[0], "dict": doc}})
                        except json.JSONDecodeError:
                            pass
                case "find_doc":
                    if len(params) >= 2:
                        db = conn.database
                        coll = await db.get_collection(params[0])
                        
                        try:
                            query = json.loads(params[1])
                            print(await coll.find_one(query))
                        except json.JSONDecodeError:
                            pass
                #case "find_all_doc":
                #    if len(params) >= 2:
                #        try:
                #            query = json.loads(params[1])
                #            await conn.send({"op": "find_all_doc", "d": {"collection": params[0], "query": query}})
                #        except json.JSONDecodeError:
                #            pass
                case "update_doc":
                    db = conn.database
                    coll = await db.get_collection(params[0])
                    
                    if len(params) >= 3:
                        try:
                            query = json.loads(params[1])
                            update = json.loads(params[2])
                            
                            await coll.update(query, update)
                        except json.JSONDecodeError:
                            pass
                case "delete_doc":
                    db = conn.database
                    coll = await db.get_collection(params[0])
                    
                    if len(params) >= 2:
                        try:
                            query = json.loads(params[1])
                            
                            await coll.delete(query)
                        except json.JSONDecodeError:
                            pass
                case _:
                    print("Unknown command. Type 'help' for a list of available commands.")
        except IndexError:
            print("Missing argument")
        except (
            DatabaseAlreadyOpenException,
            DoesntExistException,
            ExistException,
            FormatException,
            InvalidUser,
            NoDbOpenException
        ) as e:
            print(e)
    
    await conn.close()

if __name__ == "__main__":
    #try:
    asyncio.run(main())
    #except KeyboardInterrupt:
    #    raise SystemExit
