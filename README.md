# JsonDB

My own database created for my own needs.

## Features

- **JSON**: Non-sql database with json.
- **Performance**: Maybe.
- **Authentication**: More or less good? Gets reworked soon.
- **Actions**: CRUD operations and events.
- **Configuration**: Customizable look at the `config.json` (address, port, storage location (just let it as it is)). Gets changed to env variables soon.

## Todo

* [ ] Users with permissions
* [ ] Redis like cache?!
* [X] Docstrings
* [X] Zstd encoding

## Installation

1. **Prerequisites**: python 3.13.
2. Clone this repo.
Pre 3. Create venv optionally
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Start the database.
   ```bash
   python -OO main.py
   ```

## Configuration

Modify the `config.json`.

```json
{
    "address": "0.0.0.0",  // Server binding address
    "port": 8989,           // Server port
    "db_files": "."          // Directory to store database files, prob gets changed in future
}
```

## How to connect

1. Dont use the cli `lib/__main__.py`, its broken and outdated.
2. copy the lib folder to your folder.
```bash
cp -r ./lib x/y/z/jsondb
```
3. Look at the example. (fun fact: there is none)
