# JsonDB

My own database created for my own needs.

## Features

- **JSON**: Non-sql database with json.
- **Performance**: Maybe.
- **Authentication**: More or less good? Gets reworked soon.
- **Actions**: CRUD operations and events.
- **Configuration**: Its configured with env variables, look at .env.example

## Todo

* [ ] Users with permissions
* [ ] Redis like cache?!
* [X] Docstrings
* [X] Zstd encoding
* [ ] Fix cli
+ [ ] Add examples

## Installation

1. **Prerequisites**: python 3.13.
2. Clone this repo.
Pre 3. Create venv optionally
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Start the database for the first time.
   ```bash
   ROOT_PASSWORD="worldhello" python main.py
   ```
5. Then you can just start it with the following.
   ```bash
   python main.py
   ```

## Configuration

Set either the env variables in the command line or create a .env.

Command line example
```bash
SERVER_ADDRESS="127.0.0.1" SERVER_PORT=8989 SAVE_DIR="." python main.py
```

Or
.env example

```bash
SERVER_ADDRESS="127.0.0.1"
SERVER_PORT=8989
SAVE_DIR="."
```

then

```bash
python main.py
```


## How to connect

1. Dont use the cli `lib/__main__.py`, its broken and outdated.
2. copy the lib folder to your folder.
```bash
cp -r ./lib x/y/z/jsondb
```
3. Look at the example. (fun fact: there is none)
