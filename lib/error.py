class InvalidUser(Exception):
    def __init__(self) -> None:
        super().__init__("Wrong name or password")
        
class DatabaseAlreadyOpenException(Exception):
    def __init__(self, db: str) -> None:
        super().__init__(f"Database '{db}' is already open")
        
class DoesntExistException(Exception):
    def __init__(self, prefix: str, name: str) -> None:
        super().__init__(f"{prefix} '{name}' doesnt exist")
        
class ExistException(Exception):
    def __init__(self, prefix: str, name: str) -> None:
        super().__init__(f"{prefix} '{name}' already exist")
        
class FormatException(Exception):
    def __init__(self) -> None:
        super().__init__(f"Missing key for request")
        
class NoDbOpenException(Exception):
    def __init__(self) -> None:
        super().__init__(f"Open a database")

class ClientError(Exception):
    def __init__(self):
        super().__init__("Client error")
    