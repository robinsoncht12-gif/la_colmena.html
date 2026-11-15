import os
import sqlite3
from contextlib import contextmanager
from typing import Generator, Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "LaColmena.db")


def connect_db(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Abre la base SQLite compartida y crea el archivo si aún no existe."""
    return sqlite3.connect(db_path or DB_PATH)


@contextmanager
def database_connection(
    db_path: Optional[str] = None,
) -> Generator[sqlite3.Connection, None, None]:
    connection = connect_db(db_path)
    try:
        with connection:
            yield connection
    finally:
        connection.close()
