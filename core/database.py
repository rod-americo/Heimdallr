import sqlite3
import config

def get_db():
    """
    Dependency to get a SQLite database connection.
    Yields the connection and ensures it is closed after the request.
    """
    conn = None
    try:
        conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
        # Allows accessing columns by name
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        if conn:
            conn.close()
