from .sqlite import connect
from .store import ensure_schema

def get_db():
    """
    Dependency to get a SQLite database connection.
    Yields the connection and ensures it is closed after the request.
    """
    conn = None
    try:
        conn = connect(check_same_thread=False)
        ensure_schema(conn)
        yield conn
    finally:
        if conn:
            conn.close()
