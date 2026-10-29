from meshview import models, recent
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession



engine = None
async_session = None


def init_database(database_connection_string, read_only=False):
    global engine, async_session

    kwargs = {"echo": False}

    if database_connection_string.startswith("sqlite"):
        if read_only:
            # Ensure SQLite is opened in read-only mode
            database_connection_string += "?mode=ro"
            kwargs["connect_args"] = {"uri": True}
        else:
            kwargs["connect_args"] = {"timeout": 15}
    else:
        kwargs["pool_size"] = 20
        kwargs["max_overflow"] = 50

    print("Database connection settings:", kwargs)  # Debugging output

    engine = create_async_engine(database_connection_string, **kwargs)
    if database_connection_string.startswith("sqlite"):
        # Every connection also sees the logger's RAM store of the newest data (see recent.py)
        @event.listens_for(engine.sync_engine, "connect")
        def _attach_recent(dbapi_conn, _record):
            recent.attach_for_web(dbapi_conn)
    async_session = async_sessionmaker( bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
