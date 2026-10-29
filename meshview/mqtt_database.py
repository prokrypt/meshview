import datetime
from meshview import models, recent
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

# The logger writes only to the RAM store (recent.py); packets.db is read here for duplicate checks
# and written by recent.move() every few hours.

def init_database(database_connection_string):
    global engine, async_session, disk_path
    kwargs = {}
    if not database_connection_string.startswith('sqlite'):
        kwargs['pool_size'] = 20
        kwargs['max_overflow'] = 50
    print (**kwargs)
    engine = create_async_engine(database_connection_string, echo=False, connect_args={"timeout": 15})
    disk_path = engine.url.database
    recent_engine = create_async_engine(f"sqlite+aiosqlite:///{recent.PATH}", echo=False, connect_args={"timeout": 15})

    # The RAM store is in /dev/shm: no point syncing it
    @event.listens_for(recent_engine.sync_engine, "connect")
    def _recent_pragmas(dbapi_conn, _record):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA synchronous=OFF")
        cur.close()

    async_session = async_sessionmaker(recent_engine, expire_on_commit=False)

async def create_tables():
    global _next_traceroute_id, _next_packet_id
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    _next_traceroute_id, _next_packet_id = recent.prepare_for_logger(disk_path)


def next_traceroute_id():
    """Traceroute ids continue from packets.db, so a row keeps its id when it is moved there."""
    global _next_traceroute_id
    _next_traceroute_id += 1
    return _next_traceroute_id - 1


def next_packet_id():
    """Packet ids continue from packets.db (and from 2**32), see models.Packet."""
    global _next_packet_id
    _next_packet_id += 1
    return _next_packet_id - 1


async def on_disk(sql, **params):
    """First row of a query on packets.db, or None."""
    async with engine.connect() as conn:
        return (await conn.execute(text(sql), params)).first()


async def packet_from_disk(from_node_id, mesh_id):
    """A copy of a packet that is already in packets.db, to put back in the RAM store, or None.

    Used when a sighting or traceroute reply arrives for a packet moved earlier, so the new row
    and its packet are in the same database.
    """
    row = await on_disk(f"SELECT {recent.COLUMNS['packet']} FROM packet WHERE from_node_id = :f AND mesh_id = :m",
                        f=from_node_id, m=mesh_id)
    if row is None:
        return None
    values = dict(row._mapping)
    if isinstance(values["import_time"], str):
        values["import_time"] = datetime.datetime.fromisoformat(values["import_time"])
    return models.Packet(**values)
