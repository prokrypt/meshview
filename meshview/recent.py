"""RAM-side store for the newest data.

The logger (startdb.py) writes new packets, sightings, traceroutes and node updates into a small
SQLite file in /dev/shm and moves them into packets.db every few hours, so the big file on disk is
written in a few large batches instead of continuously. The file survives a logger restart; only a
reboot or power cut loses what hasn't been moved yet (a clean stop moves everything first).

- node: a full copy of packets.db's node table, kept current by the logger. The web reads nodes
  only from here (TEMP view "node"), so names and positions are always up to date.
- packet, packet_seen, traceroute: only rows not yet moved. A sighting or traceroute reply for a
  packet already on disk brings a copy of that packet back, so a row and its packet are always in
  the same database. store.py runs its queries on both databases and merges the results.
"""
import os
import sqlite3

PATH = os.environ.get("MESHVIEW_RECENT", "/dev/shm/meshview-recent.db")
FIRST_PACKET_ID = 2**32  # new packet ids start above every Meshtastic id (see models.Packet)

COLUMNS = {
    "node": "id, node_id, long_name, short_name, hw_model, firmware, role, last_lat, last_long, channel, last_update",
    "packet": "id, mesh_id, portnum, from_node_id, to_node_id, payload, import_time, channel",
    "packet_seen": "packet_id, node_id, rx_time, hop_limit, hop_start, channel, rx_snr, rx_rssi, topic, import_time",
    "traceroute": "id, packet_id, gateway_node_id, done, route, import_time",
}

# Same tables as packets.db (models.py), plus the indexes the web's queries use
SCHEMA = """
CREATE TABLE IF NOT EXISTS {s}.node (
    id VARCHAR NOT NULL PRIMARY KEY, node_id BIGINT UNIQUE, long_name VARCHAR, short_name VARCHAR,
    hw_model VARCHAR, firmware VARCHAR, role VARCHAR, last_lat BIGINT, last_long BIGINT,
    channel VARCHAR, last_update DATETIME);
CREATE TABLE IF NOT EXISTS {s}.packet (
    id BIGINT NOT NULL PRIMARY KEY, portnum INTEGER, from_node_id BIGINT, to_node_id BIGINT,
    payload BLOB, import_time DATETIME, channel VARCHAR, mesh_id BIGINT);
CREATE TABLE IF NOT EXISTS {s}.packet_seen (
    packet_id BIGINT NOT NULL, node_id BIGINT NOT NULL, rx_time BIGINT NOT NULL, hop_limit INTEGER,
    hop_start INTEGER, channel VARCHAR, rx_snr FLOAT, rx_rssi INTEGER, topic VARCHAR, import_time DATETIME,
    PRIMARY KEY (packet_id, node_id, rx_time));
CREATE TABLE IF NOT EXISTS {s}.traceroute (
    id INTEGER NOT NULL PRIMARY KEY, packet_id BIGINT, gateway_node_id BIGINT, done BOOLEAN,
    route BLOB, import_time DATETIME);
CREATE UNIQUE INDEX IF NOT EXISTS {s}.idx_packet_from_node_mesh_id ON packet (from_node_id, mesh_id);
CREATE INDEX IF NOT EXISTS {s}.idx_packet_from_node_portnum_time ON packet (from_node_id, portnum, import_time);
CREATE INDEX IF NOT EXISTS {s}.idx_packet_to_node_id ON packet (to_node_id);
CREATE INDEX IF NOT EXISTS {s}.idx_packet_import_time ON packet (import_time);
CREATE INDEX IF NOT EXISTS {s}.idx_packet_portnum_time ON packet (portnum, import_time);
CREATE INDEX IF NOT EXISTS {s}.idx_packet_seen_node_id ON packet_seen (node_id);
CREATE INDEX IF NOT EXISTS {s}.idx_packet_seen_import_time ON packet_seen (import_time);
CREATE INDEX IF NOT EXISTS {s}.idx_traceroute_packet_id ON traceroute (packet_id);
CREATE INDEX IF NOT EXISTS {s}.idx_traceroute_import_time ON traceroute (import_time);
"""


def _create_schema(cur, schema):
    stmts = [s for s in SCHEMA.format(s=schema).split(";") if s.strip()]
    for stmt in stmts:
        if "CREATE TABLE" in stmt:
            cur.execute(stmt)
    # A RAM store from before mesh_id (2026-10-28): add the column; its rows have id = mesh_id
    cur.execute(f"PRAGMA {schema}.table_info(packet)")  # fetchall: the web's aiosqlite cursor returns None from execute
    if "mesh_id" not in [r[1] for r in cur.fetchall()]:
        try:
            cur.execute(f"ALTER TABLE {schema}.packet ADD COLUMN mesh_id BIGINT")
            cur.execute(f"UPDATE {schema}.packet SET mesh_id = id")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e):  # the web or the logger got there first
                raise
    for stmt in stmts:
        if "CREATE TABLE" not in stmt:
            cur.execute(stmt)


def _seed_nodes(cur, recent, disk):
    """Copy the node table into the RAM store if it is empty (first start after a reboot)."""
    cols = COLUMNS["node"]
    cur.execute(f"INSERT OR IGNORE INTO {recent}.node ({cols}) SELECT {cols} FROM {disk}.node "
                f"WHERE NOT EXISTS (SELECT 1 FROM {recent}.node)")


def attach_for_web(dbapi_conn):
    """Web connections (packets.db is main): attach the RAM store and read nodes from it."""
    cur = dbapi_conn.cursor()
    cur.execute(f"ATTACH DATABASE '{PATH}' AS recent")
    _create_schema(cur, "recent")
    _seed_nodes(cur, "recent", "main")
    dbapi_conn.commit()
    # Unqualified "node" now means recent.node (SQLite looks in temp first). A one-table view,
    # so queries joining it still use its indexes.
    cur.execute(f"CREATE TEMP VIEW IF NOT EXISTS node AS SELECT {COLUMNS['node']} FROM recent.node")
    cur.close()


def prepare_for_logger(disk_path):
    """Create the RAM store if needed and return the next free traceroute id and packet id.

    The logger hands out traceroute and packet ids itself, continuing from packets.db, so a row keeps
    its id when it is moved (the live page uses traceroute ids as its polling cursor).
    """
    con = sqlite3.connect(PATH, timeout=60)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("ATTACH DATABASE ? AS disk", (disk_path,))
    cur = con.cursor()
    _create_schema(cur, "main")
    _seed_nodes(cur, "main", "disk")
    cur.close()
    con.commit()
    if "mesh_id" not in [r[1] for r in con.execute("PRAGMA disk.table_info(packet)")]:
        raise RuntimeError(f"{disk_path} has no packet.mesh_id column yet: run migrate_mesh_id.sql on it first")
    next_id = con.execute("SELECT max(coalesce((SELECT max(id) FROM main.traceroute), 0), "
                          "coalesce((SELECT max(id) FROM disk.traceroute), 0)) + 1").fetchone()[0]
    next_packet = con.execute("SELECT max(?, coalesce((SELECT max(id) FROM main.packet), 0) + 1, "
                              "coalesce((SELECT max(id) FROM disk.packet), 0) + 1)", (FIRST_PACKET_ID,)).fetchone()[0]
    con.close()
    return next_id, next_packet


def oldest():
    """When the oldest data waiting in the RAM store arrived (import_time text), or None.

    Judged by sightings and traceroutes: every stored packet comes with a sighting, and a packet
    copied back for a late sighting keeps its old import_time.
    """
    con = sqlite3.connect(PATH, timeout=60)
    try:
        return con.execute("SELECT min(t) FROM (SELECT min(import_time) t FROM packet_seen "
                           "UNION ALL SELECT min(import_time) FROM traceroute)").fetchone()[0]
    finally:
        con.close()


def move(disk_path, cutoff):
    """Move packets imported before cutoff (a datetime) with their sightings and traceroutes into packets.db,
    plus nodes that differ from their row on disk. Returns the row counts moved.

    One transaction; rows are inserted with OR IGNORE, so a move cut short by a crash between the
    two databases' commits is simply repeated by the next one.
    """
    con = sqlite3.connect(disk_path, timeout=60, isolation_level=None)
    try:
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA wal_autocheckpoint=10000")
        con.execute("ATTACH DATABASE ? AS recent", (PATH,))
        con.execute("BEGIN IMMEDIATE")
        con.execute("CREATE TEMP TABLE mv (id INTEGER PRIMARY KEY)")
        # import_time is stored as text like "2026-10-23 20:40:00.123456"
        con.execute("INSERT INTO mv SELECT id FROM recent.packet WHERE import_time < ?",
                    (cutoff.isoformat(sep=" "),))
        moved = {}
        c = COLUMNS["packet"]
        moved["packet"] = con.execute(f"INSERT OR IGNORE INTO main.packet ({c}) SELECT {c} FROM recent.packet "
                                      f"WHERE id IN (SELECT id FROM mv) ORDER BY import_time").rowcount
        for table, order in (("packet_seen", "import_time"), ("traceroute", "id")):
            c = COLUMNS[table]
            # With their packet; rows whose packet isn't in the RAM store (shouldn't happen) go too,
            # so nothing can be left behind for good
            which = "packet_id IN (SELECT id FROM mv) OR packet_id NOT IN (SELECT id FROM recent.packet)"
            moved[table] = con.execute(f"INSERT OR IGNORE INTO main.{table} ({c}) SELECT {c} FROM recent.{table} "
                                       f"WHERE {which} ORDER BY {order}").rowcount
            con.execute(f"DELETE FROM recent.{table} WHERE {which}")
        con.execute("DELETE FROM recent.packet WHERE id IN (SELECT id FROM mv)")
        c = COLUMNS["node"]
        same = " AND ".join(f"m.{col} IS r.{col}" for col in c.split(", "))
        moved["node"] = con.execute(f"INSERT OR REPLACE INTO main.node ({c}) SELECT {c} FROM recent.node r "
                                    f"WHERE NOT EXISTS (SELECT 1 FROM main.node m WHERE {same})").rowcount
        con.execute("COMMIT")
        return moved
    finally:
        con.close()
