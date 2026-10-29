#!/usr/bin/python3
"""Nightly meshview retention: delete rows older than KEEP_DAYS from packets.db.

Deletes in small chunks so the MQTT logger (15s busy timeout) never waits long
for the write lock. Freed pages are reused, so the file stops growing; no VACUUM.
Run from /etc/cron.d/meshview-cleanup.
"""
import sqlite3
import time

DB = "/root/meshview/packets.db"
KEEP_DAYS = 90
CHUNK = 5000

db = sqlite3.connect(DB, timeout=60)
cutoff = db.execute("SELECT datetime('now', 'localtime', ?)", (f"-{KEEP_DAYS} days",)).fetchone()[0]
start = time.monotonic()

for table in ("packet_seen", "traceroute", "packet"):
    total = 0
    while True:
        n = db.execute(
            f"DELETE FROM {table} WHERE rowid IN "
            f"(SELECT rowid FROM {table} WHERE import_time < ? LIMIT {CHUNK})",
            (cutoff,),
        ).rowcount
        db.commit()
        total += n
        if n < CHUNK:
            break
        time.sleep(0.1)
    print(f"{time.strftime('%F %T')} {table}: deleted {total} rows older than {cutoff}", flush=True)

db.execute("PRAGMA optimize")
db.close()
print(f"{time.strftime('%F %T')} done in {time.monotonic() - start:.0f}s", flush=True)
