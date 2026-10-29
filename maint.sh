#!/bin/bash
# One-off meshview maintenance 2026-10-23: 90-day copy + indexes + WAL, map report fix, cron + logrotate.
set -eu
cd /root/meshview
log() { echo "$(date '+%F %T') $*"; }
fail() { log "FAILED at line $1, starting services on the untouched old database"; rm -f packets.db.new; systemctl start rc-local; exit 1; }
trap 'fail $LINENO' ERR

log "stopping rc-local"
systemctl stop rc-local
sleep 2
if pgrep -f 'env/bin/python -u (startdb|main).py' >/dev/null; then log "meshview still running"; false; fi
[ ! -e packets.db-journal ] && [ ! -e packets.db-wal ]

CUT=$(date -d '90 days ago' '+%F %T')
log "building packets.db.new, keeping rows from $CUT"
rm -f packets.db.new
sed "s/CUTOFF/$CUT/" build_db.sql | sqlite3 packets.db.new
[ "$(sqlite3 packets.db.new 'PRAGMA journal_mode')" = wal ]

log "swapping database files"
mv packets.db packets.db.before-retention
mv packets.db.new packets.db
trap - ERR

log "applying map report fix"
cp -p meshview/mqtt_store.py meshview/mqtt_store.py.before-mapreport-fix
cp mqtt_store.py.new meshview/mqtt_store.py
./env/bin/python -m py_compile meshview/mqtt_store.py

log "starting rc-local"
systemctl start rc-local

install -m 644 meshview-cleanup.cron /etc/cron.d/meshview-cleanup
install -m 644 meshview.logrotate /etc/logrotate.d/meshview
log "done"
ls -la packets.db*
df -h /
