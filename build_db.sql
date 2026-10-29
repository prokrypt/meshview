-- Builds packets.db.new from packets.db: last 90 days only, compacted, indexed, WAL.
-- Run with services stopped: sqlite3 packets.db.new < build_db.sql   (CUTOFF replaced by the runner)
.bail on
.timer on
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
PRAGMA cache_size=-3000000;
ATTACH 'packets.db' AS old;

CREATE TABLE node (
	id VARCHAR NOT NULL,
	node_id BIGINT,
	long_name VARCHAR,
	short_name VARCHAR,
	hw_model VARCHAR,
	firmware VARCHAR,
	role VARCHAR,
	last_lat BIGINT,
	last_long BIGINT,
	channel VARCHAR,
	last_update DATETIME,
	PRIMARY KEY (id),
	UNIQUE (node_id)
);
CREATE TABLE packet (
	id BIGINT NOT NULL,
	portnum INTEGER,
	from_node_id BIGINT,
	to_node_id BIGINT,
	payload BLOB,
	import_time DATETIME,
	channel VARCHAR,
	PRIMARY KEY (id)
);
CREATE TABLE packet_seen (
	packet_id BIGINT NOT NULL,
	node_id BIGINT NOT NULL,
	rx_time BIGINT NOT NULL,
	hop_limit INTEGER,
	hop_start INTEGER,
	channel VARCHAR,
	rx_snr FLOAT,
	rx_rssi INTEGER,
	topic VARCHAR,
	import_time DATETIME,
	PRIMARY KEY (packet_id, node_id, rx_time),
	FOREIGN KEY(packet_id) REFERENCES packet (id)
);
CREATE TABLE traceroute (
	id INTEGER NOT NULL,
	packet_id BIGINT,
	gateway_node_id BIGINT,
	done BOOLEAN,
	route BLOB,
	import_time DATETIME,
	PRIMARY KEY (id),
	FOREIGN KEY(packet_id) REFERENCES packet (id)
);

INSERT INTO main.node SELECT * FROM old.node;
INSERT INTO main.packet SELECT * FROM old.packet WHERE import_time >= 'CUTOFF';
INSERT INTO main.packet_seen SELECT * FROM old.packet_seen WHERE import_time >= 'CUTOFF';
INSERT INTO main.traceroute SELECT * FROM old.traceroute WHERE import_time >= 'CUTOFF';
DETACH old;

CREATE INDEX idx_packet_from_node_portnum_time ON packet (from_node_id, portnum, import_time);
CREATE INDEX idx_packet_to_node_id ON packet (to_node_id);
CREATE INDEX idx_packet_import_time ON packet (import_time);
CREATE INDEX idx_packet_portnum_time ON packet (portnum, import_time);
CREATE INDEX idx_packet_seen_node_id ON packet_seen (node_id);
CREATE INDEX idx_packet_seen_import_time ON packet_seen (import_time);
CREATE INDEX idx_traceroute_packet_id ON traceroute (packet_id);
CREATE INDEX idx_traceroute_import_time ON traceroute (import_time);

PRAGMA analysis_limit=1000;
ANALYZE;
PRAGMA journal_mode=WAL;
SELECT 'node', count(*) FROM node;
SELECT 'packet', count(*), min(import_time), max(import_time) FROM packet;
SELECT 'packet_seen', max(rowid) FROM packet_seen;
SELECT 'traceroute', count(*) FROM traceroute;
