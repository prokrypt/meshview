-- 2026-10-28: packets are (from_node_id, mesh_id), see models.Packet. Run with the logger stopped:
--   sqlite3 packets.db < migrate_mesh_id.sql
-- Undo: DROP INDEX idx_packet_from_node_mesh_id; ALTER TABLE packet DROP COLUMN mesh_id;
.bail on
.timer on
PRAGMA busy_timeout=60000;
BEGIN IMMEDIATE;
ALTER TABLE packet ADD COLUMN mesh_id BIGINT;
UPDATE packet SET mesh_id = id;
CREATE UNIQUE INDEX idx_packet_from_node_mesh_id ON packet (from_node_id, mesh_id);
COMMIT;
SELECT 'packet', count(*), sum(mesh_id IS NULL), max(id) FROM packet;
