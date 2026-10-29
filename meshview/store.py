import datetime
from sqlalchemy import select, func
from sqlalchemy.orm import lazyload
from meshview import database
from meshview.models import Packet, PacketSeen, Node, Traceroute
from sqlalchemy import text

# Packets, sightings and traceroutes not yet moved to packets.db live in the logger's RAM store
# (recent.py). Queries on those tables run on both databases and the results are merged here; a row
# being moved is briefly in both, so the second copy is dropped. Node data comes only from the RAM
# store (TEMP view "node"), which always holds the complete, current node table.
RECENT = {"schema_translate_map": {None: "recent"}}


class Rows(list):
    """A list with .first(), like the SQLAlchemy results callers used to get."""
    def first(self):
        return self[0] if self else None


def _when(row):
    return row.import_time or datetime.datetime.min


def _merge(main, new, key, sort=None, reverse=False, limit=None):
    keys = {key(r) for r in main}
    rows = Rows(main)
    rows.extend(r for r in new if key(r) not in keys)
    if sort:
        rows.sort(key=sort, reverse=reverse)
    return rows if limit is None else Rows(rows[:limit])


async def _both(session, q):
    """Run an ORM query on packets.db and on the RAM store."""
    main = (await session.execute(q)).scalars().all()
    new = (await session.execute(q, execution_options=RECENT)).scalars().all()
    return main, new


def _seen_key(ps):
    return ps.packet_id, ps.node_id, ps.rx_time


async def get_node(node_id):
    async with database.async_session() as session:
        result = await session.execute(select(Node).where(Node.node_id == node_id))
        return result.scalar_one_or_none()


async def get_fuzzy_nodes(query):
    async with database.async_session() as session:
        q = select(Node).where(
            Node.id.ilike(query + "%")
            | Node.long_name.ilike(query + "%")
            | Node.short_name.ilike(query + "%")
        )
        result = await session.execute(q)
        return result.scalars()


async def get_packets(node_id=None, portnum=None, since=None, limit=1000, before=None, after=None):
    async with database.async_session() as session:
        q = select(Packet)

        if node_id:
            q = q.where(
                (Packet.from_node_id == node_id) | (Packet.to_node_id == node_id)
            )
        if portnum:
            q = q.where(Packet.portnum == portnum)
        if since:
            q = q.where(Packet.import_time > (datetime.datetime.now() - since))
        if before:
            q = q.where(Packet.import_time < before)
        if after:
            q = q.where(Packet.import_time > after)
        q = q.order_by(Packet.import_time.desc())
        if limit is not None:
            q = q.limit(limit)

        main, new = await _both(session, q)
        return _merge(main, new, lambda p: p.id, _when, reverse=True, limit=limit)


async def get_packets_from(node_id=None, portnum=None, since=None, limit=500):
    async with database.async_session() as session:
        q = select(Packet)

        if node_id:
            q = q.where(
                Packet.from_node_id == node_id
            )
        if portnum:
            q = q.where(Packet.portnum == portnum)
        if since:
            q = q.where(Packet.import_time > (datetime.datetime.now() - since))
        main, new = await _both(session, q.limit(limit).order_by(Packet.import_time.desc()))
        return _merge(main, new, lambda p: p.id, _when, reverse=True, limit=limit)


async def get_packet(packet_id):
    async with database.async_session() as session:
        q = select(Packet).where(Packet.id == packet_id)
        packet = (await session.execute(q, execution_options=RECENT)).scalar_one_or_none()
        return packet or (await session.execute(q)).scalar_one_or_none()


async def get_uplinked_packets(node_id, portnum=None):
    async with database.async_session() as session:
        q = select(Packet).join(PacketSeen).where(PacketSeen.node_id == node_id).order_by(Packet.import_time.desc()).limit(500)
        if portnum:
            q = q.where(Packet.portnum == portnum)
        main, new = await _both(session, q)
        return _merge(main, new, lambda p: p.id, _when, reverse=True, limit=500)


async def get_packets_seen(packet_id):
    async with database.async_session() as session:
        main, new = await _both(session,
            select(PacketSeen)
            .where(PacketSeen.packet_id == packet_id)
            .order_by(PacketSeen.import_time.desc())
        )
        return _merge(main, new, _seen_key, _when, reverse=True)


async def has_packets(node_id, portnum):
    async with database.async_session() as session:
        q = select(Packet.id).where(Packet.from_node_id == node_id).limit(1)
        return bool((await session.execute(q, execution_options=RECENT)).scalar()
                    or (await session.execute(q)).scalar())


async def get_traceroute(packet_id):
    async with database.async_session() as session:
        main, new = await _both(session,
                select(Traceroute)
                .where(Traceroute.packet_id == packet_id)
                .order_by(Traceroute.import_time)
        )
        return _merge(main, new, lambda t: t.id, _when)


async def get_traceroutes_live(since, after_id=None, channel=None, limit=5000):
    """Traceroute rows newer than since (and than row id after_id), oldest first."""
    async with database.async_session() as session:
        q = select(Traceroute).where(Traceroute.import_time > (datetime.datetime.now() - since))
        if after_id is not None:
            q = q.where(Traceroute.id > after_id)
        if channel:
            q = q.join(Packet, Packet.id == Traceroute.packet_id).where(Packet.channel == channel)
        main, new = await _both(session, q.order_by(Traceroute.id).limit(limit))
        return _merge(main, new, lambda t: t.id, lambda t: t.id, limit=limit)


async def get_nodes_by_id(node_ids):
    async with database.async_session() as session:
        result = await session.execute(select(Node).where(Node.node_id.in_(list(node_ids))))
        return result.scalars().all()


async def get_traceroutes(since):
    async with database.async_session() as session:
        main, new = await _both(session,
                select(Traceroute)
                .join(Packet)
                .where(Traceroute.import_time > (datetime.datetime.now() - since))
                .order_by(Traceroute.import_time)
        )
        return _merge(main, new, lambda t: t.id, _when)


async def get_mqtt_neighbors(since):
    async with database.async_session() as session:
        # "+ 0" keeps SQLite from looping over every packet and looking up its sightings by
        # packet_id (24s for 24h); it walks the sightings by import_time instead (0.4s)
        q = (select(PacketSeen, Packet)
            .join(Packet, Packet.id == PacketSeen.packet_id + 0)
            .where(
                (PacketSeen.hop_limit == PacketSeen.hop_start)
                & (PacketSeen.hop_start != 0)
                & (PacketSeen.import_time > (datetime.datetime.now() - since))
            )
            .options(
                lazyload(Packet.from_node),
                lazyload(Packet.to_node),
            )
        )
        main = (await session.execute(q)).all()
        new = (await session.execute(q, execution_options=RECENT)).all()
        return _merge(main, new, lambda row: _seen_key(row[0]))


# We count the total amount of packages
# This is to be used by /stats in web.py
async def get_total_packet_count():
    async with database.async_session() as session:
        q = select(func.count(Packet.id))  # Use SQLAlchemy's func to count packets
        result = await session.execute(q)
        # plus packets in the RAM store that aren't on disk (yet)
        new = await session.execute(text(
            "SELECT count(*) FROM recent.packet r WHERE NOT EXISTS (SELECT 1 FROM main.packet m WHERE m.id = r.id)"))
        return result.scalar() + new.scalar()  # Return the total count of packets

# We count the total amount of nodes
async def get_total_node_count():
    async with database.async_session() as session:
        q = select(func.count(Node.id))  # Use SQLAlchemy's func to count nodes
        q = q.where(Node.last_update > datetime.datetime.now() - datetime.timedelta(days=1)) # Look for nodes with nodeinfo updates in the last 24 hours
        result = await session.execute(q)
        return result.scalar()  # Return the total count of nodes

# We count the total amount of seen packets
async def get_total_packet_seen_count():
    async with database.async_session() as session:
        q = select(func.count(PacketSeen.node_id))  # Use SQLAlchemy's func to count nodes
        result = await session.execute(q)
        new = await session.execute(q, execution_options=RECENT)
        return result.scalar() + new.scalar()  # Return the total count of seen packets


async def get_total_node_count_longfast() -> int:
    try:
        # Open an asynchronous session with the database
        async with database.async_session() as session:
            # Build the query to count nodes where channel == 'LongFast'
            q = select(func.count(Node.id))
            q = q.where(Node.last_update > datetime.datetime.now() - datetime.timedelta( days=1))  # Look for nodes with nodeinfo updates in the last 24 hours
            q = q.where(Node.channel == 'LongFast')  #

            # Execute the query asynchronously and fetch the result
            result = await session.execute(q)

            # Return the scalar value (the count of nodes)
            return result.scalar()
    except Exception as e:
        # Log or handle the exception if needed (optional, replace with logging if necessary)
        print(f"An error occurred: {e}")
        return 0  # Return 0 or an appropriate fallback value in case of an error


async def get_active_node_counts_by_channel():
    """[(channel, nodes)] for nodes with nodeinfo updates in the last 24 hours, biggest first."""
    async with database.async_session() as session:
        q = (select(Node.channel, func.count(Node.id))
             .where(Node.last_update > datetime.datetime.now() - datetime.timedelta(days=1))
             .group_by(Node.channel)
             .order_by(func.count(Node.id).desc()))
        return [(channel or "unknown", count) for channel, count in (await session.execute(q)).all()]


async def get_total_node_count_mediumslow() -> int:
    try:
        # Open an asynchronous session with the database
        async with database.async_session() as session:
            # Build the query to count nodes where channel == 'LongFast'
            q = select(func.count(Node.id))
            q = q.where(Node.last_update > datetime.datetime.now() - datetime.timedelta(
                days=1))  # Look for nodes with nodeinfo updates in the last 24 hours
            q = q.where(Node.channel == 'MediumSlow')  #
            # Execute the query asynchronously and fetch the result
            result = await session.execute(q)

            # Return the scalar value (the count of nodes)
            return result.scalar()
    except Exception as e:
        # Log or handle the exception if needed (optional, replace with logging if necessary)
        print(f"An error occurred: {e}")
        return 0  # Return 0 or an appropriate fallback value in case of an error


# Get Nodes for mediumslow only
# p.r.
async def get_nodes_mediumslow():
    async with database.async_session() as session:
        result = await session.execute(
                select(Node)
                .where(
                (Node.channel == "MediumSlow")
                )
        )

        return result.scalars()

async def get_top_traffic_nodes(hours=24):

#    async with database.async_session() as session:
#        result = await session.execute(text("""
#            SELECT 
#                n.node_id,
#                n.channel,
#                n.long_name,
#                n.role,
#                COUNT(p.id) AS packet_count
#            FROM 
#                packet p
#            JOIN 
#                node n
#            ON 
#                p.from_node_id = n.node_id
#            WHERE 
#                p.import_time >= DATETIME('now', '-1 day')
#            GROUP BY 
#                n.long_name, n.role
#            ORDER BY 
#                packet_count DESC
#            LIMIT 100;
#        """))
#
#    async with database.async_session() as session:
#        result = await session.execute(text("""
#SELECT
#    n.node_id, n.channel, n.long_name, n.role, COUNT(p.id) AS packet_count, ps.hop_start
#FROM packet p
#JOIN
#    node n
#    ON p.from_node_id = n.node_id
#LEFT JOIN packet_seen ps
#    ON p.id = ps.packet_id
#    AND ps.import_time = (
#        SELECT MIN(import_time) FROM packet_seen WHERE packet_id = p.id
#    )
#WHERE
#    p.import_time >= DATETIME('now', '-1 day')
#GROUP BY
#    n.node_id, n.channel, n.long_name, n.role, ps.hop_start
#ORDER BY
#    packet_count DESC
#LIMIT 500;
#        """))


    # Count per packet first, then per node: the planner then starts from the
    # recent packets (idx_packet_import_time) instead of walking every node's
    # whole history. "+p.id" stops it from scanning the packet primary key to
    # avoid the GROUP BY sort. hop_start is the highest seen for the node
    # (the old query took it from an arbitrary packet_seen row).
    # Run on packets.db and on the RAM store, then add up per node. A packet the logger copied
    # back into the RAM store (for a late sighting) is counted from disk only.
    sql = """
            SELECT
                n.node_id, n.channel, n.long_name, n.role,
                COUNT(*) AS packet_count, MAX(t.hop_start) AS hop_start,
                SUM(t.seen) AS seen_count
            FROM (
                SELECT
                    p.from_node_id,
                    MAX(ps.hop_start) AS hop_start,
                    COUNT(CASE WHEN ps.node_id != p.from_node_id AND ps.hop_limit > 1 THEN 1 END) AS seen
                FROM {db}.packet p
                LEFT JOIN {db}.packet_seen ps
                    ON ps.packet_id = p.id
                WHERE
                    p.import_time >= DATETIME('now', 'localtime', '-' || :hour || ' hour')
                    {only_new}
                GROUP BY
                    +p.id
            ) t
            JOIN node n
                ON n.node_id = t.from_node_id
            GROUP BY
                n.node_id
        """
    async with database.async_session() as session:
        main = await session.execute(text(sql.format(db="main", only_new="")), {"hour": hours})
        new = await session.execute(text(sql.format(
            db="recent", only_new="AND NOT EXISTS (SELECT 1 FROM main.packet m WHERE m.id = p.id)")), {"hour": hours})
        nodes = {}
        for row in list(main) + list(new):
            node_id, channel, long_name, role, packets, hop_start, seen = row
            if node_id in nodes:
                n = nodes[node_id]
                n[4] += packets
                hops = [h for h in (n[5], hop_start) if h is not None]
                n[5] = max(hops) if hops else None
                n[6] += seen
            else:
                nodes[node_id] = [node_id, channel, long_name, role, packets, hop_start, seen]
        return sorted((tuple(n) for n in nodes.values()), key=lambda n: n[4], reverse=True)[:500]

async def get_node_traffic(node_id: int, hours: int):
    try:
        async with database.async_session() as session:
            # On packets.db and on the RAM store (packets copied back for late sightings: disk only)
            sql = """
                    SELECT 
                        node.long_name, packet.portnum, 
                        COUNT(*) AS packet_count
                    FROM {db}.packet AS packet
                    JOIN node ON packet.from_node_id = node.node_id
                    WHERE node.node_id = :node_id
                    AND packet.import_time >= DATETIME('now', 'localtime', '-' || :hour || ' hour') 
                    {only_new}
                    GROUP BY packet.portnum
                """
            args = {"node_id": node_id, "hour": hours}
            main = await session.execute(text(sql.format(db="main", only_new="")), args)
            new = await session.execute(text(sql.format(
                db="recent", only_new="AND NOT EXISTS (SELECT 1 FROM main.packet m WHERE m.id = packet.id)")), args)
            counts = {}
            for long_name, portnum, packet_count in list(main) + list(new):
                if portnum in counts:
                    counts[portnum]["packet_count"] += packet_count
                else:
                    counts[portnum] = {"long_name": long_name, "portnum": portnum, "packet_count": packet_count}

            # Map the result to include node.long_name and packet data
            traffic_data = sorted(counts.values(), key=lambda t: t["packet_count"], reverse=True)

            return traffic_data

    except Exception as e:
        # Log the error or handle it as needed
        print(f"Error fetching node traffic: {str(e)}")
        return []



async def get_nodes(role=None, channel=None, hw_model=None):
    """
    Fetches nodes from the database based on optional filtering criteria.

    Parameters:
        role (str, optional): The role of the node (converted to uppercase for consistency).
        channel (str, optional): The communication channel associated with the node.
        hw_model (str, optional): The hardware model of the node.

    Returns:
        list: A list of Node objects that match the given criteria.
    """
    try:
        async with database.async_session() as session:
            #print(channel)  # Debugging output (consider replacing with logging)

            # Start with a base query selecting all nodes
            query = select(Node)

            # Apply filters based on provided parameters
            if role is not None:
                query = query.where(Node.role == role.upper())  # Ensure role is uppercase
            if channel is not None:
                query = query.where(Node.channel == channel)
            if hw_model is not None:
                query = query.where(Node.hw_model == hw_model)

            # Exclude nodes where last_update is an empty string
            query = query.where(Node.last_update != "")

            # Order results by long_name in ascending order
            query = query.order_by(Node.long_name.asc())

            # Execute the query and retrieve results
            result = await session.execute(query)
            nodes = result.scalars().all()
            return nodes  # Return the list of nodes

    except Exception as e:
        print("error reading DB")  # Consider using logging instead of print
        return []  # Return an empty list in case of failure

async def get_nodesc(role=None, channel=None, hw_model=None):
    try:
        async with database.async_session() as session:
            query = select(
                Node.id,
                Node.node_id,
                Node.long_name,
                Node.short_name,
                Node.hw_model,
                Node.firmware,
                Node.role,
                Node.last_lat,  # Keep raw BIGINT value
                Node.last_long, # Keep raw BIGINT value
                Node.channel,
                Node.last_update,
                func.count(Packet.id).label("packet_count")
            ).outerjoin(Packet, (Packet.from_node_id == Node.node_id) | (Packet.to_node_id == Node.node_id))

            if role is not None:
                query = query.where(Node.role == role.upper())
            if channel is not None:
                query = query.where(Node.channel == channel)
            if hw_model is not None:
                query = query.where(Node.hw_model == hw_model)

            query = query.where(Node.last_update != "").group_by(Node.id).order_by(Node.long_name.asc())

            result = await session.execute(query)
            nodes = [{
                "node_id": row.node_id,
                "long_name": row.long_name,
                "short_name": row.short_name,
                "role": row.role,
                "channel": row.channel,
                "hw_model": row.hw_model,
                "firmware": row.firmware,
                "last_lat": row.last_lat,  # Raw value
                "last_long": row.last_long,  # Raw value
                "last_update": row.last_update,
                "packet_count": row.packet_count
            } for row in result.all()]
            
            return nodes  

    except Exception as e:
        print("Error reading DB:", e)
        return []
