import datetime
from sqlalchemy import select
from sqlalchemy import update
from meshtastic.protobuf.config_pb2 import Config
from meshtastic.protobuf.portnums_pb2 import PortNum
from meshtastic.protobuf.mesh_pb2 import User, HardwareModel
from meshview import mqtt_database
from meshview import decode_payload
from meshview.models import Packet, PacketSeen, Node, Traceroute


import re
def parse_gateway_node_id(gid: str | None) -> int | None:
    if not gid:
        return None
    m = re.search(r'([0-9A-Fa-f]+)$', gid)  # last run of hex chars
    if not m:
        return None
    try:
        return int(m.group(1), 16)
    except ValueError:
        return None

async def find_packet(session, from_node_id, mesh_id):
    """The stored packet that from_node_id sent with Meshtastic id mesh_id, or None.

    Looks in the RAM store, then in packets.db (copying the packet back into the RAM store).
    """
    result = await session.execute(
        select(Packet).where(Packet.from_node_id == from_node_id, Packet.mesh_id == mesh_id))
    packet = result.scalar_one_or_none()
    if not packet:
        packet = await mqtt_database.packet_from_disk(from_node_id, mesh_id)
        if packet:
            session.add(packet)
    return packet


async def process_envelope(topic, env, session):
    """Store one envelope in session (the RAM store, see recent.py); the caller commits (startdb.py).

    Duplicate checks look in packets.db too. A sighting or traceroute reply for a packet that was
    already moved to packets.db brings a copy of the packet back, so a row and its packet are
    always in the same database.
    """

    # Checking if the received packet is a MAP_REPORT
    # Update the node table with the firmware version
    if env.packet.decoded.portnum == PortNum.MAP_REPORT_APP:
        # Extract the node ID from the packet (renamed from 'id' to 'node_id' to avoid conflicts with Python's built-in id function)
        node_id = getattr(env.packet, "from")

        # Decode the MAP report payload to extract the firmware version
        map_report = decode_payload.decode_payload(PortNum.MAP_REPORT_APP, env.packet.decoded.payload)

        # Undecodable (e.g. encrypted) map reports come back as None; skip them
        if map_report is not None:
            # Construct an SQLAlchemy update statement
            stmt = (
                update(Node)
                .where(Node.node_id == node_id)  # Ensure correct column reference
                .values(firmware=map_report.firmware_version)  # Assign new firmware value
            )

            # Execute the update statement asynchronously
            await session.execute(stmt)

    # This ignores any packet that does not have a ID
    if not env.packet.id:
        return

    # Without a valid gateway nothing is stored (not even the packet)
    gw_node_id = parse_gateway_node_id(getattr(env, "gateway_id", None))
    if gw_node_id is None:
        return

    # Meshtastic packet ids are only unique per sender (see models.Packet)
    packet = await find_packet(session, getattr(env.packet, "from"), env.packet.id)
    if not packet:
        packet = Packet(
            id=mqtt_database.next_packet_id(),
            mesh_id=env.packet.id,
            portnum=env.packet.decoded.portnum,
            from_node_id=getattr(env.packet, "from"),
            to_node_id=env.packet.to,
            payload=env.packet.SerializeToString(),
            # p.r. Here seems to be where the packet is imported on the Database and import time is set.
            import_time=datetime.datetime.now(),
            channel=env.channel_id,
        )
        session.add(packet)

    result = await session.execute(
        select(PacketSeen).where(
            PacketSeen.packet_id == packet.id,
            PacketSeen.node_id == int(env.gateway_id[1:], 16),
            PacketSeen.rx_time == env.packet.rx_time,
        )
    )
    seen = None
    if not result.scalar_one_or_none() and not await mqtt_database.on_disk(
            "SELECT 1 FROM packet_seen WHERE packet_id = :p AND node_id = :n AND rx_time = :r",
            p=packet.id, n=int(env.gateway_id[1:], 16), r=env.packet.rx_time):
        seen = PacketSeen(
            packet_id=packet.id,
            node_id=int(env.gateway_id[1:], 16),
            channel=env.channel_id,
            rx_time=env.packet.rx_time,
            rx_snr=env.packet.rx_snr,
            rx_rssi=env.packet.rx_rssi,
            hop_limit=env.packet.hop_limit,
            hop_start=env.packet.hop_start,
            topic=topic,
            # p.r. Here seems to be where the packet is imported on the Database and import time is set.
            import_time=datetime.datetime.now(),
        )
        session.add(seen)



    if env.packet.decoded.portnum == PortNum.NODEINFO_APP:
        user = decode_payload.decode_payload(
            PortNum.NODEINFO_APP, env.packet.decoded.payload
        )
        if user:
            result = await session.execute(select(Node).where(Node.id == user.id))
            if user.id and user.id[0] == "!":
                try:
                    node_id = int(user.id[1:], 16)
                except ValueError:
                    node_id = None
                    pass
            else:
                node_id = None

            try:
                hw_model = HardwareModel.Name(user.hw_model)
            except ValueError:
                hw_model = "unknown"
            try:
                role = Config.DeviceConfig.Role.Name(user.role)
            except ValueError:
                role = "unknown"

            if node := result.scalar_one_or_none():
                if node.channel != env.channel_id:
                    print(f"{node.channel}: node [{node.short_name}] {node.long_name} has changed channel to {env.channel_id}")
                if node.short_name != user.short_name:
                    print(f"{env.channel_id}: node [{node.short_name}] {node.long_name} has changed shortname to {user.short_name}")
                if node.long_name != user.long_name:
                    print(f"{env.channel_id}: node [{node.short_name}] {node.long_name} has changed longname to {user.long_name}")
                if node.role != role:
                    print(f"{env.channel_id}: node [{node.short_name}] {node.long_name} has changed role from {node.role} to {role}")
                node.node_id = node_id
                node.short_name = user.short_name
                node.long_name = user.long_name
                node.hw_model = hw_model
                node.channel = env.channel_id
                node.role = role
                node.last_update = datetime.datetime.now()
            else:
                print(f"{env.channel_id}: new {hw_model} {role} node seen: [{user.short_name}] {user.long_name}")
                node = Node(
                    id=user.id,
                    node_id=node_id,
                    long_name=user.long_name,
                    short_name=user.short_name,
                    hw_model=hw_model,
                    role=role,
                    channel=env.channel_id,
                    last_update = datetime.datetime.now()
                )
                session.add(node)

    if env.packet.decoded.portnum == PortNum.POSITION_APP:
        position = decode_payload.decode_payload(
            PortNum.POSITION_APP, env.packet.decoded.payload
        )
        if position and position.latitude_i and position.longitude_i:
            from_node_id = getattr(env.packet, 'from')
            node = (await session.execute(select(Node).where(Node.node_id == from_node_id))).scalar_one_or_none()
            if node:
                node.last_lat = position.latitude_i
                node.last_long = position.longitude_i
                session.add(node)

    if env.packet.decoded.portnum == PortNum.TRACEROUTE_APP:
        packet_id = None
        if env.packet.decoded.want_response:
            packet_id = packet.id
        # A reply goes back to the requester, so the request is (reply's to, request_id)
        elif request := await find_packet(session, env.packet.to, env.packet.decoded.request_id):
            packet_id = request.id
        if packet_id is not None:
            session.add(Traceroute(
                id=mqtt_database.next_traceroute_id(),
                packet_id=packet_id,
                route=env.packet.decoded.payload,
                done=not env.packet.decoded.want_response,
                gateway_node_id=int(env.gateway_id[1:], 16),
                import_time=datetime.datetime.now(),
            ))

