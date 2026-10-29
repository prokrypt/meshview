import asyncio
import argparse
import configparser
import datetime
import signal
import time
from meshview import mqtt_reader
from meshview import recent
from meshview import mqtt_database
from meshview import mqtt_store
import json


# New data goes into the RAM store (meshview/recent.py) and reaches packets.db in a few big moves
COMMIT_INTERVAL = 1       # seconds between commits to the RAM store (no disk writes)
MOVE_AFTER = 6 * 3600     # move into packets.db once the oldest waiting data is this old (seconds)
MOVE_KEEP = 600           # data younger than this stays in RAM (late sightings and traceroute replies)


async def load_database_from_mqtt(mqtt_server: str , mqtt_port: int, topic: list, mqtt_user: str | None = None, mqtt_passwd: str | None = None):
    # Envelopes go into one session on the RAM store, committed every COMMIT_INTERVAL. The lock
    # keeps commits and moves from running in the middle of an envelope.
    lock = asyncio.Lock()
    pending = 0
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    async with mqtt_database.async_session() as session:

        async def commit():
            nonlocal pending
            if pending:
                await session.commit()
                pending = 0

        async def move(cutoff):
            await commit()
            t = time.monotonic()
            moved = await asyncio.to_thread(recent.move, mqtt_database.disk_path, cutoff)
            print(f"{datetime.datetime.now():%F %T} moved to packets.db in {time.monotonic() - t:.1f}s: {moved}", flush=True)

        async def housekeeping():
            last_check = 0
            while True:
                await asyncio.sleep(COMMIT_INTERVAL)
                async with lock:
                    await commit()
                    if time.monotonic() - last_check < 60:
                        continue
                    last_check = time.monotonic()
                    oldest = await asyncio.to_thread(recent.oldest)
                    now = datetime.datetime.now()
                    if oldest and oldest < (now - datetime.timedelta(seconds=MOVE_AFTER)).isoformat(sep=" "):
                        await move(now - datetime.timedelta(seconds=MOVE_KEEP))

        async def consume():
            nonlocal pending
            async for msg_topic, env in mqtt_reader.get_topic_envelopes(mqtt_server, mqtt_port, topic, mqtt_user, mqtt_passwd):
                async with lock:
                    await mqtt_store.process_envelope(msg_topic, env, session)
                    pending += 1

        tasks = [asyncio.create_task(housekeeping()), asyncio.create_task(consume())]
        stopped = asyncio.create_task(stop.wait())
        done, _ = await asyncio.wait(tasks + [stopped], return_when=asyncio.FIRST_COMPLETED)
        async with lock:
            for task in tasks:
                task.cancel()
            if stop.is_set():
                # Clean stop (systemctl stop, reboot, pkill): move everything, so nothing is left
                # in RAM to lose
                await move(datetime.datetime.now() + datetime.timedelta(days=1))
        for task in done:
            if task is not stopped:
                task.result()  # a failed commit or store ends the logger, as before (data stays in RAM)


async def main(config):
    mqtt_database.init_database(config["database"]["connection_string"])

    await mqtt_database.create_tables()
    mqtt_user = None
    mqtt_passwd = None
    if config["mqtt"]["username"] != "":
        mqtt_user: str = config["mqtt"]["username"]
    if config["mqtt"]["password"] != "":
        mqtt_passwd: str = config["mqtt"]["password"]
    mqtt_topics = json.loads(config["mqtt"]["topics"])
   
    
    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            load_database_from_mqtt(config["mqtt"]["server"], int(config["mqtt"]["port"]), mqtt_topics, mqtt_user, mqtt_passwd)
        )
    

def load_config(file_path):
    """Load configuration from an INI-style text file."""
    config_parser = configparser.ConfigParser()
    config_parser.read(file_path)

    # Convert to a dictionary for easier access
    config = {section: dict(config_parser.items(section)) for section in config_parser.sections()}
    return config


if __name__ == '__main__':
    parser = argparse.ArgumentParser("meshview")
    parser.add_argument("--config", help="Path to the configuration file.", default='config.ini')
    args = parser.parse_args()

    config = load_config(args.config)

    asyncio.run(main(config))