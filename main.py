

import os
import json
import asyncio
import logging

from nats.aio.client import Client


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# App Settings
NATS_SERVERS = os.environ.get('NATS_SERVERS', 'nats://127.0.0.1:4222')

def find_entity_ids(entity):
    ids = {}
    ids['character'] = entity['character']['id'] if 'character' in entity else None
    ids['corporation'] = entity['corporation']['id'] if 'corporation' in entity else None
    ids['alliance'] = entity['alliance']['id'] if 'alliance' in entity else None

    return ids


def merge_ids(old_ids, new_ids):
    old_ids['characters'].add(new_ids['character'])
    old_ids['corporations'].add(new_ids['corporation'])
    old_ids['alliances'].add(new_ids['alliance'])

    return old_ids


def find_all_unique_ids(killmail):
    ids = {
        'characters': set(),
        'corporations': set(),
        'alliances': set(),
    }

    new_ids = find_entity_ids(killmail['victim'])
    ids = merge_ids(ids, new_ids)

    for attacker in killmail['attackers']:
        new_ids = find_entity_ids(attacker)
        ids = merge_ids(ids, new_ids)

    ids['characters'].discard(None)
    ids['corporations'].discard(None)
    ids['alliances'].discard(None)

    converted_ids = {
        'characters': list(ids['characters']),
        'corporations': list(ids['corporations']),
        'alliances': list(ids['alliances']),
    }

    return converted_ids

async def run(loop):
    client = Client()
    servers = NATS_SERVERS.split(',')

    await client.connect(io_loop=loop, servers=servers)
    logger.info('Connected to NATS server...')


    async def message_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            killmail = data['killmail']

            unique_ids = find_all_unique_ids(killmail)
            logger.info(unique_ids)

            payload = {
                'ids': unique_ids,
                'zkb_data': data,
            }

            count = len(unique_ids['characters']) + len(unique_ids['corporations']) + len(unique_ids['alliances'])
            logging.info('Found {} unique IDs in killmail ID {}'.format(count, killmail['killID']))

            await client.publish('zkillboard.processed.unique_ids', str.encode(json.dumps(payload)))
        except Exception as e:
            logger.info('Got exception: {}'.format(e))

    await client.subscribe('zkillboard.raw', cb=message_handler)


if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.run_forever()
