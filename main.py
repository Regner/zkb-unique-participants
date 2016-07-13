

import os
import asyncio
import logging

from nats.aio.client import Client


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# App Settings
NATS_SERVERS = os.environ.get('NATS_SERVERS', 'nats://127.0.0.1:4222')


async def run(loop):
    client = Client()
    servers = NATS_SERVERS.split(',')

    await client.connect(io_loop=loop, servers=servers)

    async def message_handler(msg):
        logger.info('Got message {}'.format(msg))

    sid = await client.subscribe('zkillboard.raw', cb=message_handler)


if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.run_forever()




# def find_entity_ids(entity):
#     ids = {}
#     ids['character'] = entity['character']['id'] if character in entity
#     ids['corporation'] = entity['corporation']['id'] if 'corporation' in entity
#     ids['alliance'] = entity['alliance']['id'] if 'alliance' in entity

#     return ids


# def merge_ids(old_ids, new_ids):
#     if 'character' in new_ids:
#         old_ids['characters'].add(character_id)

#     if 'corporation' in new_ids:
#         old_ids['corporations'].add(corporation_id)

#     if 'alliance' in new_ids:
#         old_ids['alliances'].add(alliance_id)


# def find_all_unique_ids(killmail):
#     ids = {
#         'characters': set(),
#         'corporations': set(),
#         'alliances': set(),
#     }

#     new_ids = find_entity_ids(killmail['victim'])
#     ids = merge_ids(ids, new_ids)

#     for attacker in killmail['attackers']:
#         new_ids = find_entity_ids(attacker)
#         ids = merge_ids(ids, new_ids)

#     return ids


# @app.route('/', methods=['POST'])
# def message():
#     message = request.json['message']

#     killmail = json.loads(base64.b64decode(str(message['message']['data'])))
#     unique_ids = find_unique_ids(killmail)

#     PS_TOPIC.publish(json.dumps({
#         'ids': unique_ids,
#         'killmail': killmail,
#     })
    
#     return ('', 204)


