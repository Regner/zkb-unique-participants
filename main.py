

import os
import json
import pika
import logging


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# App Settings
RABBITMQ_SERVER = os.environ.get('RABBITMQ_SERVER', 'rabbitmq-alpha')

# RabbitMQ Setup
connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_SERVER))
channel = connection.channel()

channel.exchange_declare(exchange='regner', type='topic')
channel.queue_declare(queue='zkb-unique-participants', durable=True)
channel.queue_bind(exchange='regner', queue='zkb-unique-participants', routing_key='zkillboard.raw')
logger.info('Connected to RabbitMQ server...')


def callback(ch, method, properties, body):
    data = json.loads(body.decode())
    killmail = data['killmail']
    unique_ids = {
        'attackers': {},
        'victim': {},
    }

    if 'character' in killmail['victim'] :
        unique_ids['victim']['character'] = killmail['victim']['character']['id']
    
    if 'corporation' in killmail['victim']:
        unique_ids['victim']['corporation'] = killmail['victim']['corporation']['id']
    
    if 'alliance' in killmail['victim']:
        unique_ids['victim']['alliance'] = killmail['victim']['alliance']['id']

    attacker_chars = set()
    attacker_corps = set()
    attacker_allis = set()

    for attacker in killmail['attackers']:
        if 'character' in attacker :
            attacker_chars.add(attacker['character']['id'])
        
        if 'corporation' in attacker:
            attacker_corps.add(attacker['corporation']['id'])
        
        if 'alliance' in attacker:
            attacker_allis.add(attacker['alliance']['id'])

    unique_ids['attackers'] = {
        'characters': list(attacker_chars),
        'corporations': list(attacker_corps),
        'alliances': list(attacker_allis),
    }


    payload = {
        'ids': unique_ids,
        'zkb_data': data,
    }

    logging.info('Processed killmail with ID {}.'.format(killmail['killID']))

    channel.basic_publish(
        exchange='regner',
        routing_key='zkillboard.processed.unique_ids',
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode = 2,
        ),
    )
    ch.basic_ack(delivery_tag = method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue='zkb-unique-participants')
channel.start_consuming()
