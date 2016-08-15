# Adapted from
# http://alesnosek.com/blog/2015/05/25/openstack-nova-notifications-subscriber/

import logging as log
import os
import sys

from kombu import BrokerConnection
from kombu import Exchange
from kombu import Queue
from kombu.mixins import ConsumerMixin

EXCHANGE_NAME = os.environ.get("OS_EXCHANGE", "nova")
ROUTING_KEY = os.environ.get("OS_ROUTING_KEY", "notifications.info")
QUEUE_NAME = "dump_queue"
BROKER_USER = os.environ.get("RABBIT_USERNAME", "guest")
BROKER_PASSWORD = os.environ.get("RABBIT_PASSWORD", "guest")
BROKER_HOST = os.environ.get("RABBIT_HOST", "localhost")
BROKER_PORT = os.environ.get("RABBIT_PORT", "5672")
BROKER_VHOST = os.environ.get("RABBIT_VHOST", "/")
BROKER_URI = "amqp://{0}:{1}@{2}:{3}/{4}".format(
    BROKER_USER, BROKER_PASSWORD, BROKER_HOST, BROKER_PORT, BROKER_VHOST)

log.basicConfig(stream=sys.stdout, level=log.DEBUG)


class NotificationsDump(ConsumerMixin):

    def __init__(self, connection):
        self.connection = connection
        return

    def get_consumers(self, consumer, channel):
        exchange = Exchange(EXCHANGE_NAME, type="topic", durable=False)
        queue = Queue(QUEUE_NAME, exchange, routing_key=ROUTING_KEY,
                      durable=False, auto_delete=True, no_ack=True)
        return [consumer(queue, callbacks=[self.on_message])]

    def on_message(self, body, message):
        log.info('Body: %r' % body)
        log.info('---------------')

if __name__ == "__main__":
    log.info("Connecting to broker {}".format(BROKER_URI))
    with BrokerConnection(BROKER_URI) as connection:
        NotificationsDump(connection).run()
