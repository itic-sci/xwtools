import pika
from .config_log import config


class BaseRabbit(object):

    def __init__(self, label='rabbit', socket_timeout=300, heartbeat=30, config_info=None):
        if config_info:
            self._host = config_info['host']
            self._port = int(config_info['port'])
            self._user = config_info['user']
            password = config_info.get('password', '')
            if password == '':
                password = config_info.get('pass', '')
        else:
            self._host = config(label, 'host')
            self._port = int(config(label, 'port'))
            self._user = config(label, 'user')
            password = config(label, 'password', '')
            if password == '':
                password = config(label, 'pass', '')
        self._password = password
        self._connection = None
        self._channel = None
        self._caller = None
        self.socket_timeout = socket_timeout
        self.heartbeat = heartbeat
        self.no_ack = False

    def init_connection(self):
        credentials = pika.PlainCredentials(self._user, self._password)
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(self._host, self._port, '/', credentials, heartbeat=self.heartbeat,
                                      socket_timeout=self.socket_timeout))
        self._channel = self._connection.channel()

    def check_connection(self):
        if not self._connection:
            self.init_connection()

    def close_connection(self):
        if self._connection:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None


class RabbitProducer0(BaseRabbit):

    def __init__(self, label='rabbit', socket_timeout=300, heartbeat=30, config_info=None):
        super(RabbitProducer0, self).__init__(label, socket_timeout, heartbeat, config_info)

    def send(self, topic, message, need_close=True, **kwargs):
        self.check_connection()
        if kwargs.get('exchange_type', None):
            self._channel.exchange_declare(exchange=topic, exchange_type=kwargs['exchange_type'],
                                           durable=True)
            routing_key, exchange = kwargs.get('routing_key', ''), topic
        else:
            self._channel.queue_declare(queue=topic, durable=True)
            routing_key, exchange = topic, ''
        self._channel.basic_publish(exchange=exchange,
                                    routing_key=routing_key,
                                    body=message,
                                    properties=pika.BasicProperties(
                                        delivery_mode=2,  # 使得消息持久化
                                    ))
        if need_close:
            self.close_connection()


class RabbitConsumer0(BaseRabbit):

    def __init__(self, topic, label='rabbit', socket_timeout=300, heartbeat=30, config_info=None):
        self._topic = topic
        super(RabbitConsumer0, self).__init__(label, socket_timeout, heartbeat, config_info)

    def message_count(self):
        self.check_connection()
        queue = self._channel.queue_declare(queue=self._topic, durable=True)
        count = queue.method.message_count
        self.close_connection()
        return count

    def callback(self, ch, method, properties, body):
        self._caller(body)
        if not self.no_ack:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def execute_once(self, caller, no_ack=False):
        self._caller = caller
        self.check_connection()
        self._channel.queue_declare(queue=self._topic, durable=True)
        mframe, hframe, body = self._channel.basic_get(queue=self._topic, no_ack=no_ack)
        if body is not None:
            caller(body.decode())
            if not no_ack:
                self._channel.basic_ack(delivery_tag=mframe.delivery_tag)
        self.close_connection()
        return body is not None

    def execute(self, caller, prefetch_count=1, no_ack=False, exchange=None):
        self._caller = caller
        self.no_ack = no_ack
        self.check_connection()
        if exchange:
            self._channel.exchange_declare(exchange=exchange,
                                           exchange_type='fanout', durable=True)
        else:
            self._channel.queue_declare(queue=self._topic, durable=True)
        self._channel.basic_qos(prefetch_count=prefetch_count)
        self._channel.basic_consume(self.callback,
                                    queue=self._topic,
                                    no_ack=no_ack)
        self._channel.start_consuming()
