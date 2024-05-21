from .queue0 import *
from .config_log import log
from .etl_time import sleep

class RabbitConsumer2(RabbitConsumer0):

    def __init__(self, topic, label='rabbit', socket_timeout=300, heartbeat=30, config_info=None, no_ack=False,
                 retry_times=0, prefetch_count=1):
        super(RabbitConsumer2, self).__init__(topic, label, socket_timeout, heartbeat, config_info)
        self.no_ack = no_ack
        self.retry_times = retry_times
        self.prefetch_count = prefetch_count

    def callback(self, ch, method, properties, body):
        self._caller(body)
        if not self.no_ack:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def execute_once(self, caller, no_ack=False):
        self._caller = caller
        self.no_ack = no_ack
        flag = self._execute_once()
        self.close_connection()
        return flag

    def _execute_once(self):
        if not self._connection:
            self.check_connection()
            self._channel.queue_declare(queue=self._topic, durable=True)
            self._channel.basic_qos(prefetch_count=self.prefetch_count)
        mframe, hframe, body = self._channel.basic_get(queue=self._topic, no_ack=self.no_ack)
        if body is not None:
            retry_times = self.retry_times if self.retry_times > 0 else 10000000
            while retry_times >= 0:
                try:
                    self._caller(body.decode())
                    break
                except Exception as ex:
                    log.exception(ex)
                    sleep(2)
                    retry_times -= 1
            if not self.no_ack:
                self._channel.basic_ack(delivery_tag=mframe.delivery_tag)
        else:
            log.info("not get new queue message for {0}".format(self._topic))
            sleep(1)
        return body is not None

    def _consume_one(self):
        try:
            self._execute_once()
        except Exception as ex:
            log.info("rabbit except {0}".format(ex))
            self.close_connection()
            sleep(2)

    def consume_loop(self, caller):
        self._caller = caller
        while True:
            self._consume_one()
