from .queue0 import *
from .config_log import log
from .etl_time import sleep
from pika.exceptions import *


class RabbitProducer(RabbitProducer0):

    def send_safely(self, topic, message, need_close=False, times=None, **kwargs):
        if times is None:
            times = 4
        while times >= 0:
            try:
                times -= 1
                self.send(topic, message, need_close, **kwargs)
                return True
            except Exception as ex:
                log.warning("send message fir {0} failed ex:{1}".format(topic, str(ex)))
                self.close_safely()
                sleep(1)
        return False

    def close_safely(self):
        try:
            self.close_connection()
        except Exception as ex:
            log.exception(ex)


class RabbitConsumer(RabbitConsumer0):

    def _close_safely(self):
        try:
            self.close_connection()
        except Exception as ex:
            log.exception(ex)

    def execute_safely(self, caller, prefetch_count=1, no_ack=False, exchange=None):
        while True:
            try:
                self.execute(caller, prefetch_count, no_ack, exchange)
            except (ConnectionClosed, ChannelClosed, ConnectionError) as ex:
                log.info("rabbit except {0}".format(ex))
                self.close_connection()
                sleep(2)
            except Exception as ex:
                log.exception(ex)
                self._close_safely()
                raise ex
