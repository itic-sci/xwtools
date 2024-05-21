### RabbitMQ 封装

简单封装了producer和consumer的使用，示例如下：

生产者：

    def test_rabbit_produce():
        producer = gt.RabbitProducer('rabbit_label')
        for i in range(1, 100):
            producer.send('topic_name', 'helloo=%d' % i)

producer 还有个 send_safely，如果send失败（比如连接不上）它不会抛异常，不重要的数据（比如日志）就可以使用send_safely。

消费者

    def consume_message(msg):
        gt.log_info(msg)
        time.sleep(2)

    def test_kafka_consumer():
        consumer = gt.RabbitConsumer('topic_name','rabbit_label')
        consumer.execute_safely(consume_message)
        
        
consumer的execute_safely 对连接异常进行了重试处理，但回调的consume_message如果抛异常不做处理，需要使用者进行处理。




