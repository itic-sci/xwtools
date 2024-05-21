
import xwtools as xt

def test_log_info():
    xt.init_log(file_path='./log', module='test')
    xt.log.info('logxxxxx')

    try:
        a = 1 / 0
    except Exception as e:
        xt.log.exception(e)


def test_config():
    print(xt.config('db_stock', 'dbms'))
    print(xt.config('database'))
    print(xt.config('log_level'))


def test_db():
    db = xt.MysqlOp(label='msyql', db='test')
    df = db.get_df_sql("sql")
    query_list = db.get_dict_sql("sql")
    db.to_sql_write_data(df, 'table_name')
    db.execute_many_write_data(df, 'table_name')
    db.operate_sql("sql")


    db = xt.SqlServerOp()


    db = xt.RedisOp(label='', db=2)
    db.set()
    db.redis_hget()


    db =xt.MongoOp()

    db = xt.Neo4jOp()

    db = xt.HiveOp()

    db = xt.EsOp()

    esSearch = xt.BaseSearcher()
    total, query_list = esSearch.search_by_multi_bool()


def test_spider():
    spider_op = xt.SpiderOp()


def test_multi_thread():
    xt.multi_thread_map()

    xt.multi_process_map()


xt.init_log('./log', module='test_raise_exception')
@xt.catch_error_log
def test_raise_exception():
    return 1 / 0


def test_encrypt():
    key = 'sdai0000'
    sk = xt.Encryption.encrypt_key(key)
    print('\n')
    print('encrypt code: {}'.format(sk))
    code = xt.Encryption.decrypt_key(sk)
    print(code)
    print(code == key)


def test_encrypt_dict():
    from xwtools import Encryption
    config_map = {'user': 'sdai', 'pass': 'sddddd222', 'host': '192.168.1.1', 'port': '32'}
    est = Encryption.encryt_dict(config_map)
    print(est)
    st = Encryption.decrypt_dict(est)
    print(st)
    est['port'] = 32
    print(est)
    print(Encryption.decrypt_dict(est))

#
# def consume_message(msg):
#     print(msg)
#     print('deal one')
#     # xt.log_info('consumer ' + str(os.getpid()) + ' msg=' + msg)
#     # time.sleep(2)
#
#
# def test_rabbit_produce():
#     producer = xt.RabbitProducer()
#     for i in range(1, 10):
#         producer.send('test1', 'helloo=%d' % i)
#
#
# def test_kafka_consumer():
#     consumer = xt.RabbitConsumer(topic='test3',
#                                  config_info={'user': 'admin', 'password': 'sense_mq@2018', 'host': '52.82.30.135',
#                                               'port': 5672})
#     consumer.execute(consume_message)
#
#
# def function_cal(body):
#     import json
#     msg = json.loads(body)
#     print(msg['msg_header']['msg_idx'])
#
#
# def test_rabbit_produce1():
#     producer = xt.RabbitProducer(label='rabbit')
#     producer.send('test1', 'helloo=%d' % 1)
#
#
# def test_rabbit_consume():
#     consumer = xt.RabbitConsumer('test1', label='rabbit')
#     consumer.execute_safely(rabbit_consume, prefetch_count=1, no_ack=False)
#
#
# def rabbit_consume(body):
#     print(body)
#
#
# def function_test():
#     consumer = xt.RabbitConsumer('cloud_data_pdf_test', 'rabbit_cloud')
#     consumer.execute_safely(function_cal)

#
# def test_mq_fanout_produce():
#     from xwtools import RabbitProducer
#     import json
#     produce = RabbitProducer('rabbit_88')
#     for _k in range(1, 20):
#         st = {'index': _k}
#         produce.send_safely('alg_test_exc', json.dumps(st), exchange_type='fanout')
#         print('send one {} sleep 10s'.format(_k))
#
#
# def test_mq_fanout_consumer():
#     def handler(body):
#         print(body)
#         st = xt.load_json(body)
#         print(st)
#
#     consumer = xt.RabbitConsumer(topic='alg_dealed_result2', label='rabbit_88', heartbeat=300)
#     consumer.execute_safely(handler, exchange='alg_test_exc')
#
#
# def test_wechat():
#     from xwtools import WechatFactory
#     wechat = WechatFactory('wechat')
#     wechat.send_message('111', [])
#
#
# def test_email():
#     from xwtools import EmailFactory
#     email = EmailFactory('email')
#     email.send_message('瞅你咋地', '你瞅啥')


if __name__ == '__main__':
    pass
    test_log_info()
    # test_config()
    # test_raise_exception()
