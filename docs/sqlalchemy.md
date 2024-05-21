### sqlalchemy封装

xwtools目前对mysql orm库sqlalchemy做了简单的封装，这个库质量上不如django rom，但使用上也算简洁。
非django项目推荐使用，以代替原生sql使用。

model的一个简单的定义如下：

    from sense_data import *
    from common import *
    from sqlalchemy import Column, Integer, String, DateTime


    class RiskStockIndex(sd.Base, sd.BaseModel):
        __tablename__ = 'graph_hosts'
        __label__ = DB_LABEL_STOCK
        __database__ = 'stock_db'

        id = Column('id', Integer, primary_key=True, autoincrement=True)
        host = Column('host', String(256))
        time = Column('time', DateTime)
        
gt.BaseModel 封装了常用的一些方法，比如：

    get_by_ids(cls, ids, session=None)
    get_all(cls, session=None)
    get_by_fields(cls, field, values, session=None)
    get_by_id(cls, id, session=None)
    get_by_field(cls, field, value, session=None)
    execute_sql(cls, session, *multiparams, **params)
    fetch_sql(cls, *multiparams, **params)
    save(cls, o, session=None)
    save_quietly(cls, o, session=None)
    bulk_save(cls, o, session=None)
    delete_all(cls)
    
在sqlalchemy里，每一次sql操作针对的连接是session，如果不关心多语句的事务处理，调用上述方法时忽略session参数即可，
相当于每次sql都是打开关闭一次session。如果需要自己管理session，gt.BaseModel 提供了：

    get_session(cls)
    close_session(cls, session)

上述方法中execute_sql可以执行sql，如果是查询，返回的就是一个原生的list结果。
