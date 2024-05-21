### 日志打印和收集

日志调用的方法有：

    log_init_config(module='unknown', root_path='.', monit_queue='')
    log_debug(msg)
    log_info(msg)
    log_warn(msg)
    raise_exception(msg)
    log_exception(ex)
    log_error(msg)
    log_notice(msg, module='')
    log_task_schedule(task_name, period, module='', exclude_time=None)
    
基于xwtools的项目需要在入口调用log_init_config来初始化日志和配置，因为库是被引用的，但是配置是在项目根目录，
所以必须项目开始就要初始化解析配置，否则就可能找不到配置文件。

基于django的项目，需要在settings.py中配置示例如下：

    LOGGING_CONFIG = None
    log_init_config('stock', config('log_path'))
    
非django的项目，为避免多个入口指定初始化，可以将初始化代码做个封装，比如：

    __is_init = False

    def init_config():
        global __is_init
        if __is_init:
            return
        gt.log_init_config('graph', gt.config('log_path'), 'rabbit_monit')
        gt.set_log_thread_name(True)
        __is_init = True

在某个"__init__.py"文件里放置init_config(),后续的执行脚本入口确保先执行这个"__init__.py"就可以了。


log_init_config的参数说明如下：

    module：项目模块名，各项目唯一，用于日志收集和监控使用。
    root_path：日志文件所在目录，xwtools会根据级别在目录里生成debug.log,info.log,warn.log,error.log。对于web项目，info.log需要做日志收集用于统计。error.log需要用于监控。
    monit_queue：默认可以不用配置，但如果使用log_notice、log_task_schedule需要配置，它是监控队列的配置label。
    
日志相关的配置通常配置在settings.ini,本地开发环境配置示例：
    
    [settings]
    log_path = /tmp
    log_level = debug
    debug = 1
    
这样开发时配置是打到临时目录/tmp/info.log，避免指定固定目录时可能因为脚本入口层次问题造成配置文件目录不存在的情况。

如果希望控制台看日志，可以把log_path改成console就行了。开发时debug=1可以让log_debug()的日志显示出来。

生产环境配置示例如下：

    [settings]
    log_path = /data/app/stock/logs
    debug = 0
    
生产日志存放路径统一到/data/app/module_name/logs下，docker启动脚本做好映射。

python日志库存在一个缺陷，就是高级别的日志会打到低级别日志文件里，造成info里有error日志，目前收集时做些过滤操作。

另外，为方便日志查看和控制日志文件大小，可以采用logrotate进行日志切割，logrotate.conf示例如下：

    /data/app/stock/logs/*.log {
        daily
        missingok
        rotate 7
        compress
        delaycompress
        notifempty
        copytruncate
    }
 
重点调用说明：

log_exception 传入exception会打出异常栈，raise_exception 内部调用log_exception 后抛出异常。

log_exception 和 log_error 会把日志打到error.log，如果配置logstash收集了日志，这些错误日志会被监控程序
收集到数据库，可以在监控后台查看。对于logstash的配置使用，可以联系李军解决。

监控后台的测试地址：http://52.82.48.248:4002/ 生产地址：http://sensedeal.wiki:4002/

为了出现错误日志能第一时间报警，需要在后台的日志管理-服务管理里配置模块信息。

log_notice 用于把调用信息发送到监控队列后在监控后台展示，通常用于周期调用的任务关键流程信息记录和查看。
比如图谱生成程序，整个生成需要2个小时左右，需要进行一系列操作，可以把一些关键信息记录供查看。
log_notice如果使用log_init_config 指定的module，调用参数就一个message，和log_info一样

log_task_schedule 用于周期性任务的执行监控。参数说明如下：

    task_name：任务名称
    period：执行周期，单位为秒
    module：可以不传，默认使用 log_init_config 传入的module
    exclude_time：忽略某段时间的监控，比如"0-8"表示0点到8点不监控，也可以指定多个时间段，比如"0-8,20-22"

具体使用时，period通常比调度周期长一些，比如一个一小时执行一次的脚本，可以在该任务的最后加上：

    log_task_schedule('task1', 4000, exclude_time='0-6')
    
log_notice 和 log_task_schedule 都需要配置队列信息，所以需要在配置文件里rabbitmq，测试环境配置如下：

    [rabbit_monit]
    host = 52.82.58.138
    user = admin
    port = 5672
    password = sense_mq@2018
    
生产环境如下：

    [rabbit_monit]
    host = 39.107.106.125
    user = admin
    port = 5672
    password = sense_mq@2018
    
log_init_config示例如：
    
    log_init_config('stock', config('log_path'), 'rabbit_monit')


    




