redis-py-tornado
================

greenlet based redis-py doodad


```python
import tornado
from tornado import options, ioloop, gen
import logging
import redis
import greenlet
import string
import random
from tornadoconnection import TornadoConnection, TornadoHiredisParser

@tornado.gen.engine
def execute(func, *args, **kwargs):
    def greenlet_func():
        callback = kwargs.pop('callback')
        ret = func(*args, **kwargs)
        callback(ret)

    gr = greenlet.greenlet(greenlet_func)
    gr.switch()

@tornado.gen.engine
def test():
    items = range(60)
    pipe = r.pipeline()
    for item in items:
        pipe.lrange(item, 0, -1)
    results = yield tornado.gen.Task(execute, pipe.execute)
    logging.info("test results: %.200r" % results)

if __name__ == '__main__':
    tornado.options.parse_command_line()
    logging.getLogger().setLevel(logging.DEBUG)
    
    pool = redis.ConnectionPool(connection_class=TornadoConnection, host='localhost', port=6379, db=0)
    r = redis.StrictRedis(connection_pool=pool)
    logging.info("starting tornado redis")
    
    ioloop.IOLoop.instance().add_callback(test)
    ioloop.IOLoop.instance().start()
```
