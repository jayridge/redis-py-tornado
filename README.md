redis-py-tornado
================

greenlet based redis-py doodad


```python
import tornado
import logging
import redis
import greenlet
import string
import random

from tornado import gen
from tornadoconnection import TornadoConnection


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
    load_data()
    pipe = r.pipeline()
    for key in keys:
        pipe.lrange(key, 0, -1)
    results = yield tornado.gen.Task(execute, pipe.execute)
    logging.info("test results: %r" % results)

def load_data():
    for key in keys:
        val = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(24))
        execute(r.lpush, key, val, callback=(yield gen.Callback(key)))
    tornado.gen.WaitAll(keys)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    
    keys = range(50)
    pool = redis.ConnectionPool(connection_class=TornadoConnection, host='localhost', port=6379, db=0)
    r = redis.StrictRedis(connection_pool=pool)
    
    tornado.ioloop.PeriodicCallback(test, 1000).start()
    tornado.ioloop.IOLoop.instance().start()
```
