Zmq POC
-------

start in separate shells:

```
$ python parsec/backend.py tcp://127.0.0.1:5001
$ python parsec/core.py tcp://127.0.0.1:5000 tcp://127.0.0.1:5001
$ python parsec/front.py tcp://127.0.0.1:5000
```
