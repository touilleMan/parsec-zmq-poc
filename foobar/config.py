from os import environ


CONFIG = {
    'SERVER_PUBLIC': '',
    'HOST': '127.0.0.1',
    'PORT': 9999,
    'CLIENTS_SOCKET_URL': environ.get('CLIENTS_SOCKET_URL', 'tcp://localhost:9090'),
    'BACKEND_URL': environ.get('BACKEND_URL', ''),
    'ANONYMOUS_PUBKEY': 'y4scJ4mV09t5FJXtjwTctrpFg+xctuCyh+e4EoyuDFA=',
    'ANONYMOUS_PRIVKEY': 'ua1CbOtQ0dUrWG0+Satf2SeFpQsyYugJTcEB4DNIu/c=',
    'LOCAL_STORAGE_DIR': ''
}
