import zmq
from time import sleep


def main(addr):    
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(addr)

    while True:
        cmd = socket.recv_json()
        print('Received %s' % cmd)
        sleep(1)
        socket.send_json({'status': 'ok'})
        print('Done %s' % cmd)


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("usage: core.py <address>")
        raise SystemExit(1)
    main(sys.argv[1])
