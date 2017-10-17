import zmq
import random, string
import readline
import json
from pprint import pprint


def _unique_enough_id():
    # Colision risk is high, but this is pretty fine (and much more readable
    # than a uuid4) for giving id to connections
    return ''.join([random.choice(string.ascii_letters + string.digits) for ch in range(4)])


def main(addr):    
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(addr)
    conn_id = _unique_enough_id()

    while True:
        cmd = input('%s>> ' % conn_id)
        if cmd in ['q', 'quit']:
            print('bye ;-)')
            return False  # Don't restart main
        try:
            msg = {**json.loads(cmd), 'conn_id': conn_id}
        except:
            try:
                cmd, path = cmd.split()
                msg = {'cmd': cmd, 'path': path, 'conn_id': conn_id}
            except:
                print('Invalid format')
                continue
        socket.send_json(msg)
        try:
            resp = socket.recv_json()
        except KeyboardInterrupt:
            return True  # Restart main
        pprint(resp)


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("usage: front.py <address>")
        raise SystemExit(1)
    while main(sys.argv[1]):
        continue
