import zmq
import readline
import json
from pprint import pprint


def main(addr):    
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(addr)

    while True:
        cmd = input('>> ')
        if cmd in ['q', 'quit']:
            print('bye ;-)')
            return False  # Don't restart main
        try:
            msg = json.loads(cmd)
        except:
            try:
                cmd, path = cmd.split()
                msg = {'cmd': cmd, 'path': path}
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
