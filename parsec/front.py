import zmq
import random, string


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
            break
        socket.send_json({'cmd': cmd, 'conn_id': conn_id})
        resp = socket.recv_json()
        print(resp)


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("usage: front.py <address>")
        raise SystemExit(1)
    main(sys.argv[1])
