import zmq
import json

# Dummy data

user_manifest = {
    '/': {'type': 'folder'},
    '/foo': {'type': 'folder'},
    '/foo/sub.txt': {'type': 'file', 'id': '001'},
    '/bar.txt': {'type': 'file', 'id': '002'},
}

file_manifests = {
    '001': {
        'blocks': [
            {'id': '001001'},
            {'id': '001002'},
        ]
    },
    '002': {
        'blocks': [
            {'id': '002001'},
            {'id': '002002'},
        ]
    }
}

# Also handle blocks in backend for the sake of simplicity
blocks = {
    # Should be bytes, but use str instead not to bother with json encoding
    '001001': 'hello ',
    '001002': 'world !',
    '002001': 'foo',
    '002002': 'bar',
}


def main(addr):
    global user_manifest
    global file_manifests
    global blocks
    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)
    socket.bind(addr)

    try:
        while True:
            id, _, raw_msg = socket.recv_multipart()
            msg = json.loads(raw_msg.decode())
            if msg['cmd'] == 'user_manifest_read':
                resp = {'status': 'ok', 'content': user_manifest}
            elif msg['cmd'] == 'user_manifest_write':
                user_manifest = msg['content']
                resp = {'status': 'ok'}
            elif msg['cmd'] == 'file_manifest_read':
                try:
                    resp = {'status': 'ok', 'content': file_manifests[msg['id']]}
                except KeyError:
                    resp = {'status': 'not_found'}
            elif msg['cmd'] == 'file_manifest_write':
                try:
                    file_manifests[msg['id']] = msg['content']
                except KeyError:
                    resp = {'status': 'not_found'}
                resp = {'status': 'ok'}
            elif msg['cmd'] == 'block_read':
                try:
                    resp = {'status': 'ok', 'content': blocks[msg['id']]}
                except KeyError:
                    resp = {'status': 'not_found'}
            elif msg['cmd'] == 'block_write':
                try:
                    blocks[msg['id']] = msg['content']
                except KeyError:
                    resp = {'status': 'not_found'}
                resp = {'status': 'ok'}
            socket.send_multipart([id, b'', json.dumps(resp).encode()])
            print('MSG: %s ==> %s' % (msg, resp))
    except KeyboardInterrupt:
        print('bye ;-)')


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("usage: backend.py <address>")
        raise SystemExit(1)
    main(sys.argv[1])
