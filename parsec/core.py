import zmq
from  threading import Thread
from time import sleep
from uuid import uuid4


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


blocks = {
    # Should be bytes, but use str instead not to bother with json encoding
    '001001': 'hello ',
    '001002': 'world !',
    '002001': 'foo',
    '002002': 'bar',
}


# Pipeline stages


def init_stage():
    context = zmq.Context()
    puller = context.socket(zmq.PULL)
    puller.bind('ipc://init')
    pusher_to_umr = context.socket(zmq.PUSH)
    pusher_to_umr.connect('ipc://umr')

    print('INIT: ready')
    while True:
        msg = puller.recv_json()
        print('INIT: recv %s' % msg)
        # TODO: check message format here ?
        pusher_to_umr.send_json(
            {'msg': msg, 'umr': {'path': msg['path']}, 'cmd': msg['cmd']}
        )


def user_manifest_read_stage():
    context = zmq.Context()
    puller = context.socket(zmq.PULL)
    puller.bind('ipc://umr')
    pusher_to_reply = context.socket(zmq.PUSH)
    pusher_to_reply.connect('ipc://reply')
    pusher_to_fmr = context.socket(zmq.PUSH)
    pusher_to_fmr.connect('ipc://fmr')
    pusher_to_fmw = context.socket(zmq.PUSH)
    pusher_to_fmw.connect('ipc://fmw')
    pusher_to_umw = context.socket(zmq.PUSH)
    pusher_to_umw.connect('ipc://umw')

    print('UMR: ready')
    while True:
        msg = puller.recv_json()
        print('UMR: recv %s' % msg)
        # Do something...
        cmd = msg['cmd']
        path = msg['umr']['path']
        if cmd == 'stat':
            if path in user_manifest:
                msg['resp'] = {'status': 'ok', 'type': user_manifest[path]['type']}
            else:
                msg['resp'] = {'status': 'unknown_path'}
            pusher_to_reply.send_json(msg)
        elif cmd == 'read_file':
            if path not in user_manifest:
                msg['resp'] = {'status': 'unknown_path'}
                pusher_to_reply.send_json(msg)
            else:
                file_access = user_manifest[path]
                if file_access['type'] == 'folder':
                    msg['resp'] = {'status': 'not_a_file'}
                    pusher_to_reply.send_json(msg)
                else:
                    msg['fmr'] = file_access
                    pusher_to_fmr.send_json(msg)
        elif cmd == 'write_file':
            if path not in user_manifest:
                msg['resp'] = {'status': 'unknown_path'}
                pusher_to_reply.send_json(msg)
            else:
                file_access = user_manifest[path]
                if file_access['type'] == 'folder':
                    msg['resp'] = {'status': 'not_a_file'}
                msg['fmr'] = file_access
                pusher_to_fmr.send_json(msg)
        elif cmd == 'create_file':
            msg['fmw'] = {'path': path}
            pusher_to_fmw.send_json(msg)
        elif cmd == 'delete':
            msg['umw'] = {'path': path}
            pusher_to_umw.send_json(msg)
        else:
            print('Unknown command `%s`' % cmd)
            msg['resp'] = {'status': 'unknown_cmd'}
            pusher_to_reply.send_json(msg)


def user_manifest_write_stage():
    context = zmq.Context()
    puller = context.socket(zmq.PULL)
    puller.bind('ipc://umw')
    pusher_to_reply = context.socket(zmq.PUSH)
    pusher_to_reply.connect('ipc://reply')

    print('UMW: ready')
    while True:
        msg = puller.recv_json()
        print('UMW: recv %s' % msg)
        cmd = msg['cmd']
        path = msg['umw']['path']
        if cmd == 'create_file':
            if path in user_manifest:
                msg['resp'] = {'status': 'path_already_exists'}
            else:
                user_manifest[path] = {'type': 'file', 'id': msg['umw']['id']}
                msg['resp'] = {'status': 'ok'}
            pusher_to_reply.send_json(msg)
        elif cmd == 'delete':
            if path in user_manifest:
                del user_manifest[path]
                msg['resp'] = {'status': 'ok'}
            else:
                msg['resp'] = {'status': 'unknown_path'}
            pusher_to_reply.send_json(msg)


def file_manifest_read_stage():
    context = zmq.Context()
    puller = context.socket(zmq.PULL)
    puller.bind('ipc://fmr')
    pusher_to_reply = context.socket(zmq.PUSH)
    pusher_to_reply.connect('ipc://reply')
    pusher_to_br = context.socket(zmq.PUSH)
    pusher_to_br.connect('ipc://br')

    print('FMR: ready')
    while True:
        msg = puller.recv_json()
        print('FMR: recv %s' % msg)
        if msg['fmr']['id'] not in file_manifests:
            print('Unknown file manifest %s' % msg['fmr']['id'])
            msg['resp'] = {'status': 'unknown_file_manifest'}
            pusher_to_reply.send_json(msg)
        file_manifest = file_manifests[msg['fmr']['id']]
        # TODO: check which blocks we should read
        msg['br'] = {'blocks': file_manifest['blocks']}
        pusher_to_br.send_json(msg)


def file_manifest_write_stage():
    context = zmq.Context()
    puller = context.socket(zmq.PULL)
    puller.bind('ipc://fmw')
    pusher_to_reply = context.socket(zmq.PUSH)
    pusher_to_reply.connect('ipc://reply')
    pusher_to_umw = context.socket(zmq.PUSH)
    pusher_to_umw.connect('ipc://umw')

    print('FMW: ready')
    while True:
        msg = puller.recv_json()
        print('FMW: recv %s' % msg)
        if msg['cmd'] == 'create_file':
            # TODO: do backend access here
            id = uuid4().hex
            file_manifests[id] = {'blocks': []}
            msg['umw'] = {'id': id, 'path': msg['msg']['path']}
            pusher_to_umw.send_json(msg)
        elif msg['cmd'] == 'write_file':
            if msg['fmw']['id'] not in file_manifests:
                print('Unknown file manifest %s' % msg['fmw']['id'])
                msg['resp'] = {'status': 'unknown_file_manifest'}
                pusher_to_reply.send_json(msg)
            # TODO: do backend access here
            file_manifests[msg['fmw']['id']]['blocks'] = msg['fmw']['blocks']
            msg['resp'] = {'status': 'ok'}
            pusher_to_reply.send_json(msg)


def block_read_stage():
    context = zmq.Context()
    puller = context.socket(zmq.PULL)
    puller.bind('ipc://br')
    pusher_to_reply = context.socket(zmq.PUSH)
    pusher_to_reply.connect('ipc://reply')
    pusher_to_bw = context.socket(zmq.PUSH)
    pusher_to_bw.connect('ipc://bw')

    print('BR: ready')
    while True:
        msg = puller.recv_json()
        print('BR: recv %s' % msg)
        content = []
        for block in msg['br']['blocks']:
            # TODO: do block I/O access here
            if block['id'] not in blocks:
                print('Unknown block id `%s`' % block['id'])
                msg['resp'] = {'status': 'unknown_block'}
                pusher_to_reply.send_json(msg)
            else:
                content.append(blocks[block['id']])
        if msg['cmd'] == 'read_file':
            msg['resp'] = {'status': 'ok', 'content': ''.join(content)}
            pusher_to_reply.send_json(msg)
        elif msg['cmd'] == 'write_file':
            msg['bw'] = {'old_content': ''.join(content)}
            pusher_to_bw.send_json(msg)


def block_write_stage():
    context = zmq.Context()
    puller = context.socket(zmq.PULL)
    puller.bind('ipc://bw')
    pusher_to_fmw = context.socket(zmq.PUSH)
    pusher_to_fmw.connect('ipc://fmw')

    print('BW: ready')
    while True:
        msg = puller.recv_json()
        print('BW: recv %s' % msg)
        content = msg['bw']['old_content'][:msg['msg'].get('offset', 0)] + msg['msg']['content']
        blksize = 10
        file_blocks = []
        remaincontent = content
        # TODO: do block I/O access here
        while remaincontent:
            blkcontent, remaincontent = remaincontent[:blksize], remaincontent[blksize:]
            blkid = uuid4().hex
            blocks[blkid] = blkcontent
            file_blocks.append({'id': blkid})
        msg['fmw'] = {'blocks': file_blocks, "id": msg['fmr']['id']}
        pusher_to_fmw.send_json(msg)


def reply_stage():
    context = zmq.Context()
    puller = context.socket(zmq.PULL)
    puller.bind('ipc://reply')
    pusher_to_finish = context.socket(zmq.PUSH)
    pusher_to_finish.connect('ipc://finish')

    while True:
        msg = puller.recv_json()
        pusher_to_finish.send_json(msg['resp'])


# R on user manifest
def stat(msg):
    pass


# R on user manifest + file manifest + blocks
def read_file(msg):
    pass


# R on user manifest, R/W on file manifest + blocks
def write_file(msg):
    pass


# R/W on user manifest, W on file manifest + blocks
def create_file(msg):
    pass


# R/W on user manifest
def delete(msg):
    pass


CMDS = {
    'stat': stat,
    'read_file': read_file,
    'write_file': write_file,
    'create_file': create_file,
    'delete': delete,
}


class Pipeline:
    def __init__(self):
        def bootstrap(func):
            def x(*args, **kwargs):
                print('started %s(args=%s, kwargs=%s)' % (func.__name__, args, kwargs))
                ret = func(*args, **kwargs)
                print('stopped %s ret=%s' % (func.__name__, ret))
                return ret

            return x
        self._stage_br = Thread(target=bootstrap(block_read_stage), daemon=True)
        self._stage_bw = Thread(target=bootstrap(block_write_stage), daemon=True)

        self._stage_fmr = Thread(target=bootstrap(file_manifest_read_stage), daemon=True)
        self._stage_fmw = Thread(target=bootstrap(file_manifest_write_stage), daemon=True)

        self._stage_umr = Thread(target=bootstrap(user_manifest_read_stage), daemon=True)
        self._stage_umw = Thread(target=bootstrap(user_manifest_write_stage), daemon=True)

        self._stage_init = Thread(target=bootstrap(init_stage), daemon=True)
        self._stage_reply = Thread(target=bootstrap(reply_stage), daemon=True)

    def start(self):
        self._stage_br.start()
        self._stage_bw.start()

        self._stage_fmr.start()
        self._stage_fmw.start()

        self._stage_umr.start()
        self._stage_umw.start()

        self._stage_init.start()
        self._stage_reply.start()

    def stop(self):
        # TODO: send stop command through zmq
        self._stage_reply.terminate()
        self._stage_init.terminate()
        self._stage_umr.terminate()
        self._stage_umw.terminate()
        self._stage_fmr.terminate()
        self._stage_fmw.terminate()
        self._stage_br.terminate()
        self._stage_bw.terminate()

        self._stage_reply.join()
        self._stage_init.join()
        self._stage_umr.join()
        self._stage_umw.join()
        self._stage_fmr.join()
        self._stage_fmw.join()
        self._stage_br.join()
        self._stage_bw.join()


def main(addr):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(addr)

    pusher = context.socket(zmq.PUSH)
    pusher.connect('ipc://init')
    puller = context.socket(zmq.PULL)
    puller.bind('ipc://finish')

    pipeline = Pipeline()
    pipeline.start()

    try:
        while True:
            # TODO: use DEALER/ROUTER
            msg = socket.recv_json()
            print('Received %s' % msg)
            if msg['cmd'] == 'info':
                resp = {
                    'user_manifest': user_manifest,
                    'file_manifests': file_manifests,
                    'blocks': blocks
                }
            else:
                pusher.send_json(msg)
                resp = puller.recv_json()
            print('Done %s => %s' % (msg, resp))
            socket.send_json(resp)
    except KeyboardInterrupt:
        print('bye ;-)')
    pipeline.stop()


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("usage: core.py <address>")
        raise SystemExit(1)
    main(sys.argv[1])
