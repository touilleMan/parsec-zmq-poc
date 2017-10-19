import zmq
import zmq.devices
import signal
import base64
import json
from  threading import Thread
from time import sleep
from uuid import uuid4


# +---------------------------------------------------------+
# | command     | user manifest  | files manifests | blocks |
# +---------------------------------------------------------+
# | stat        |       R        |                 |        |
# | read_file   |       R        |        R        |   R    |
# | write_file  |       R        |       R/W       |  R/W   |
# | create_file |      R/W       |        W        |        |
# | delete      |      R/W       |                 |        |
# +---------------------------------------------------------+


# Initialized during user_manifest_read_stage
user_manifest = None


# Pipeline stages


def init_stage(context):
    puller = context.socket(zmq.PULL)
    puller.bind('inproc://init')
    pusher_to_umr = context.socket(zmq.PUSH)
    pusher_to_umr.connect('inproc://umr')

    print('INIT: ready')
    while True:
        msg = puller.recv_json()
        print('INIT: recv %s' % msg)
        if '__system_exit__' in msg:
            pusher_to_umr.send_json(msg)
            print('INIT: exit')
            break
        # TODO: check message format here ?
        pusher_to_umr.send_json(
            {'msg': msg, 'umr': {'path': msg['path']}, 'cmd': msg['cmd'],
             '__client_id__': msg.pop('__client_id__')}
        )


def user_manifest_read_stage(context):
    puller = context.socket(zmq.PULL)
    puller.bind('inproc://umr')
    pusher_to_reply = context.socket(zmq.PUSH)
    pusher_to_reply.connect('inproc://reply')
    pusher_to_fmr = context.socket(zmq.PUSH)
    pusher_to_fmr.connect('inproc://fmr')
    pusher_to_fmw = context.socket(zmq.PUSH)
    pusher_to_fmw.connect('inproc://fmw')
    pusher_to_umw = context.socket(zmq.PUSH)
    pusher_to_umw.connect('inproc://umw')

    backend = context.socket(zmq.REQ)
    backend.connect('inproc://backend')
    global user_manifest
    backend.send_json({'cmd': 'user_manifest_read'})
    resp = backend.recv_json()
    assert resp['status'] == 'ok'
    user_manifest = resp['content']

    print('UMR: ready')
    while True:
        msg = puller.recv_json()
        print('UMR: recv %s' % msg)
        if '__system_exit__' in msg:
            pusher_to_fmr.send_json(msg)
            print('UMR: exit')
            break
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


def user_manifest_write_stage(context):
    puller = context.socket(zmq.PULL)
    puller.bind('inproc://umw')
    pusher_to_reply = context.socket(zmq.PUSH)
    pusher_to_reply.connect('inproc://reply')

    backend = context.socket(zmq.REQ)
    backend.connect('inproc://backend')

    print('UMW: ready')
    while True:
        msg = puller.recv_json()
        print('UMW: recv %s' % msg)
        if '__system_exit__' in msg:
            pusher_to_reply.send_json(msg)
            print('UMW: exit')
            break
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
        # Sync user manifest with the backend here
        # TODO: should be done in the synchronizer
        backend.send_json({'cmd': 'user_manifest_write', 'content': user_manifest})
        assert backend.recv_json()['status'] == 'ok'


def file_manifest_read_stage(context):
    puller = context.socket(zmq.PULL)
    puller.bind('inproc://fmr')
    pusher_to_reply = context.socket(zmq.PUSH)
    pusher_to_reply.connect('inproc://reply')
    pusher_to_br = context.socket(zmq.PUSH)
    pusher_to_br.connect('inproc://br')

    backend = context.socket(zmq.REQ)
    backend.connect('inproc://backend')

    print('FMR: ready')
    while True:
        msg = puller.recv_json()
        print('FMR: recv %s' % msg)
        if '__system_exit__' in msg:
            pusher_to_br.send_json(msg)
            print('FMR: exit')
            break
        backend.send_json({'cmd': 'file_manifest_read', 'id': msg['fmr']['id']})
        resp = backend.recv_json()
        if resp['status'] != 'ok':
            print('Unknown file manifest %s' % msg['fmr']['id'])
            msg['resp'] = {'status': 'unknown_file_manifest'}
            pusher_to_reply.send_json(msg)
        file_manifest = resp['content']
        # TODO: check which blocks we should read
        msg['br'] = {'blocks': file_manifest['blocks']}
        pusher_to_br.send_json(msg)


def file_manifest_write_stage(context):
    puller = context.socket(zmq.PULL)
    puller.bind('inproc://fmw')
    pusher_to_reply = context.socket(zmq.PUSH)
    pusher_to_reply.connect('inproc://reply')
    pusher_to_umw = context.socket(zmq.PUSH)
    pusher_to_umw.connect('inproc://umw')

    backend = context.socket(zmq.REQ)
    backend.connect('inproc://backend')

    print('FMW: ready')
    while True:
        msg = puller.recv_json()
        print('FMW: recv %s' % msg)
        if '__system_exit__' in msg:
            pusher_to_umw.send_json(msg)
            print('FMW: exit')
            break
        if msg['cmd'] == 'create_file':
            id = uuid4().hex
            backend.send_json({'cmd': 'file_manifest_write', 'id': id, 'content': {'blocks': []}})
            resp = backend.recv_json()
            if resp['status'] != 'ok':
                msg['resp'] = resp
                pusher_to_reply.send_json(msg)
            else:
                msg['umw'] = {'id': id, 'path': msg['msg']['path']}
                pusher_to_umw.send_json(msg)
        elif msg['cmd'] == 'write_file':
            backend.send_json({
                'cmd': 'file_manifest_write', 'id': msg['fmw']['id'],
                'content': {'blocks': msg['fmw']['blocks']}})
            resp = backend.recv_json()
            if resp['status'] != 'ok':
                print('Unknown file manifest %s' % msg['fmw']['id'])
                msg['resp'] = {'status': 'unknown_file_manifest'}
                pusher_to_reply.send_json(msg)
            else:
                msg['resp'] = {'status': 'ok'}
                pusher_to_reply.send_json(msg)


def block_read_stage(context):
    puller = context.socket(zmq.PULL)
    puller.bind('inproc://br')
    pusher_to_reply = context.socket(zmq.PUSH)
    pusher_to_reply.connect('inproc://reply')
    pusher_to_bw = context.socket(zmq.PUSH)
    pusher_to_bw.connect('inproc://bw')

    backend = context.socket(zmq.REQ)
    backend.connect('inproc://backend')

    print('BR: ready')
    while True:
        msg = puller.recv_json()
        print('BR: recv %s' % msg)
        if '__system_exit__' in msg:
            pusher_to_bw.send_json(msg)
            print('BR: exit')
            break
        content = []
        for block in msg['br']['blocks']:
            backend.send_json({'cmd': 'block_read', 'id': block['id']})
            resp = backend.recv_json()
            if resp['status'] != 'ok':
                print('Unknown block id `%s`' % block['id'])
                msg['resp'] = {'status': 'unknown_block'}
                pusher_to_reply.send_json(msg)
            else:
                content.append(resp['content'])
        if msg['cmd'] == 'read_file':
            msg['resp'] = {'status': 'ok', 'content': ''.join(content)}
            pusher_to_reply.send_json(msg)
        elif msg['cmd'] == 'write_file':
            msg['bw'] = {'old_content': ''.join(content)}
            pusher_to_bw.send_json(msg)


def block_write_stage(context):
    puller = context.socket(zmq.PULL)
    puller.bind('inproc://bw')
    pusher_to_fmw = context.socket(zmq.PUSH)
    pusher_to_fmw.connect('inproc://fmw')

    backend = context.socket(zmq.REQ)
    backend.connect('inproc://backend')

    print('BW: ready')
    while True:
        msg = puller.recv_json()
        print('BW: recv %s' % msg)
        if '__system_exit__' in msg:
            pusher_to_fmw.send_json(msg)
            print('BW: exit')
            break
        content = msg['bw']['old_content'][:msg['msg'].get('offset', 0)] + msg['msg']['content']
        blksize = 10
        file_blocks = []
        remaincontent = content
        # TODO: do block I/O access here
        while remaincontent:
            blkcontent, remaincontent = remaincontent[:blksize], remaincontent[blksize:]
            blkid = uuid4().hex
            backend.send_json({'cmd': 'block_write', 'id': blkid, 'content': blkcontent})
            resp = backend.recv_json()
            assert resp['status'] == 'ok'
            file_blocks.append({'id': blkid})
        msg['fmw'] = {'blocks': file_blocks, "id": msg['fmr']['id']}
        pusher_to_fmw.send_json(msg)


def reply_stage(context):
    puller = context.socket(zmq.PULL)
    puller.bind('inproc://reply')
    pusher_to_finish = context.socket(zmq.PUSH)
    pusher_to_finish.connect('inproc://finish')

    print('REPLY: ready')
    while True:
        msg = puller.recv_json()
        print('REPLY: recv %s' % msg)
        if '__system_exit__' in msg:
            pusher_to_finish.send_json(msg)
            print('REPLY: exit')
            break
        pusher_to_finish.send_json({**msg['resp'], '__client_id__': msg['__client_id__']})


class Pipeline:
    def __init__(self, context):
        def bootstrap(func):

            def start(*args):
                print('started %s' % func.__name__)
                ret = func(context, *args)
                print('stopped %s' % func.__name__)
                return ret

            return start

        self._stage_br = Thread(target=bootstrap(block_read_stage))
        self._stage_bw = Thread(target=bootstrap(block_write_stage))

        self._stage_fmr = Thread(target=bootstrap(file_manifest_read_stage))
        self._stage_fmw = Thread(target=bootstrap(file_manifest_write_stage))

        self._stage_umr = Thread(target=bootstrap(user_manifest_read_stage))
        self._stage_umw = Thread(target=bootstrap(user_manifest_write_stage))

        self._stage_init = Thread(target=bootstrap(init_stage))
        self._stage_reply = Thread(target=bootstrap(reply_stage))

    def start(self):
        self._stage_br.start()
        self._stage_bw.start()

        self._stage_fmr.start()
        self._stage_fmw.start()

        self._stage_umr.start()
        self._stage_umw.start()

        self._stage_init.start()
        self._stage_reply.start()

    def wait_stop(self):
        self._stage_reply.join()
        self._stage_init.join()
        self._stage_umr.join()
        self._stage_umw.join()
        self._stage_fmr.join()
        self._stage_fmw.join()
        self._stage_br.join()
        self._stage_bw.join()


def main(addr, backend_addr):
    context = zmq.Context.instance()
    client = context.socket(zmq.ROUTER)
    client.bind(addr)

    pusher = context.socket(zmq.PUSH)
    pusher.connect('inproc://init')
    puller = context.socket(zmq.PULL)
    puller.bind('inproc://finish')

    # Multiplexing backend connection between pipeline stages
    backend = zmq.devices.ThreadDevice(zmq.QUEUE, zmq.REP, zmq.REQ)
    backend.bind_in('inproc://backend')
    backend.connect_out(backend_addr)
    backend.start()
    # TODO: check if the backend connection is ready

    pipeline = Pipeline(context)
    pipeline.start()
    # TODO: should make sure the pipeline is ready before going further

    graceful_shudown = False
    def _signint_handler(signum, frame):
        nonlocal graceful_shudown
        if not graceful_shudown:
            # Start exit signal propagation to the pipeline
            graceful_shudown = True
            msg = {"__system_exit__": True}
            pusher.send_json(msg)
        else:
            # Another signint has already been send, so user *really* want to leave
            raise SystemExit("Forced exit, you may have lose data :'-(")
    signal.signal(signal.SIGINT, _signint_handler)

    poll = zmq.Poller()
    poll.register(client, zmq.POLLIN)
    poll.register(puller, zmq.POLLIN)
    try:
        while True:
            for sock, _ in poll.poll():
                if sock is client:
                    # Get command from client and push it into the pipeline
                    id, _, raw_msg = client.recv_multipart()
                    msg = json.loads(raw_msg.decode())
                    msg['__client_id__'] = base64.encodebytes(id).decode()
                    print('[%s] Received %s' % (id, msg))
                    if graceful_shudown:
                        # Pipeline is closing, so we're no longer processing requests
                        resp = {'status': 'shutting_down'}
                        client.send_multipart([id, b'', json.dumps(resp).encode()])
                    if msg['cmd'] == 'info':
                        resp = {
                            'user_manifest': user_manifest,
                            'file_manifests': file_manifests,
                            'blocks': blocks
                        }
                        client.send_multipart([id, b'', json.dumps(resp).encode()])
                    else:
                        pusher.send_json(msg)
                elif sock is puller:
                    # Command response is available, send it back from client
                    resp = puller.recv_json()
                    if '__system_exit__' in resp:
                        # All pipeline stages are down, we can leave
                        print('bye ;-)')
                        return
                    id = base64.decodebytes(resp.pop('__client_id__').encode())
                    print('[%s] Done => %s' % (id, resp))
                    client.send_multipart([id, b'', json.dumps(resp).encode()])
    finally:
        pipeline.wait_stop()
        # backend is started in daemon mode, will stop by itself


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("usage: core.py <address> <backend-address>")
        raise SystemExit(1)
    main(*sys.argv[1:])
