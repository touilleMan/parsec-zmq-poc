import trio
from urllib.parse import urlparse
from nacl.signing import SigningKey

from .utils import to_jsonb64, CookedSocket


class BackendConnection:
    def __init__(self, authid, auth_privkey, addr):
        self.authid = authid
        self.auth_privkey = auth_privkey
        self.addr = urlparse(addr)
        self.is_connected = False
        self.socket = None
        self.req_queue = trio.Queue(1)
        self.rep_queue = trio.Queue(1)

    async def send(self, req):
        await self.req_queue.put(req)
        return await self.rep_queue.get()

    async def init(self, nursery):
        self.nursery = nursery
        nursery.start_soon(self._backend_connection)

    async def teardown(self):
        pass

    async def ping(self):
        pass

    async def _backend_connection(self):
        with trio.socket.socket() as sock:
            await sock.connect((self.addr.hostname, self.addr.port))
            import pdb; pdb.set_trace()
            sock = CookedSocket(sock)
            # Handshake
            hds1 = await sock.recv()
            assert hds1['handshake'] == 'challenge'
            k = SigningKey(self.auth_privkey.encode())
            answer = k.sign(hds1['challenge'].encode())
            hds2 = {'handshake': 'answer', 'identity': self.authid, 'answer': to_jsonb64(answer)}
            await sock.send(hds2)
            hds3 = await sock.recv()
            assert hds3 == {'status': 'ok', 'handshake': 'done'}
            # Regular communication
            while True:
                # TODO: handle disconnection
                req = await self.req_queue.get()
                await sock.send(req)
                rep = await sock.recv()
                await self.req_queue.put(rep)
