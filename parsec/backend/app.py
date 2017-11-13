import attr
import trio
from marshmallow import fields
from nacl.public import PrivateKey
from urllib.parse import urlparse

from parsec.core.utils import CookedSocket, BaseCmdSchema, ParsecError


class cmd_LOGIN_Schema(BaseCmdSchema):
    id = fields.String(required=True)
    password = fields.String(missing=None)


class BackendApp:

    def __init__(self, config):
        self.config = config
        self.server_ready = trio.Event()
        self.host = config['HOST']
        self.port = int(config['PORT'])
        self.nursery = None

    def _get_user(self, userid, password):
        return None

    async def _serve_client(self, client_sock):
        sock = CookedSocket(client_sock)
        while True:
            req = await sock.recv()
            if not req:  # Client disconnected
                print('CLIENT DISCONNECTED')
                return
            print('REQ %s' % req)
            cmd_func = getattr(self, '_cmd_%s' % req['cmd'].upper())
            try:
                rep = await cmd_func(req)
            except ParsecError as err:
                rep = err.to_dict()
            print('REP %s' % rep)
            await sock.send(rep)

    async def _wait_clients(self, nursery):
        with trio.socket.socket() as listen_sock:
            listen_sock.bind((self.host, self.port))
            listen_sock.listen()
            self.server_ready.set()
            while True:
                server_sock, _ = await listen_sock.accept()
                nursery.start_soon(self._serve_client, server_sock)

    async def run(self):
        async with trio.open_nursery() as self.nursery:
            self.nursery.start_soon(self._wait_clients, self.nursery)
