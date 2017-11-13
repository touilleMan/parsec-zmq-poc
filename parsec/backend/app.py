import attr
import trio
from marshmallow import fields
import nacl.utils
from nacl.public import PrivateKey
from nacl.signing import VerifyKey
from nacl.exceptions import BadSignatureError
from urllib.parse import urlparse

from parsec.core.utils import CookedSocket, BaseCmdSchema, ParsecError, to_jsonb64, from_jsonb64
from parsec.backend.pubkey import MockedPubKeyComponent
from parsec.backend.vlob import MockedVlobComponent
from parsec.backend.user_vlob import MockedUserVlobComponent


class cmd_LOGIN_Schema(BaseCmdSchema):
    id = fields.String(required=True)
    password = fields.String(missing=None)


@attr.s
class ClientContext:
    id = attr.ib()
    pubkey = attr.ib()


class BackendApp:

    def __init__(self, config):
        self.config = config
        self.server_ready = trio.Event()
        self.host = config['HOST']
        self.port = int(config['PORT'])
        self.nursery = None
        self.pubkey = MockedPubKeyComponent()
        self.vlob = MockedVlobComponent()
        self.user_vlob = MockedUserVlobComponent()
        self.pubkey = MockedPubKeyComponent()

    async def _do_handshake(self, sock):
        challenge = nacl.utils.random(self.config.get('HANDSHAKE_CHALLENGE_SIZE', 48))
        hds1 = {'handshake': 'challenge', 'challenge': to_jsonb64(challenge)}
        await sock.send(hds1)
        hds2 = await sock.recv()
        # TODO: check response validity...
        claimed_identity = hds2['identity']
        rawkey = await self.pubkey.get(claimed_identity)
        if not rawkey:
            await sock.send({'status': 'bad_identity'})
            return
        try:
            returned_challenge = VerifyKey(rawkey).verify(from_jsonb64(hds2['answer']))
            if returned_challenge != challenge:
                raise BadSignatureError()
            await sock.send({"status": "ok", "handshake": "done"})
            return ClientContext(claimed_identity, rawkey)
        except BadSignatureError:
            await sock.send({'status': 'bad_identity'})

    async def _serve_client(self, client_sock):
        with client_sock:
            sock = CookedSocket(client_sock)
            print('START HANDSHAKE')
            client_ctx = await self._do_handshake(sock)
            if not client_ctx:
                # Invalid handshake
                print('BAD HANDSHAKE')
                return
            print('HANDSHAKE DONE, CLIENT IS `%s`' % client_ctx.id)
            while True:
                req = await sock.recv()
                if not req:  # Client disconnected
                    print('CLIENT DISCONNECTED')
                    break
                print('REQ %s' % req)
                cmd_func = getattr(self, '_cmd_%s' % req['cmd'].upper())
                try:
                    rep = await cmd_func(client_ctx, req)
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
