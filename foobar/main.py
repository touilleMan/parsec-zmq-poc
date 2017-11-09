import attr
import trio
from marshmallow import fields
from nacl.public import PrivateKey

from .config import CONFIG
from .utils import CookedSocket, BaseCmdSchema, ParsecMessageError
from .fs import FSPipeline


class cmd_LOGIN_Schema(BaseCmdSchema):
    id = fields.String(required=True)
    password = fields.String(missing=None)


class CoreApp:

    def __init__(self, config=None):
        self.config = CONFIG.copy()
        if config:
            self.config.update(config)
        self.server_ready = trio.Event()
        self.host = self.config['HOST']
        self.port = self.config['PORT']
        self.auth_user = None
        self.auth_privkey = None
        self.fs = None

    def _get_user(self, userid, password):
        return None

    async def _serve_client(self, client_sock):
        sock = CookedSocket(client_sock)
        while True:
            req = await sock.recv()
            if not req:  # Client disconnected
                return
            cmd_func = getattr(self, '_cmd_%s' % req['cmd'].upper())
            try:
                rep = await cmd_func(req)
            except ParsecMessageError as err:
                rep = err.to_dict()
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
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._wait_clients, nursery)

    async def login(self, userid, rawprivkey):
        self.auth_user = userid
        self.auth_privkey = PrivateKey(rawprivkey)
        self.fs = FSPipeline(self)
        await self.fs.init()

    async def logout(self):
        await self.fs.teardown()
        self.auth_user = self.auth_privkey = None

    async def _cmd_REGISTER(self, req):
        return {'status': 'not_implemented'}

    async def _cmd_IDENTITY_LOGIN(self, req):
        if self.auth_user:
            return {'status': 'already_logged'}
        msg = cmd_LOGIN_Schema().load(req)
        userkeys = self._get_user(msg['id'], msg['password'])
        if not userkeys:
            return {'status': 'unknown_user'}
        await self.login(msg['id'], userkeys)
        return {'status': 'ok'}

    async def _cmd_IDENTITY_LOGOUT(self, req):
        if not self.auth_user:
            return {'status': 'login_required'}
        await self.logout()
        return {'status': 'ok'}

    async def _cmd_IDENTITY_INFO(self, req):
        return {'status': 'ok', 'loaded': bool(self.auth_user), 'id': self.auth_user}

    async def _cmd_GET_AVAILABLE_LOGINS(self, req):
        pass

    async def _cmd_GET_CORE_STATE(self, req):
        return {'status': 'ok'}

    async def _cmd_FILE_CREATE(self, req):
        if not self.auth_user:
            return {'status': 'login_required'}
        return {'status': 'ok'}

    async def _cmd_FILE_READ(self, req):
        if not self.auth_user:
            return {'status': 'login_required'}
        return await self.fs._cmd_FILE_READ(req)

    async def _cmd_FILE_WRITE(self, req):
        if not self.auth_user:
            return {'status': 'login_required'}
        return await self.fs._cmd_FILE_WRITE(req)

    async def _cmd_STAT(self, req):
        if not self.auth_user:
            return {'status': 'login_required'}
        return await self.fs._cmd_STAT(req)

    async def _cmd_FOLDER_CREATE(self, req):
        if not self.auth_user:
            return {'status': 'login_required'}
        return await self.fs._cmd_FOLDER_CREATE(req)

    async def _cmd_MOVE(self, req):
        if not self.auth_user:
            return {'status': 'login_required'}
        return await self.fs._cmd_MOVE(req)

    async def _cmd_DELETE(self, req):
        if not self.auth_user:
            return {'status': 'login_required'}
        return await self.fs._cmd_DELETE(req)

    async def _cmd_FILE_TRUNCATE(self, req):
        if not self.auth_user:
            return {'status': 'login_required'}
        return await self.fs._cmd_FILE_TRUNCATE(req)


def main():
    app = CoreApp()
    trio.run(app.run)


async def run_client():
    with trio.socket.socket() as sock:
        await sock.connect(('127.0.0.1', 9999))
        while True:
            sock = CookedSocket(sock)
            req = {'msg': 'ping'}
            print('client: =>', req)
            await sock.send(req)
            rep = await sock.recv()
            print('client: <=', rep)
            await trio.sleep(1)
            return 42


if __name__ == '__main__':
    import sys
    if sys.argv[1] == 'client':
        raise SystemExit(trio.run(run_client))
    else:
        main()
