import attr
import trio

from .utils import CookedSocket


class CoreApp:
    def __init__(self):
        self.server_ready = trio.Event()

    async def _serve_client(self, client_sock):
        sock = CookedSocket(client_sock)
        while True:
            req = await sock.recv()
            if not req:  # Client disconnected
                return
            cmd_func = getattr(self, '_cmd_%s' % req['cmd'].upper())
            rep = await cmd_func(req)
            await sock.send(rep)

    async def _wait_clients(self, nursery, host, port):
        with trio.socket.socket() as listen_sock:
            listen_sock.bind((host, port))
            listen_sock.listen()
            self.server_ready.set()
            while True:
                server_sock, _ = await listen_sock.accept()
                nursery.start_soon(self._serve_client, server_sock)

    async def run(self, host="127.0.0.1", port=9999):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self._wait_clients, nursery, host, port)

    async def _cmd_REGISTER(self, req):
        return {'status': 'not_implemented'}

    async def _cmd_LOGIN(self, req):
        pass

    async def _cmd_GET_AVAILABLE_LOGINS(self, req):
        pass

    async def _cmd_GET_CORE_STATE(self, req):
        return {'status': 'ok'}

    async def _cmd_LOGOUT(self, req):
        pass

    async def _cmd_FILE_CREATE(self, req):
        self.connected
        pass

    async def _cmd_FILE_READ(self, req):
        pass

    async def _cmd_FILE_WRITE(self, req):
        pass

    async def _cmd_STAT(self, req):
        pass

    async def _cmd_FOLDER_CREATE(self, req):
        pass

    async def _cmd_MOVE(self, req):
        pass

    async def _cmd_DELETE(self, req):
        pass

    async def _cmd_FILE_TRUNCATE(self, req):
        pass


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
