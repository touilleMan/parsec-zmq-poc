import trio
import socket
from collections import defaultdict
from unittest.mock import patch

from foobar.main import CoreApp
from foobar.utils import CookedSocket


TEST_USERS = {
    'alice@test': b'\xb4A\x03`X\xb8\xae3\xa5\x1f\xd7\x99~\xab+$!A\xfb\xcb\xfd\x18\x03\x9f\xfbD\x83\x81\x1f\xa1\xbb\xb0',
    'bob@test': b"&\x14\n'\x06Y;\xae.\xb6\xb7\xbf\xf4\xa7'QZ\xd1\x161\xa1\x00\xa8\xb0\xdcm\xb4\x1d\x9b\xackJ",
    'mallory@test': b"\xad&\xb4'\xe9\x00\xd3\x0cSk\t\xff\x9c\xd0L\xd4\x90]u>\t\x8a\xb3\xe2\n/\xa8\x91\xc4\xd1\xaa\xc1"
}


# `unittest.mock.patch` doesn't work as decorator on async functions
def async_patch(*patch_args, **patch_kwargs):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            with patch(*patch_args, **patch_kwargs) as patched:
                return await func(patched, *args, **kwargs)
        return wrapper
    return decorator


def _get_unused_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class TestCoreApp(CoreApp):
    def test_connect(self, auth_as=None):
        return ConnectToCore(self, auth_as)


class ConnectToCore:
    def __init__(self, core, auth_as):
        self.core = core
        self.auth_as = auth_as

    async def __aenter__(self):
        self.sock = trio.socket.socket()
        if self.auth_as and self.core.auth_user != self.auth_as:
            assert self.auth_as in TEST_USERS
            privkey = TEST_USERS[self.auth_as]
            if self.core.auth_user:
                await self.core.logout()
            await self.core.login(self.auth_as, privkey)
        await self.sock.connect((self.core.host, self.core.port))
        cookedsock = CookedSocket(self.sock)
        return cookedsock

    async def __aexit__(self, exc_type, exc, tb):
        self.sock.close()


def mocked_local_storage_factory():
    # LocalStorage should store on disk, but faster and easier to do that
    # in memory during tests
    class MockedLocalStorage:
        # Can be changed before initialization (that's why we use a factory btw)
        blocks = {}
        file_manifests = defaultdict(dict)
        user_manifests = defaultdict(dict)

        def __init__(self, app):
            self.app = app

        async def init(self):
            pass

        async def teardown(self):
            pass

        async def get_block(self, id):
            return self.blocks.get(id)

        async def get_file_manifest(self, id, rts, wts, version=None):
            fm = self.file_manifests.get(id)
            if not fm:
                return None
            if version is not None:
                return fm.get(version)
            else:
                return fm[sorted(fm)[-1]]

        async def get_user_manifest(self, userid, version=None):
            um = self.user_manifests.get(userid)
            if not um:
                return None
            if version is not None:
                return um.get(version)
            else:
                return um[sorted(um)[-1]]

    return MockedLocalStorage


def with_core(config=None, mocked_local_storage=True):
    config = config or {}
    config['PORT'] = _get_unused_port()

    def decorator(testfunc):
        async def wrapper(*args, **kwargs):
            app = TestCoreApp(config)

            async def run_test_and_cancel_scope(nursery):
                if mocked_local_storage:
                    mocked_local_storage_cls = mocked_local_storage_factory()
                    app.mocked_local_storage_cls = mocked_local_storage_cls
                    with patch('foobar.fs.LocalStorage', mocked_local_storage_cls):
                        await testfunc(app, *args, **kwargs)
                else:
                    await testfunc(app, *args, **kwargs)
                nursery.cancel_scope.cancel()

            async with trio.open_nursery() as nursery:
                nursery.start_soon(app.run)
                with trio.move_on_after(1) as cancel_scope:
                    await app.server_ready.wait()
                if not cancel_scope.cancelled_caught:
                    nursery.start_soon(run_test_and_cancel_scope, nursery)
        return wrapper
    return decorator
