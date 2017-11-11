import trio
import socket
import attr
from collections import defaultdict
from unittest.mock import Mock, patch
from functools import wraps
from nacl.public import PrivateKey

from foobar.main import CoreApp
from foobar.utils import CookedSocket
from foobar.local_storage import BaseLocalStorage

from tests.populate_local_storage import populate_local_storage_cls


TEST_USERS = {
    'alice@test': b'\xb4A\x03`X\xb8\xae3\xa5\x1f\xd7\x99~\xab+$!A\xfb\xcb\xfd\x18\x03\x9f\xfbD\x83\x81\x1f\xa1\xbb\xb0',
    'bob@test': b"&\x14\n'\x06Y;\xae.\xb6\xb7\xbf\xf4\xa7'QZ\xd1\x161\xa1\x00\xa8\xb0\xdcm\xb4\x1d\x9b\xackJ",
    'mallory@test': b"\xad&\xb4'\xe9\x00\xd3\x0cSk\t\xff\x9c\xd0L\xd4\x90]u>\t\x8a\xb3\xe2\n/\xa8\x91\xc4\xd1\xaa\xc1"
}


@attr.s
class User:
    id = attr.ib()
    privkey = attr.ib()

    @property
    def pubkey(self):
        return self.privkey.public_key


for userid, userkey in TEST_USERS.items():
    print('load', userid)
    locals()[userid.split('@')[0]] = User(userid, PrivateKey(userkey))


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


class CoreAppTesting(CoreApp):
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


def mocked_local_storage_cls_factory():

    @attr.s
    class InMemoryStorage:
        # Can be changed before initialization (that's why we use a factory btw)
        blocks = attr.ib(default=attr.Factory(dict))
        dirty_blocks = attr.ib(default=attr.Factory(dict))
        dirty_file_manifests = attr.ib(default=attr.Factory(dict))
        placeholder_file_manifests = attr.ib(default=attr.Factory(dict))
        file_manifests = attr.ib(default=attr.Factory(dict))
        local_user_manifest = attr.ib(default=None)

        def get_block(self, id):
            return self.blocks.get(id)

        def get_file_manifest(self, id):
            return self.file_manifests.get(id)

        def get_local_user_manifest(self):
            return self.local_user_manifest

        def save_local_user_manifest(self, data):
            self.local_user_manifest = data

        def get_dirty_block(self, id):
            return self.dirty_blocks.get(id)

        def save_dirty_block(self, id, data):
            self.dirty_blocks[id] = data

        def get_dirty_file_manifest(self, id):
            return self.dirty_file_manifests.get(id)

        def save_dirty_file_manifest(self, id, data):
            self.dirty_file_manifests[id] = data

        def get_placeholder_file_manifest(self, id):
            return self.placeholder_file_manifests.get(id)

        def save_placeholder_file_manifest(self, id, data):
            self.placeholder_file_manifests[id] = data

    # LocalStorage should store on disk, but faster and easier to do that
    # in memory during tests
    mls_cls = Mock(spec=BaseLocalStorage)
    # Can be changed before initialization (that's why we use a factory btw)
    mls_cls.test_storage = InMemoryStorage()
    mls_instance = mls_cls.return_value

    mls_instance.get_block.side_effect = mls_cls.test_storage.get_block
    mls_instance.get_file_manifest.side_effect = mls_cls.test_storage.get_file_manifest
    mls_instance.get_local_user_manifest.side_effect = mls_cls.test_storage.get_local_user_manifest
    mls_instance.save_local_user_manifest.side_effect = mls_cls.test_storage.save_local_user_manifest
    mls_instance.get_dirty_block.side_effect = mls_cls.test_storage.get_dirty_block
    mls_instance.save_dirty_block.side_effect = mls_cls.test_storage.save_dirty_block
    mls_instance.get_dirty_file_manifest.side_effect = mls_cls.test_storage.get_dirty_file_manifest
    mls_instance.save_dirty_file_manifest.side_effect = mls_cls.test_storage.save_dirty_file_manifest
    mls_instance.get_placeholder_file_manifest.side_effect = mls_cls.test_storage.get_placeholder_file_manifest
    mls_instance.save_placeholder_file_manifest.side_effect = mls_cls.test_storage.save_placeholder_file_manifest

    return mls_cls


def with_core(config=None, mocked_local_storage=True):
    config = config or {}
    config['PORT'] = _get_unused_port()

    def decorator(testfunc):
        # @wraps(testfunc)
        async def wrapper(*args, **kwargs):
            core = CoreAppTesting(config)

            async def run_test_and_cancel_scope(nursery):
                if mocked_local_storage:
                    mocked_local_storage_cls = mocked_local_storage_cls_factory()
                    core.mocked_local_storage_cls = mocked_local_storage_cls
                    with patch('foobar.local_fs.LocalStorage', mocked_local_storage_cls):
                        await testfunc(core, *args, **kwargs)
                else:
                    await testfunc(core, *args, **kwargs)
                nursery.cancel_scope.cancel()

            async with trio.open_nursery() as nursery:
                nursery.start_soon(core.run)
                with trio.move_on_after(1) as cancel_scope:
                    await core.server_ready.wait()
                if not cancel_scope.cancelled_caught:
                    nursery.start_soon(run_test_and_cancel_scope, nursery)
        return wrapper
    return decorator


def with_populated_local_storage(user='alice'):
    if isinstance(user, str):
        user = globals()[user]
    assert isinstance(user, User)

    def decorator(testfunc):
        # @wraps(testfunc)
        async def wrapper(core, *args, **kwargs):
            assert isinstance(core, CoreAppTesting), 'missing `@with_core` parent decorator !'
            populate_local_storage_cls(user, core.mocked_local_storage_cls)
            await testfunc(core, *args, **kwargs)
        return wrapper
    return decorator
