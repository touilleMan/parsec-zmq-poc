import pytest
import trio
from trio.testing import trio_test
import socket
from unittest.mock import patch

from foobar.main import CoreApp
from foobar.utils import CookedSocket


def _get_unused_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def with_core(config=None):
    config = config or {}
    config['PORT'] = _get_unused_port()

    def decorator(testfunc):
        async def wrapper(*args, **kwargs):
            app = CoreApp(config)

            async def run_test_and_cancel_scope(nursery):
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


@trio_test
@with_core()
async def test_connection(core):
    with trio.socket.socket() as sock:
        await sock.connect((core.host, core.port))
        sock = CookedSocket(sock)
        await sock.send({'cmd': 'get_core_state'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}


@trio_test
@with_core()
async def test_login_and_logout(core):
    with patch('foobar.main.CoreApp._get_user') as get_user_mock:
        get_user_mock.return_value = ('a', 'b')
        with trio.socket.socket() as sock:
            await sock.connect((core.host, core.port))
            sock = CookedSocket(sock)
            await sock.send({'cmd': 'identity_info'})
            rep = await sock.recv()
            assert rep == {'status': 'ok', 'loaded': False, 'id': None}
            # Do the login
            await sock.send({'cmd': 'identity_login', 'id': 'john@test.com', 'password': '<secret>'})
            rep = await sock.recv()
            assert rep == {'status': 'ok'}

            await sock.send({'cmd': 'identity_info'})
            rep = await sock.recv()
            assert rep == {'status': 'ok', 'loaded': True, 'id': 'john@test.com'}

        # Changing socket should not trigger logout
        with trio.socket.socket() as sock:
            await sock.connect((core.host, core.port))
            sock = CookedSocket(sock)
            await sock.send({'cmd': 'identity_info'})
            rep = await sock.recv()
            assert rep == {'status': 'ok', 'loaded': True, 'id': 'john@test.com'}
            # Actual logout
            await sock.send({'cmd': 'identity_logout'})
            rep = await sock.recv()
            assert rep == {'status': 'ok'}
            await sock.send({'cmd': 'identity_info'})
            rep = await sock.recv()
            assert rep == {'status': 'ok', 'loaded': False, 'id': None}


@trio_test
@with_core()
async def test_need_login_cmds(core):
    with trio.socket.socket() as sock:
        await sock.connect((core.host, core.port))
        sock = CookedSocket(sock)
        for cmd in [
                'identity_logout',
                'file_create',
                'file_read',
                'file_write',
                'stat',
                'folder_create',
                'move',
                'delete',
                'file_truncate'
            ]:
            await sock.send({'cmd': cmd})
            rep = await sock.recv()
            assert rep == {'status': 'login_required'}
