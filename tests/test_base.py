import pytest
from trio.testing import trio_test

from tests.common import with_core, async_patch, TEST_USERS


@trio_test
@with_core()
async def test_connection(core):
    async with core.test_connect() as sock:
        await sock.send({'cmd': 'get_core_state'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}




@trio_test
@async_patch('foobar.main.CoreApp._get_user')
@with_core()
async def test_login_and_logout(core, get_user_mock):
    # Return user's curve private key
    get_user_mock.return_value = (b"\xf2O}\x1f\xc7\xaeZ\xed_\xd3yT\xa4\xea'\xe3"
                                  b"\x9dx\xfd\x8a\x16\xc0G\xbeA(\xad\x93z\xdf\xc7[")
    async with core.test_connect() as sock:
        await sock.send({'cmd': 'identity_info'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'loaded': False, 'id': None}
        # Do the login
        await sock.send({'cmd': 'identity_login', 'id': 'john@test', 'password': '<secret>'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}

        await sock.send({'cmd': 'identity_info'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'loaded': True, 'id': 'john@test'}

    # Changing socket should not trigger logout
    async with core.test_connect() as sock:
        await sock.send({'cmd': 'identity_info'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'loaded': True, 'id': 'john@test'}
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
    async with core.test_connect() as sock:
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
