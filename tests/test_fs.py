import pytest
import json
from trio.testing import trio_test
from nacl.public import Box, PrivateKey

from tests.common import with_core, TEST_USERS


@trio_test
@with_core()
async def test_connection(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'get_core_state'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}


def _populate_local_storage_for_alice(core):
    aliceid = 'alice@test'
    alice_privkey = PrivateKey(TEST_USERS[aliceid])
    alice_user_manifest = {
        'version': 0,
        'root': {
            'type': 'folder',
            'children': {
                'foo': {
                    'type': 'folder',
                    'children': {},
                    'stat': {'created': '2017-12-02T12:30:23', 'updated': '2017-12-02T12:30:23'}
                },
                'bar.txt': {
                    'type': 'file',
                    'id': '42',
                    'read_trust_seed': '<rts>',
                    'write_trust_seed': '<wts>',
                    'key': '<key>'
                }
            },
            'stat': {'created': '2017-12-02T12:30:23', 'updated': '2017-12-02T12:30:23'}
        }
    }
    box = Box(alice_privkey, alice_privkey.public_key)
    core.mocked_local_storage_cls.user_manifests[aliceid][0] = box.encrypt(json.dumps(alice_user_manifest).encode())


@trio_test
@with_core()
async def test_stat_folder(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['bar.txt', 'foo'], 'type': 'folder'}


@pytest.mark.xfail
@trio_test
@with_core()
async def test_stat_file(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'stat', 'path': '/bar.txt'})
        rep = await sock.recv()
    assert rep == {
        'status': 'ok',
        'type': 'file',
        'version': 2,
        'created': '2017-12-02T12:30:23+00:00',
        'updated': '2017-12-02T12:30:23+00:00',
        'size': 32
    }
