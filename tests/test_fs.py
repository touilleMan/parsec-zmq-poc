import pytest
import json
from trio.testing import trio_test
from nacl.public import Box, PrivateKey
from nacl.secret import SecretBox

from foobar.utils import to_jsonb64, from_jsonb64
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

    bar_txt_id = '1a08acb35bc64ee6acff58b09e1ac939'
    bar_txt_key = b'0\xba\x9fY\xd1\xb4D\x93\r\xf6\xa7[\xe8\xaa\xf9\xeea\xb8\x01\x98\xc1~im}C\xfa\xde\\\xe6\xa1-'
    bar_txt_fms = [
        {
            'version': 1,
            'created': '2017-12-02T12:30:30',
            'updated': '2017-12-02T12:30:30',
            'blocks': [],
            'size': 0
        },
        {
            'version': 2,
            'created': '2017-12-02T12:30:30',
            'updated': '2017-12-02T12:30:45',
            'blocks': [
            ],
            'size': 20
        }
    ]

    foo_spam_txt_id = 'c876f62e7c6348739255834e062f5f31'
    foo_spam_txt_key = b'\xe8\x12\xf5i\xeb]6Py\xb8\xec\x15\x19#\xe61\xd1s\xd5\x98\xbe\xb8\xfa\xda\x0e\x8fk6\xb15\x01\xdf'
    foo_spam_txt_fms = [
         {
            'version': 1,
            'created': '2017-12-02T12:30:50',
            'updated': '2017-12-02T12:30:50',
            'blocks': [],
            'size': 0
        },
    ]

    alice_user_manifest = {
        'version': 0,
        'root': {
            'type': 'folder',
            'children': {
                'foo': {
                    'type': 'folder',
                    'children': {
                        'spam.txt': {
                            'type': 'file',
                            'id': foo_spam_txt_id,
                            'read_trust_seed': '<rts>',
                            'write_trust_seed': '<wts>',
                            'key': to_jsonb64(foo_spam_txt_key)
                        }
                    },
                    'stat': {'created': '2017-12-02T12:30:23', 'updated': '2017-12-02T12:30:23'}
                },
                'bar.txt': {
                    'type': 'file',
                    'id': bar_txt_id,
                    'read_trust_seed': '<rts>',
                    'write_trust_seed': '<wts>',
                    'key': to_jsonb64(bar_txt_key)
                }
            },
            'stat': {'created': '2017-12-02T12:30:23', 'updated': '2017-12-02T12:30:23'}
        }
    }
    box = Box(alice_privkey, alice_privkey.public_key)
    core.mocked_local_storage_cls.user_manifests[aliceid][0] = box.encrypt(json.dumps(alice_user_manifest).encode())

    bar_txt_box = SecretBox(bar_txt_key)
    for fm in bar_txt_fms:
        core.mocked_local_storage_cls.file_manifests[bar_txt_id][fm['version']] = bar_txt_box.encrypt(json.dumps(fm).encode())

    foo_spam_txt_box = SecretBox(foo_spam_txt_key)
    for fm in foo_spam_txt_fms:
        core.mocked_local_storage_cls.file_manifests[foo_spam_txt_id][fm['version']] = foo_spam_txt_box.encrypt(json.dumps(fm).encode())


@trio_test
@with_core()
async def test_stat_folder(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['bar.txt', 'foo'], 'type': 'folder'}
        # Test nested folder as well
        await sock.send({'cmd': 'stat', 'path': '/foo'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['spam.txt'], 'type': 'folder'}


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
        'created': '2017-12-02T12:30:30',
        'updated': '2017-12-02T12:30:45',
        'size': 20
    }
