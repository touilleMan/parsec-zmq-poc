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
    """
    Generated tree:
    /
    /bar.txt
    /foo/
    /foo/spam.txt
    /grok/
    """

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
                'grok': {
                    'type': 'folder',
                    'children': {},
                    'stat': {'created': '2017-12-02T12:30:55', 'updated': '2017-12-02T12:30:55'}
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
        assert rep == {'status': 'ok', 'children': ['bar.txt', 'foo', 'grok'], 'type': 'folder'}
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


@trio_test
@with_core()
async def test_create_folder(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'folder_create', 'path': '/new_folder'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure folder is visible
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['bar.txt', 'foo', 'grok', 'new_folder'], 'type': 'folder'}
    # Test nested as well
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'folder_create', 'path': '/foo/new_folder'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure folder is visible
        await sock.send({'cmd': 'stat', 'path': '/foo'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['new_folder', 'spam.txt'], 'type': 'folder'}


@trio_test
@with_core()
async def test_create_duplicated_folder(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'folder_create', 'path': '/foo'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': 'Path `/foo` already exist'}
        # Try with existing file as well
        await sock.send({'cmd': 'folder_create', 'path': '/bar.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': 'Path `/bar.txt` already exist'}


@trio_test
@with_core()
async def test_move_folder(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'move', 'src': '/foo', 'dst': '/new_foo'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure folder is visible
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['bar.txt', 'grok', 'new_foo'], 'type': 'folder'}
        # Make sure folder still contains the same stuff
        await sock.send({'cmd': 'stat', 'path': '/new_foo'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['spam.txt'], 'type': 'folder'}
        # And old folder nam is no longer available
        await sock.send({'cmd': 'stat', 'path': '/foo'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/foo` doesn't exist"}


@trio_test
@with_core()
async def test_move_folder_bad_dst(core):
    _populate_local_storage_for_alice(core)
    for src in ['/foo', '/bar.txt']:
        async with core.test_connect('alice@test') as sock:
            # Destination already exists
            await sock.send({'cmd': 'move', 'src': src, 'dst': '/grok'})
            rep = await sock.recv()
            assert rep == {'status': 'invalid_path', 'reason': "Path `/grok` already exist"}
            # Destination already exists
            await sock.send({'cmd': 'move', 'src': src, 'dst': '/bar.txt'})
            rep = await sock.recv()
            assert rep == {'status': 'invalid_path', 'reason': "Path `/bar.txt` already exist"}
            # Cannot replace root !
            await sock.send({'cmd': 'move', 'src': src, 'dst': '/'})
            rep = await sock.recv()
            assert rep == {'status': 'invalid_path', 'reason': "Root `/` folder always exists"}
            # Destination contains non-existent folders
            await sock.send({'cmd': 'move', 'src': src, 'dst': '/grok/unknown/new_foo'})
            rep = await sock.recv()
            assert rep == {'status': 'invalid_path', 'reason': "Path `/grok/unknown` doesn't exist"}


@trio_test
@with_core()
async def test_move_file(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'move', 'src': '/foo/spam.txt', 'dst': '/new_spam.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Check the destination exists
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['bar.txt', 'foo', 'grok', 'new_spam.txt'], 'type': 'folder'}
        # Check the source no longer exits
        await sock.send({'cmd': 'stat', 'path': '/foo'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': [], 'type': 'folder'}
        # Make sure we can no longer stat source name...
        await sock.send({'cmd': 'stat', 'path': '/foo/spam.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/foo/spam.txt` doesn't exist"}
        # ...and we can stat destination name
        await sock.send({'cmd': 'stat', 'path': '/new_spam.txt'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'file',
            'version': 1,
            'created': '2017-12-02T12:30:50',
            'updated': '2017-12-02T12:30:50',
            'size': 0
        }


@trio_test
@with_core()
async def test_move_unknow_file(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'move', 'src': '/dummy.txt', 'dst': '/new_dummy.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/dummy.txt` doesn't exist"}


@trio_test
@with_core()
async def test_delete_folder(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'delete', 'path': '/grok'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure the folder disappeared
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['bar.txt', 'foo'], 'type': 'folder'}
        await sock.send({'cmd': 'stat', 'path': '/grok'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/grok` doesn't exist"}


@trio_test
@with_core()
async def test_delete_non_empty_folder(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'delete', 'path': '/foo'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure the folder disappeared
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['bar.txt', 'grok'], 'type': 'folder'}
        await sock.send({'cmd': 'stat', 'path': '/foo'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/foo` doesn't exist"}
        # Children should have disappeared as well
        await sock.send({'cmd': 'stat', 'path': '/foo/spam.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/foo` doesn't exist"}


@trio_test
@with_core()
async def test_delete_file(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'delete', 'path': '/bar.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure the folder disappeared
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {'status': 'ok', 'children': ['foo', 'grok'], 'type': 'folder'}
        await sock.send({'cmd': 'stat', 'path': '/bar.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/bar.txt` doesn't exist"}


@trio_test
@with_core()
async def test_delete_unknow_file(core):
    _populate_local_storage_for_alice(core)
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'delete', 'path': '/dummy.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/dummy.txt` doesn't exist"}


@pytest.mark.xfail
@trio_test
@with_core()
async def test_read(core):
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_read_with_offset(core):
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_read_bad_file(core):
    # Try read bad path and folder
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_write(core):
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_write_with_offset(core):
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_write_bad_file(core):
    # Try write in bad path and folder
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_write_bad_file(core):
    # Try write in bad path and folder
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_truncate(core):
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_truncate_bad_file(core):
    # Try truncate in bad path and folder
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_create_file(core):
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_create_bad_file(core):
    # Path already exists or within unknown folder
    raise NotImplementedError()
