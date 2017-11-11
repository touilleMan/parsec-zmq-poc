import pytest
from trio.testing import trio_test

from tests.common import with_core, with_populated_local_storage


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_stat_folder(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['dir', 'empty_dir']
        }
        # Test nested folder as well
        await sock.send({'cmd': 'stat', 'path': '/dir'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['modified.txt', 'new.txt', 'up_to_date.txt']
        }


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_stat_file(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'stat', 'path': '/dir/up_to_date.txt'})
        rep = await sock.recv()
    assert rep == {
        'status': 'ok',
        'type': 'file',
        'is_dirty': False,
        'is_placeholder': False,
        'version': 2,
        'created': '2017-12-02T12:30:30',
        'updated': '2017-12-02T12:30:45',
        'size': 26
    }


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_create_folder(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'folder_create', 'path': '/new_folder'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure folder is visible
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['dir', 'empty_dir', 'new_folder']
        }
    # Test nested as well
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'folder_create', 'path': '/dir/new_folder'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure folder is visible
        await sock.send({'cmd': 'stat', 'path': '/dir'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['modified.txt', 'new.txt', 'new_folder', 'up_to_date.txt']
        }


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_create_duplicated_folder(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'folder_create', 'path': '/dir'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': 'Path `/dir` already exist'}
        # Try with existing file as well
        await sock.send({'cmd': 'folder_create', 'path': '/dir/up_to_date.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': 'Path `/dir/up_to_date.txt` already exist'}


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_move_folder(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'move', 'src': '/dir', 'dst': '/renamed_dir'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure folder is visible
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['empty_dir', 'renamed_dir'],
        }
        # Make sure folder still contains the same stuff
        await sock.send({'cmd': 'stat', 'path': '/renamed_dir'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['modified.txt', 'new.txt', 'up_to_date.txt']
        }
        # And old folder nam is no longer available
        await sock.send({'cmd': 'stat', 'path': '/dir'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/dir` doesn't exist"}


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_move_folder_bad_dst(core):
    for src in ['/empty_dir', '/dir/up_to_date.txt']:
        async with core.test_connect('alice@test') as sock:
            # Destination already exists
            await sock.send({'cmd': 'move', 'src': src, 'dst': '/dir'})
            rep = await sock.recv()
            assert rep == {'status': 'invalid_path', 'reason': "Path `/dir` already exist"}
            # Destination already exists
            await sock.send({'cmd': 'move', 'src': src, 'dst': '/dir/modified.txt'})
            rep = await sock.recv()
            assert rep == {'status': 'invalid_path', 'reason': "Path `/dir/modified.txt` already exist"}
            # Cannot replace root !
            await sock.send({'cmd': 'move', 'src': src, 'dst': '/'})
            rep = await sock.recv()
            assert rep == {'status': 'invalid_path', 'reason': "Root `/` folder always exists"}
            # Destination contains non-existent folders
            await sock.send({'cmd': 'move', 'src': src, 'dst': '/dir/unknown/new_foo'})
            rep = await sock.recv()
            assert rep == {'status': 'invalid_path', 'reason': "Path `/dir/unknown` doesn't exist"}


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_move_file(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'move', 'src': '/dir/up_to_date.txt', 'dst': '/renamed.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Check the destination exists
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['dir', 'empty_dir', 'renamed.txt']
        }
        # Check the source no longer exits
        await sock.send({'cmd': 'stat', 'path': '/dir'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['modified.txt', 'new.txt']
        }
        # Make sure we can no longer stat source name...
        await sock.send({'cmd': 'stat', 'path': '/dir/up_to_date.txt'})
        rep = await sock.recv()
        assert rep == {
            'status': 'invalid_path',
            'reason': "Path `/dir/up_to_date.txt` doesn't exist"
        }
        # ...and we can stat destination name
        await sock.send({'cmd': 'stat', 'path': '/renamed.txt'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'file',
            'is_dirty': False,
            'is_placeholder': False,
            'version': 2,
            'created': '2017-12-02T12:30:30',
            'updated': '2017-12-02T12:30:45',
            'size': 26
        }


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_move_unknow_file(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'move', 'src': '/dummy.txt', 'dst': '/new_dummy.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/dummy.txt` doesn't exist"}


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_delete_folder(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'delete', 'path': '/empty_dir'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure the folder disappeared
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['dir'],
        }
        await sock.send({'cmd': 'stat', 'path': '/empty_dir'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/empty_dir` doesn't exist"}


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_delete_non_empty_folder(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'delete', 'path': '/dir'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure the folder disappeared
        await sock.send({'cmd': 'stat', 'path': '/'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['empty_dir']
        }
        await sock.send({'cmd': 'stat', 'path': '/dir'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/dir` doesn't exist"}
        # Children should have disappeared as well
        await sock.send({'cmd': 'stat', 'path': '/dir/modified.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/dir` doesn't exist"}


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_delete_file(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'delete', 'path': '/dir/up_to_date.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
        # Make sure the folder disappeared
        await sock.send({'cmd': 'stat', 'path': '/dir'})
        rep = await sock.recv()
        assert rep == {
            'status': 'ok',
            'type': 'folder',
            'created': '2017-12-02T12:30:23',
            'children': ['modified.txt', 'new.txt']
        }
        await sock.send({'cmd': 'stat', 'path': '/dir/up_to_date.txt'})
        rep = await sock.recv()
        assert rep == {
            'status': 'invalid_path',
            'reason': "Path `/dir/up_to_date.txt` doesn't exist"
        }


@trio_test
@with_core()
@with_populated_local_storage('alice')
async def test_delete_unknow_file(core):
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
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'file_create', 'path': '/new.txt'})
        rep = await sock.recv()
        assert rep == {'status': 'invalid_path', 'reason': "Path `/dummy.txt` doesn't exist"}
    raise NotImplementedError()


@pytest.mark.xfail
@trio_test
@with_core()
async def test_create_bad_file(core):
    # Path already exists or within unknown folder
    raise NotImplementedError()
