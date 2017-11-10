import pytest
import json
from collections import defaultdict
from trio.testing import trio_test
from nacl.public import Box, PrivateKey
from nacl.secret import SecretBox
import attr
from unittest.mock import patch

from foobar.local_fs import LocalFS
from foobar.local_user_manifest import LocalUserManifest
from foobar.utils import to_jsonb64, from_jsonb64
from tests.common import with_core, TEST_USERS


@attr.s
class User:
    id = attr.ib()
    privkey = attr.ib()

    @property
    def pubkey(self):
        return self.privkey.public_key


@pytest.fixture
def alice():
    return User('alice@test', TEST_USERS['alice@test'])


def _populate_mocked_local_storage_for_alice(mocked_local_storage_cls):
    """
    Generated tree:
    /
    /dir/ <= directory
    /dir/up_to_date.txt <= regular file
    /dir/modified.txt <= regular file with local modifications
    /dir/new.txt <== placeholder file
    """

    aliceid = 'alice@test'
    alice_privkey = PrivateKey(TEST_USERS[aliceid])

    # Hold my beer...

    # /dir/up_to_date.txt - Blocks
    up_to_date_txt_block_1_id = '505b0bef5dd44763abc9eac03c765bc3',
    up_to_date_txt_block_1_key = b'\xec\x1d\x84\x80\x05\x18\xb0\x8a\x1d\x81\xe0\xdb\xe5%wx\x9f\x7f\x01\xa6\x8f#>\xc5]\xae|\xfd\x1d\xc22\x05'
    mocked_local_storage_cls.blocks[up_to_date_txt_block_1_id] = SecretBox(up_to_date_txt_block_1_key).encrypt(b'Hello from')
    up_to_date_txt_block_2_id = '0187fa3fc8a5480cbb3ef9df5dd2b7e9'
    up_to_date_txt_block_2_key = b'\xae\x85y\xdd:\xae\xa6\xf2\xdf\xce#U\x17\xffa\xde\x19\x1d\xa7\x84[\xb8\x92{$6\xf9\xc4\x8b\xbcT\x14'
    mocked_local_storage_cls.blocks[up_to_date_txt_block_2_id] = SecretBox(up_to_date_txt_block_2_key).encrypt(b'up_to_date.txt !')

    # /dir/up_to_date.txt - File manifest

    up_to_date_txt_id = '1a08acb35bc64ee6acff58b09e1ac939'
    up_to_date_txt_key = b'0\xba\x9fY\xd1\xb4D\x93\r\xf6\xa7[\xe8\xaa\xf9\xeea\xb8\x01\x98\xc1~im}C\xfa\xde\\\xe6\xa1-'
    up_to_date_txt_fms = {
        'version': 2,
        'created': '2017-12-02T12:30:30',
        'updated': '2017-12-02T12:30:45',
        'blocks': [
            {'id': up_to_date_txt_block_1_id, 'key': to_jsonb64(up_to_date_txt_block_1_key), 'offset': 0},
            {'id': up_to_date_txt_block_2_id, 'key': to_jsonb64(up_to_date_txt_block_2_key), 'offset': 10},
        ],
        'size': 26
    }
    mocked_local_storage_cls.file_manifests[up_to_date_txt_id] = SecretBox(up_to_date_txt_key).encrypt(json.dumps(up_to_date_txt_fms).encode())

    # /dir/up_to_date.txt - No dirty blocks (the file is up to date...)
    # /dir/up_to_date.txt - No dirty file manifest (the file is up to date...)

    # /dir/modified.txt - Blocks

    modified_txt_block_1_id = '973a198b344d403888472e17b610a43e'
    modified_txt_block_1_key = b'\xc7|\xd7+\xe5\xfbv\xd2\x8c0\xea\r\xff{;2\x0f\xb8s-H\xfd\xfb\xd4\xa157\x86\xde<3\xaa'
    mocked_local_storage_cls.blocks[modified_txt_block_1_id] = SecretBox(modified_txt_block_1_key).encrypt(b'This is version 1.')

    # /dir/modified.txt - File manifest
    # This file manifest shoudl be shadowed by the dirty file manifest

    modified_txt_id = 'ba3d58e140ca44bc91cd53745961397d'
    modified_txt_key = b'0\xba\x9fY\xd1\xb4D\x93\r\xf6\xa7[\xe8\xaa\xf9\xeea\xb8\x01\x98\xc1~im}C\xfa\xde\\\xe6\xa1-'
    modified_txt_fms = {
        'version': 2,
        'created': '2017-12-02T12:50:30',
        'updated': '2017-12-02T12:50:45',
        'blocks': [
            {'id': modified_txt_block_1_id, 'key': to_jsonb64(modified_txt_block_1_key), 'offset': 0},
        ],
        'size': 18
    }
    mocked_local_storage_cls.file_manifests[modified_txt_id] = SecretBox(modified_txt_key).encrypt(json.dumps(modified_txt_fms).encode())

    # /dir/modified.txt - Dirty blocks

    modified_txt_dirty_block_1_id = 'a38646aabf264f4fb9db0f636c4999a7'
    modified_txt_dirty_block_1_key = b'=\x04\xc8\x1d\xb6\xb2\x0c\xbf\xaf\xee\x04%zk\x12\xa4\xed\xda\\\xf5\x95\xa1\xf6\x99\x965G|\xca;\x8e\x05'
    mocked_local_storage_cls.dirty_blocks[modified_txt_dirty_block_1_id] = SecretBox(modified_txt_dirty_block_1_key).encrypt(b'SPARTAAAA !')

    # /dir/modified.txt - Dirty file manifest

    modified_txt_dirty_fm = {
        'base_version': 2,
        'created': '2017-12-02T12:50:30',
        'updated': '2017-12-02T12:51:00',
        'blocks': [
            {'id': modified_txt_block_1_id, 'key': to_jsonb64(modified_txt_block_1_key), 'offset': 0},
        ],
        'dirty_blocks': [
            {'id': modified_txt_dirty_block_1_id, 'key': to_jsonb64(modified_txt_dirty_block_1_key), 'offset': 16}
        ],
        'size': 27
    }
    mocked_local_storage_cls.dirty_file_manifests[modified_txt_id] = SecretBox(modified_txt_key).encrypt(json.dumps(modified_txt_dirty_fm).encode())

    # /dir/new.txt - No blocks (given the file is a placeholder so far)
    # /dir/new.txt - No file manifest (given the file is a placeholder so far)
    # /dir/new.txt - Dirty blocks

    new_txt_dirty_block_1_id = 'faa4e1068dad47b4a758a73102478388'
    new_txt_dirty_block_1_key = b'\xab\xcfn\xc8*\xe8|\xc42\xf2\xfao\x1b\xc1Xm\xb4\xb9JBe\x9a1W\r(\xcc\xbd1\x12RB'
    mocked_local_storage_cls.dirty_blocks[new_txt_dirty_block_1_id] = SecretBox(new_txt_dirty_block_1_key).encrypt(b'Welcome to')

    new_txt_dirty_block_2_id = '4c5b4338a47c462098d6c98856f5bf56'
    new_txt_dirty_block_2_key = b'\xcb\x1c\xe4\x80\x8d\xca\rl?z\xa4\x82J7\xc5\xd5\xed5^\xb6\x05\x8cR;A\xbd\xb1 \xbd\xc2?\xe9'
    mocked_local_storage_cls.dirty_blocks[new_txt_dirty_block_2_id] = SecretBox(new_txt_dirty_block_2_key).encrypt(b'the new file."')

    # /dir/new.txt - No dirty file manifest (you know the reason...)
    # /dir/new.txt - Placeholder file manifest

    new_txt_placeholder_id = '3ca6cb2ba8a9446f8508296b7a8c3ed4'
    new_txt_placeholder_key = b'"\x08"Q\xfbc\xa3 \xf9\xde\xbf\xc3\x07?\x9a\xa6V\xcet\x0c\xa1C\xf2\xa06\xa1\xc9 \xbf\xf6t\xbb'
    new_txt_placeholder_fm = {
        'base_version': 2,
        'created': '2017-12-02T12:50:30',
        'updated': '2017-12-02T12:51:00',
        'blocks': [
        ],
        'dirty_blocks': [
            {'id': new_txt_dirty_block_1_id, 'key': to_jsonb64(new_txt_dirty_block_1_key), 'offset': 0},
            {'id': new_txt_dirty_block_2_id, 'key': to_jsonb64(new_txt_dirty_block_2_key), 'offset': 10}
        ],
        'size': 23
    }
    mocked_local_storage_cls.placeholder_file_manifests[new_txt_placeholder_id] = SecretBox(new_txt_placeholder_key).encrypt(json.dumps(new_txt_placeholder_fm).encode())


    # Finally, create the dirty user manifest

    local_user_manifest = {
        'base_version': 3,
        'is_dirty': True,
        'file_placeholders': [new_txt_placeholder_id],
        'tree': {
            'type': 'folder',
            'children': {
                'dir': {
                    'type': 'folder',
                    'children': {

                        'new.txt': {
                            'type': 'placeholder_file',
                            'id': new_txt_placeholder_id,
                            'key': to_jsonb64(new_txt_placeholder_key)
                        },
                        'up_to_date.txt': {
                            'type': 'file',
                            'id': up_to_date_txt_id,
                            'read_trust_seed': '<rts>',
                            'write_trust_seed': '<wts>',
                            'key': to_jsonb64(up_to_date_txt_key)
                        },
                        'modified.txt': {
                            'type': 'file',
                            'id': modified_txt_id,
                            'read_trust_seed': '<rts>',
                            'write_trust_seed': '<wts>',
                            'key': to_jsonb64(modified_txt_key)
                        }

                    },
                    'stat': {'created': '2017-12-02T12:30:23', 'updated': '2017-12-02T12:30:23'}
                },
            },
            'stat': {'created': '2017-12-02T12:30:23', 'updated': '2017-12-02T12:30:23'}
        }
    }
    box = Box(alice_privkey, alice_privkey.public_key)
    mocked_local_storage_cls.local_user_manifest = box.encrypt(json.dumps(local_user_manifest).encode())



def mocked_local_storage_cls_factory():
    # LocalStorage should store on disk, but faster and easier to do that
    # in memory during tests
    class MockedLocalStorage:
        # Can be changed before initialization (that's why we use a factory btw)
        blocks = {}
        dirty_blocks = {}
        dirty_file_manifests = {}
        placeholder_file_manifests = {}
        file_manifests = defaultdict(dict)
        local_user_manifest = None

        def get_block(self, id):
            return self.blocks.get(id)

        def get_file_manifest(self, id, version=None):
            fm = self.file_manifests.get(id)
            if not fm:
                return None
            if version is not None:
                return fm.get(version)
            else:
                return fm[sorted(fm)[-1]]

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

    return MockedLocalStorage


@trio_test
async def test_init_local_fs(alice):
    local_storage_cls = mocked_local_storage_cls_factory()
    _populate_mocked_local_storage_for_alice(local_storage_cls)
    with patch('foobar.local_fs.LocalStorage', local_storage_cls):
        fs = LocalFS(alice.id, alice.privkey)
        await fs.init()
        assert isinstance(fs.local_user_manifest, LocalUserManifest)
        assert fs.local_user_manifest.is_dirty
        assert sorted(fs.local_user_manifest.tree['children']['dir']['children'].keys()) == [
            'modified.txt', 'new.txt', 'up_to_date.txt']


def test_load_local_user_manifest():
    local_storage_cls = mocked_local_storage_cls_factory()
    _populate_mocked_local_storage_for_alice(local_storage_cls)
    local_storage = local_storage_cls()

    # from foobar.local_user_manifest import load_local_user_manifest, LocalUserManifest
    # raw_dum = local_storage.get_local_user_manifest()
    # alice_privkey = TEST_USERS['alice@test']
    # dum = load_local_user_manifest(alice_privkey, raw_dum)
    # assert isinstance(dum, LocalUserManifest)
    # assert dum.is_dirty


class TestFileManage:
    def test_base(self):
        local_storage = mocked_local_storage_cls_factory()
        # file_manager = FileManager(local_storage)
