import pytest
from unittest.mock import Mock

from foobar.file_manager import PlaceHolderFile, _merge_patches, _try_merge_two_patches


@pytest.mark.parametrize('data', [
    {
        'label': 'No patches',
        'patches': [],
        'expected': []
    },
    {
        'label': 'Contiguous patches',
        'patches': [(0, 'hello '), (6, 'world !')],
        'expected': [(0, 'hello world !')]
    },
    {
        'label': 'Non-contiguous patches',
        'patches': [(0, 'hello'), (10, 'world !')],
        'expected': [(0, 'hello'), (10, 'world !')]
    },
    {
        'label': 'Overwrite single patch',
        'patches': [(0, 'hello '), (6, 'world !'), (6, 'SPARTAAAA !')],
        'expected': [(0, 'hello SPARTAAAA !')]
    },
    {
        'label': 'Overwrite multiple patches',
        'patches': [(0, 'hello '), (6, 'world'), (11, ' !'), (6, 'SPARTAAAA !')],
        'expected': [(0, 'hello SPARTAAAA !')]
    },
    {
        'label': 'Cascade overwrite patches',
        'patches': [(0, 'hello '), (6, 'world'), (11, ' !'), (6, 'SPARTAAAA !')],
        'expected': [(0, 'hello SPARTAAAA !')]
    },
], ids=lambda x: x['label'])
def test_merge_patches(data):
    patches = [(offset, buffer, len(buffer)) for offset, buffer in data['patches']]
    expected = [(offset, buffer, len(buffer)) for offset, buffer in data['expected']]
    merged = _merge_patches(patches)
    assert merged == expected


@pytest.mark.parametrize('data', [
    {
        'label': 'Contiguous patches',
        'patches': [(0, 'hello ', 6), (6, 'world !', 7)],
        'expected': (0, 'hello world !', 13)
    },
    {
        'label': 'Non-contiguous patches',
        'patches': [(0, 'hello', 5), (10, 'world !', 7)],
        'expected': None
    },
    {
        'label': 'P1 in P2',
        'patches': [(3, 'abc', 3), (0, '123456789', 9)],
        'expected': (0, '123456789', 9)
    },
    {
        'label': 'P2 in P1',
        'patches': [(0, '123456789', 9), (3, 'abc', 3)],
        'expected': (0, '123abc789', 9)
    },
    {
        'label': "P1 on P2's left",
        'patches': [(0, 'abcdef', 6), (3, '456789', 6)],
        'expected': (0, 'abc456789', 9)
    },
    {
        'label': "P1 on P2's right",
        'patches': [(3, 'defghi', 6), (0, '123456', 6)],
        'expected': (0, '123456ghi', 9)
    },
    {
        'label': "Same size, same pos",
        'patches': [(3, 'def', 3), (3, '456', 3)],
        'expected': (3, '456', 3)
    },
], ids=lambda x: x['label'])
def test_try_merge_two_patches(data):
    ret = _try_merge_two_patches(*data['patches'])
    assert ret == data['expected']


class TestPlaceHolderFile:
    def test_base(self):
        mocked_file_manager = Mock()
        phf = PlaceHolderFile.create(mocked_file_manager)
        out = phf.read()
        assert out == b''
