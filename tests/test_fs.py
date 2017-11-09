import pytest
from trio.testing import trio_test

from tests.common import with_core


@trio_test
@with_core()
async def test_connection(core):
    async with core.test_connect('alice@test') as sock:
        await sock.send({'cmd': 'get_core_state'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
