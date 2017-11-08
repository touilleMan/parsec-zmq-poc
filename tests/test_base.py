import pytest
import trio
from trio.testing import trio_test

from foobar.main import CoreApp
from foobar.utils import CookedSocket


def with_core(testfunc):
    async def wrapper(*args, **kwargs):
        app = CoreApp()

        async def run_test_and_cancel_scope(nursery):
            await testfunc(app)
            nursery.cancel_scope.cancel()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(app.run)
            await app.server_ready.wait()
            nursery.start_soon(run_test_and_cancel_scope, nursery)

    return wrapper


@trio_test
@with_core
async def test_connection(app):
    with trio.socket.socket() as sock:
        await sock.connect(('127.0.0.1', 9999))
        sock = CookedSocket(sock)
        await sock.send({'cmd': 'get_core_state'})
        rep = await sock.recv()
        assert rep == {'status': 'ok'}
