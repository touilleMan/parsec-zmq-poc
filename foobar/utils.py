import attr
import pickle


BUFFSIZE = 4049


@attr.s
class CookedSocket:
    rawsocket = attr.ib()

    async def send(self, msg):
        await self.rawsocket.sendall(pickle.dumps(msg))

    async def recv(self):
        raw = await self.rawsocket.recv(BUFFSIZE)
        if not raw:
            return None
        return pickle.loads(raw)
