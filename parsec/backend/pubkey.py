import attr
from marshmallow import fields

from nacl.public import PublicKey

from parsec.core.utils import UnknownCheckedSchema, ParsecError


class PubKeyError(ParsecError):
    status = 'pubkey_error'


class PubKeyNotFound(PubKeyError):
    status = 'pubkey_not_found'


class cmd_PUBKEY_GET_Schema(UnknownCheckedSchema):
    id = fields.String(required=True)


class BasePubKeyComponent:

    async def api_pubkey_get(self, client_ctx, msg):
        msg = cmd_PUBKEY_GET_Schema().load(msg)
        key = self.get(msg['id'])
        if not key:
            return {'pubkey_not_found', 'No public key for identity `%s`' % msg['id']}
        return {'status': 'ok', 'id': msg['id'], 'key': key}

    # async def api_pubkey_add(self, msg):
    #     msg = cmd_PUBKEY_ADD_Schema().load(msg)
    #     key = await Effect(EPubKeyGet(**msg, raw=True))
    #     return {'status': 'ok', 'id': msg['id'], 'key': key}

    async def add(self, intent):
        raise NotImplementedError()

    async def get(self, intent):
        raise NotImplementedError()


@attr.s
class MockedPubKeyComponent:
    _keys = attr.ib(default=attr.Factory(dict))

    async def add(self, id, key):
        assert isinstance(key, (bytes, bytearray))
        if id in self._keys:
            raise PubKeyError('Identity `%s` already has a public key' % id)
        else:
            self._keys[id] = key

    async def get(self, id, raw=True):
        try:
            key = self._keys[id]
            return key if raw else PublicKey(key)
        except KeyError:
            raise PubKeyNotFound('No public key for identity `%s`' % id)
