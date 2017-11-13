import attr
from collections import defaultdict
from marshmallow import fields
from effect2 import TypeDispatcher, Effect

from parsec.base import EEvent
from parsec.backend.session import EGetAuthenticatedUser
from parsec.tools import UnknownCheckedSchema, to_jsonb64


@attr.s
class EMessageNew:
    recipient = attr.ib()
    body = attr.ib()


@attr.s
class EMessageGet:
    offset = attr.ib()


class cmd_NEW_Schema(UnknownCheckedSchema):
    recipient = fields.String(required=True)
    body = fields.Base64Bytes(required=True)


class cmd_GET_Schema(UnknownCheckedSchema):
    offset = fields.Int(missing=0)


async def api_message_new(msg):
    msg = cmd_NEW_Schema().load(msg)
    await Effect(EMessageNew(**msg))
    return {'status': 'ok'}


async def api_message_get(msg):
    msg = cmd_GET_Schema().load(msg)
    messages = await Effect(EMessageGet(**msg))
    offset = msg['offset']
    return {
        'status': 'ok',
        'messages': [{'count': i, 'body': to_jsonb64(msg)}
                     for i, msg in enumerate(messages, offset + 1)]
    }


@attr.s
class InMemoryMessageComponent:
    _messages = attr.ib(default=attr.Factory(lambda: defaultdict(list)))

    async def perform_message_new(self, intent):
        self._messages[intent.recipient].append(intent.body)
        await Effect(EEvent('message_arrived', intent.recipient))

    async def perform_message_get(self, intent):
        id = await Effect(EGetAuthenticatedUser())
        return self._messages[id][intent.offset:]

    def get_dispatcher(self):
        return TypeDispatcher({
            EMessageNew: self.perform_message_new,
            EMessageGet: self.perform_message_get,
        })
