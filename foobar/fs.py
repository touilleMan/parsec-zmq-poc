import json
from marshmallow import fields, validate
from nacl.public import Box

from .utils import UnknownCheckedSchema


class PathOnlySchema(UnknownCheckedSchema):
    path = fields.String(required=True)


class cmd_CREATE_GROUP_MANIFEST_Schema(UnknownCheckedSchema):
    group = fields.String()


class cmd_SHOW_dustbin_Schema(UnknownCheckedSchema):
    path = fields.String(missing=None)


class cmd_HISTORY_Schema(UnknownCheckedSchema):
    first_version = fields.Integer(missing=1, validate=lambda n: n >= 1)
    last_version = fields.Integer(missing=None, validate=lambda n: n >= 1)
    summary = fields.Boolean(missing=False)


class cmd_RESTORE_MANIFEST_Schema(UnknownCheckedSchema):
    version = fields.Integer(missing=None, validate=lambda n: n >= 1)


class cmd_FILE_READ_Schema(UnknownCheckedSchema):
    path = fields.String(required=True)
    offset = fields.Int(missing=0, validate=validate.Range(min=0))
    size = fields.Int(missing=None, validate=validate.Range(min=0))


class cmd_FILE_WRITE_Schema(UnknownCheckedSchema):
    path = fields.String(required=True)
    offset = fields.Int(missing=0, validate=validate.Range(min=0))
    content = fields.Base64Bytes(required=True)


class cmd_FILE_TRUNCATE_Schema(UnknownCheckedSchema):
    path = fields.String(required=True)
    length = fields.Int(required=True, validate=validate.Range(min=0))


class cmd_FILE_HISTORY_Schema(UnknownCheckedSchema):
    path = fields.String(required=True)
    first_version = fields.Int(missing=1, validate=validate.Range(min=1))
    last_version = fields.Int(missing=None, validate=validate.Range(min=1))


class cmd_FILE_RESTORE_Schema(UnknownCheckedSchema):
    path = fields.String(required=True)
    version = fields.Int(required=True, validate=validate.Range(min=1))


class cmd_MOVE_Schema(UnknownCheckedSchema):
    src = fields.String(required=True)
    dst = fields.String(required=True)


class cmd_UNDELETE_Schema(UnknownCheckedSchema):
    vlob = fields.String(required=True)


class FSPipeline:
    def __init__(self, config, auth_user, auth_key):
        self.config = config
        self.auth_user = auth_user
        self.auth_key = auth_key
        self.local_storage = LocalStorage(self)
        self.backend_conn = BackendConnection(self)
        self.user_manifest_svc = UserManifestService(self)
        self.file_svc = FilService(self)

    async def init(self):
        await self.backend_conn.init()
        await self.local_storage.init()
        await self.user_manifest_svc.init()
        await self.file_svc.init()

    async def teardown(self):
        await self.file_svc.teardown()
        await self.user_manifest_svc.teardown()
        await self.local_storage.teardown()
        await self.backend_conn.teardown()

    async def _cmd_FILE_CREATE(self, req):
        req = PathOnlySchema().load(req)
        return {'status': 'ok'}

    async def _cmd_FILE_READ(self, req):
        req = cmd_FILE_READ_Schema().load(req)
        return {'status': 'ok'}

    async def _cmd_FILE_WRITE(self, req):
        req = cmd_FILE_WRITE_Schema().load(req)
        return {'status': 'ok'}

    async def _cmd_STAT(self, req):
        req = PathOnlySchema().load(req)
        return {'status': 'ok'}

    async def _cmd_FOLDER_CREATE(self, req):
        req = PathOnlySchema().load(req)
        return {'status': 'ok'}

    async def _cmd_MOVE(self, req):
        req = cmd_MOVE_Schema().load(req)
        return {'status': 'ok'}

    async def _cmd_DELETE(self, req):
        req = PathOnlySchema().load(req)
        return {'status': 'ok'}

    async def _cmd_FILE_TRUNCATE(self, req):
        req = cmd_FILE_TRUNCATE_Schema().load(req)
        return {'status': 'ok'}

    async def _cmd_SYNCHRONISE(self, req):
        UnknownCheckedSchema().load(req)
        return {'status': 'not_implemented'}


class LocalStorage:
    def __init__(self, app):
        self.app = app

    async def init(self):
        pass

    async def teardown(self):
        pass


class BackendConnection:
    def __init__(self, app):
        self.app = app
        self.is_connected = False

    async def init(self):
        pass

    async def teardown(self):
        pass

    async def ping(self):
        pass


class UserManifestService:
    def __init__(self, app):
        self.app = app
        self.user_manifest_version = 0
        self.user_manifest = {}

    async def init(self):
        raw_user_manifest = await self.app.local_storage.get_user_manifest()
        if raw_user_manifest is not None:
            box = Box(self.app.user_privkey, self.app.user_privkey.public_key)
            user_manifest = json.loads(box.decrypt(raw_user_manifest))
            self.user_manifest_version = user_manifest['version']
            self.user_manifest = user_manifest['root']

    async def teardown(self):
        pass


class FilService:
    def __init__(self, app):
        self.app = app

    async def init(self):
        pass

    async def teardown(self):
        pass
