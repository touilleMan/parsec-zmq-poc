import attr
import json
import pendulum
from marshmallow import fields, validate
from nacl.public import Box
from nacl.secret import SecretBox

from .utils import BaseCmdSchema, from_jsonb64, abort, ParsecError


class InvalidPath(ParsecError):
    status = 'invalid_path'


class PathOnlySchema(BaseCmdSchema):
    path = fields.String(required=True)


class cmd_CREATE_GROUP_MANIFEST_Schema(BaseCmdSchema):
    group = fields.String()


class cmd_SHOW_dustbin_Schema(BaseCmdSchema):
    path = fields.String(missing=None)


class cmd_HISTORY_Schema(BaseCmdSchema):
    first_version = fields.Integer(missing=1, validate=lambda n: n >= 1)
    last_version = fields.Integer(missing=None, validate=lambda n: n >= 1)
    summary = fields.Boolean(missing=False)


class cmd_RESTORE_MANIFEST_Schema(BaseCmdSchema):
    version = fields.Integer(missing=None, validate=lambda n: n >= 1)


class cmd_FILE_READ_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    offset = fields.Int(missing=0, validate=validate.Range(min=0))
    size = fields.Int(missing=None, validate=validate.Range(min=0))


class cmd_FILE_WRITE_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    offset = fields.Int(missing=0, validate=validate.Range(min=0))
    content = fields.Base64Bytes(required=True)


class cmd_FILE_TRUNCATE_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    length = fields.Int(required=True, validate=validate.Range(min=0))


class cmd_FILE_HISTORY_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    first_version = fields.Int(missing=1, validate=validate.Range(min=1))
    last_version = fields.Int(missing=None, validate=validate.Range(min=1))


class cmd_FILE_RESTORE_Schema(BaseCmdSchema):
    path = fields.String(required=True)
    version = fields.Int(required=True, validate=validate.Range(min=1))


class cmd_MOVE_Schema(BaseCmdSchema):
    src = fields.String(required=True)
    dst = fields.String(required=True)


class cmd_UNDELETE_Schema(BaseCmdSchema):
    vlob = fields.String(required=True)


class FSPipeline:
    def __init__(self, app):
        self.app = app
        self.local_storage = LocalStorage(app)
        self.backend_conn = BackendConnection(app)
        self.user_manifest_svc = UserManifestService(app)
        self.file_svc = FilService(app)

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
        path = req['path']
        self.user_manifest_svc.check_path(path, should_exists=True)
        obj = self.user_manifest_svc.retrieve_path(path)
        if obj['type'] == 'folder':
            return {'status': 'ok', 'type': obj['type'], 'children': list(sorted(obj['children'].keys()))}
        else:
            key = from_jsonb64(obj['key'])
            file = await self.file_svc.get_file(obj['id'], obj['read_trust_seed'], obj['write_trust_seed'], key)
            if not file:
                # Data not in local and backend is offline
                abort('unavailable_resource')
            return {
                'status': 'ok',
                'type': 'file',
                'created': file.file_manifest['created'],
                'updated': file.file_manifest['updated'],
                'version': file.file_manifest['version'],
                'size': file.file_manifest['size'],
            }

    async def _cmd_FOLDER_CREATE(self, req):
        req = PathOnlySchema().load(req)
        path = req['path']
        self.user_manifest_svc.check_path(path, should_exists=False)
        dirpath, name = path.rsplit('/', 1)
        dirobj = self.user_manifest_svc.retrieve_path(dirpath)
        now = pendulum.now().isoformat()
        dirobj['children'][name] = {
            'type': 'folder', 'children': {}, 'stat': {'created': now, 'updated': now}}
        return {'status': 'ok'}

    async def _cmd_MOVE(self, req):
        req = cmd_MOVE_Schema().load(req)
        src = req['src']
        dst = req['dst']

        self.user_manifest_svc.check_path(src, should_exists=True)
        self.user_manifest_svc.check_path(dst, should_exists=False)

        srcdirpath, scrfilename = src.rsplit('/', 1)
        dstdirpath, dstfilename = dst.rsplit('/', 1)

        srcobj = self.user_manifest_svc.retrieve_path(srcdirpath)
        dstobj = self.user_manifest_svc.retrieve_path(dstdirpath)
        dstobj['children'][dstfilename] = srcobj['children'][scrfilename]
        del srcobj['children'][scrfilename]
        return {'status': 'ok'}

    async def _cmd_DELETE(self, req):
        req = PathOnlySchema().load(req)
        path = req['path']
        self.user_manifest_svc.check_path(path, should_exists=True)
        dirpath, leafname = path.rsplit('/', 1)
        obj = self.user_manifest_svc.retrieve_path(dirpath)
        del obj['children'][leafname]
        return {'status': 'ok'}

    async def _cmd_FILE_TRUNCATE(self, req):
        req = cmd_FILE_TRUNCATE_Schema().load(req)
        return {'status': 'ok'}

    async def _cmd_SYNCHRONISE(self, req):
        BaseCmdSchema().load(req)
        return {'status': 'not_implemented'}


class LocalStorage:
    def __init__(self, app):
        self.app = app

    async def init(self):
        pass

    async def teardown(self):
        pass

    async def get_block(self, id):
        raise NotImplementedError()

    async def get_file_manifest(self, id, rts, wts, version=None):
        raise NotImplementedError()

    async def get_user_manifest(self, userid, version=None):
        raise NotImplementedError()


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
        raw_user_manifest = await self.app.fs.local_storage.get_user_manifest(self.app.auth_user)
        if raw_user_manifest is not None:
            box = Box(self.app.auth_privkey, self.app.auth_privkey.public_key)
            user_manifest = json.loads(box.decrypt(raw_user_manifest).decode())
            self.user_manifest_version = user_manifest['version']
            self.user_manifest = user_manifest['root']

    async def teardown(self):
        pass

    def check_path(self, path, should_exists=True, type=None):
        if path == '/':
            if not should_exists or type not in ('folder', None):
                raise InvalidPath('Root `/` folder always exists')
            else:
                return
        dirpath, leafname = path.rsplit('/', 1)
        obj = self.retrieve_path(dirpath)
        if obj['type'] != 'folder':
            raise InvalidPath("Path `%s` is not a folder" % path)
        try:
            leafobj = obj['children'][leafname]
            if not should_exists:
                raise InvalidPath("Path `%s` already exist" % path)
            if type is not None and leafobj['type'] != type:
                raise InvalidPath("Path `%s` is not a %s" % (path, type))
        except KeyError:
            if should_exists:
                raise InvalidPath("Path `%s` doesn't exist" % path)

    def retrieve_path(self, path):
        if not path:
            return self.user_manifest
        if not path.startswith('/'):
            raise InvalidPath("Path must start with `/`")
        cur_dir = self.user_manifest
        reps = path.split('/')
        for rep in reps:
            # TODO: drop support for . and .. ?
            if not rep or rep == '.':
                continue
            elif rep == '..':
                cur_dir = cur_dir['parent']
            else:
                try:
                    cur_dir = cur_dir['children'][rep]
                except KeyError:
                    raise InvalidPath("Path `%s` doesn't exist" % path)
        return cur_dir


@attr.s(slots=True)
class File:
    id = attr.ib()
    rts = attr.ib()
    wts = attr.ib()
    key = attr.ib()
    file_manifest = attr.ib()
    patches = attr.ib(default=attr.Factory(list))


class FilService:
    def __init__(self, app):
        self.app = app
        self.files = {}

    async def init(self):
        pass

    async def teardown(self):
        pass

    async def get_file(self, id, rts, wts, key):
        file = self.files.get(id)
        if not file:
            raw_fm = await self.app.fs.local_storage.get_file_manifest(id, rts, wts)
            if raw_fm is None:
                return None
            box = SecretBox(key)
            fm = json.loads(box.decrypt(raw_fm).decode())
            file = self.files[id] = File(id, rts, wts, key, fm)
        return file
