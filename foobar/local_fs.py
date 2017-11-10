import attr
import json
import pendulum
from uuid import uuid4
from marshmallow import fields, validate
from nacl.public import Box, PrivateKey
from nacl.secret import SecretBox
import nacl.utils

from .local_storage import LocalStorage
from .local_user_manifest import LocalUserManifest, decrypt_and_load_local_user_manifest
from .file_manager import FileManager
from .backend_connection import BackendConnection
from .utils import BaseCmdSchema, from_jsonb64, abort, ParsecError


def _generate_sym_key():
    return nacl.utils.random(SecretBox.KEY_SIZE)


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


class LocalFS:
    def __init__(self, authid, auth_privkey):
        self.authid = authid
        self.auth_privkey = PrivateKey(auth_privkey)
        self.local_storage = LocalStorage()
        self.backend_conn = BackendConnection()
        self.local_user_manifest = None
        self.files_manager = FileManager(self.local_storage)

    async def init(self):
        await self.backend_conn.init()
        raw = self.local_storage.get_local_user_manifest()
        if not raw:
            self.local_user_manifest = LocalUserManifest()
        else:
            self.local_user_manifest = decrypt_and_load_local_user_manifest(
                self.auth_privkey, raw)

    async def teardown(self):
        await self.backend_conn.teardown()

    async def _cmd_FILE_CREATE(self, req):
        req = PathOnlySchema().load(req)
        path = req['path']
        self.user_manifest_svc.check_path(path, should_exists=False)
        now = pendulum.now().isoformat()
        file_manifest = {
            'version': 1,
            'created': now,
            'updated': now,
            'blocks': [],
            'size': 0
        }
        vlob_key = _generate_sym_key()
        box = SecretBox(vlob_key)
        vlob_content = box.encrypt(json.dumps(file_manifest).encode())
        # TODO finish this shit...

        # vlob_access = await self._create_vlob(vlob_content)
        dirpath, name = path.rsplit('/', 1)
        dirobj = self._retrieve_path(dirpath)
        dirobj['children'][name] = {
            'type': 'file',
            'id': vlob_access.id,
            'read_trust_seed': vlob_access.read_trust_seed,
            'write_trust_seed': vlob_access.write_trust_seed,
            'key': to_jsonb64(vlob_key.key)
        }
        await self._commit_manifest()
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
