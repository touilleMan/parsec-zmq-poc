import attr
import json
import trio
import pendulum
from nacl.public import PrivateKey
from nacl.secret import SecretBox


class FileManager:

    def __init__(self, local_storage):
        self.local_storage = local_storage
        self._files = {}

    async def get_file(self, id, rts, wts, key):
        file = self._files.get(id)
        if not file:
            ciphered_data = self.local_storage.get_file_manifest(id)
            if ciphered_data:
                file = LocalFile.load(self, id, rts, wts, key, ciphered_data)
                self._files[id] = file
            else:
                # TODO: handle cache miss with async request to backend
                return None
        return file

    def get_placeholder_file(self, id, key):
        file = self._files.get(id)
        if not file:
            ciphered_data = self.local_storage.get_placeholder_file_manifest(id)
            if ciphered_data:
                file = PlaceHolderFile.load(self, id, key, ciphered_data)
                self._files[id] = file
            else:
                # TODO: handle cache miss with async request to backend
                return None
        return file


class BaseLocalFile:
    def __init__(self, file_manager):
        self.file_manager = file_manager
        self.patches = []

    async def read(self, size, offset=0):
        raise NotImplementedError()

    def write(self, buffer, offset=0):
        self.patches.append((offset, buffer, len(buffer)))

    def truncate(self, length):
        updated_patches = []
        for patch_offset, patch_buffer, patch_size in self.patches:
            if patch_offset < length:
                max_patch_size = length - patch_offset
                if patch_size < max_patch_size:
                    updated_patches.append(patch_offset, patch_buffer, patch_size)
                else:
                    # Need to cut this patch
                    updated_patches.append(
                        patch_offset, patch_buffer[:max_patch_size], max_patch_size)
        self.patches = updated_patches

    def sync(self):
        raise NotImplementedError()


@attr.s
class LocalFile(BaseLocalFile):
    file_manager = attr.ib()
    id = attr.ib()
    rts = attr.ib()
    wts = attr.ib()
    box = attr.ib()
    data = attr.ib()
    is_ready = attr.ib(default=attr.Factory(trio.Event))

    @property
    def created(self):
        return self.data['created']

    @property
    def updated(self):
        return self.data['updated']

    @property
    def version(self):
        return self.data['version']

    @property
    def size(self):
        return self.data['size']

    @property
    def is_dirty(self):
        return self.data.get('is_dirty', False)

    @classmethod
    def load(cls, file_manager, id, rts, wts, key, ciphered_data):
        box = SecretBox(key)
        data = json.loads(box.decrypt(ciphered_data).decode())
        return cls(file_manager, id, rts, wts, box, data)

    def dump(self):
        return self.box.encrypt(json.dumps(self.data).encode())

    def sync(self):
        self.file_manager.local_storage.save_dirty_file_manifest(self.id, ciphered_data)


class PlaceHolderFile(BaseLocalFile):
    file_manager = attr.ib()
    id = attr.ib()
    box = attr.ib()
    data = attr.ib()

    @data.default
    def _default_data():
        now = pendulum.now().isoformat()
        return {
            'created': now,
            'updated': now,
            'version': 0,
            'size': 0,
            'blocks': [],
            'placeholder_blocks': []
        }

    @property
    def created(self):
        return self.data['created']

    @property
    def updated(self):
        return self.data['updated']

    @property
    def version(self):
        return self.data['version']

    @property
    def size(self):
        return self.data['size']

    @property
    def is_dirty(self):
        return True

    @classmethod
    def load(cls, file_manager, id, key, ciphered_data):
        box = SecretBox(key)
        data = json.loads(box.decrypt(ciphered_data).decode())
        return cls(file_manager, id, box, data)

    def dump(self):
        return self.box.encrypt(json.dumps(self.data).encode())
