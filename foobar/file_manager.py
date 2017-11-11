import attr
import json
import trio
from uuid import uuid4
import pendulum
from nacl.public import PrivateKey
from nacl.secret import SecretBox
import nacl.utils


def _generate_sym_key():
    return nacl.utils.random(SecretBox.KEY_SIZE)


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
                file.is_ready.set()
                self._files[id] = file
            else:
                # TODO: handle cache miss with async request to backend
                return None
        await file.is_ready.wait()
        return file

    def get_placeholder_file(self, id, key):
        file = self._files.get(id)
        if not file:
            ciphered_data = self.local_storage.get_placeholder_file_manifest(id)
            if ciphered_data:
                file = PlaceHolderFile.load(self, id, key, ciphered_data)
            else:
                # TODO: better exception ?
                raise RuntimeError('Unknown placeholder file `%s`' % id)
            self._files[id] = file
        return file

    def create_placeholder_file(self):
        file, key = PlaceHolderFile.create(self)
        self._files[file.id] = file
        return file, key


class BaseLocalFile:
    async def read(self, size, offset=0):
        raise NotImplementedError()

    def write(self, buffer, offset=0):
        raise NotImplementedError()

    def truncate(self, length):
        raise NotImplementedError()

    def sync(self):
        raise NotImplementedError()


@attr.s(init=False)
class PatchedLocalFileFixture(BaseLocalFile):
    _patches = attr.ib()

    def __init__(self):
        self._patches = []
        for db in self.data['dirty_blocks']:
            self._patches.append(Patch.build_from_dirty_block(self.file_manager, **db))

    async def read(self, size, offset=0):
        self._patches = _merge_patches(self._patches)

    def write(self, buffer, offset=0):
        self._patches.append(Patch(self.file_manager, offset, len(buffer), buffer=buffer))
        if offset + len(buffer) > self.size:
            self.data['size'] = offset + len(buffer)

    def truncate(self, length):
        if self.size < length:
            return
        self.data['size'] = length
        # updated_patches = []
        # for patch_offset, patch_buffer, patch_size in self._patches:
        #     if patch_offset < length:
        #         max_patch_size = length - patch_offset
        #         if patch_size < max_patch_size:
        #             updated_patches.append(patch_offset, patch_buffer, patch_size)
        #         else:
        #             # Need to cut this patch
        #             updated_patches.append(
        #                 patch_offset, patch_buffer[:max_patch_size], max_patch_size)
        # self._patches = updated_patches

    def sync(self):
        # Now is time to clean the patches
        pass
        # for patch in _merge_patches(self._patches):


@attr.s
class LocalFile(PatchedLocalFileFixture, BaseLocalFile):
    file_manager = attr.ib()
    id = attr.ib()
    rts = attr.ib()
    wts = attr.ib()
    box = attr.ib()
    data = attr.ib()
    is_ready = attr.ib(default=attr.Factory(trio.Event))
    _patches = attr.ib(default=attr.Factory(list))

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
        data.setdefault('placeholder_blocks', [])
        return cls(file_manager, id, rts, wts, box, data)

    def dump(self):
        return self.box.encrypt(json.dumps(self.data).encode())

    def sync(self):
        super().sync()
        self.file_manager.local_storage.save_dirty_file_manifest(self.id, self.dump())


@attr.s
class PlaceHolderFile(PatchedLocalFileFixture, BaseLocalFile):
    file_manager = attr.ib()
    id = attr.ib()
    box = attr.ib()
    data = attr.ib()
    _patches = attr.ib(default=attr.Factory(list))

    @data.default
    def _default_data(field):
        now = pendulum.utcnow().isoformat()
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

    def sync(self):
        super().sync()
        self.file_manager.local_storage.save_placeholder_file_manifest(self.id, self.dump())

    @classmethod
    def create(cls, file_manager):
        id = uuid4().hex
        key = _generate_sym_key()
        box = SecretBox(key)
        return cls(file_manager, id, box), key


def _try_merge_two_patches(p1, p2):
    # p2 has priority over p1
    p1offset, p1buffer, p1size = p1
    p2offset, p2buffer, p2size = p2
    if ((p1offset < p2offset and p1offset + p1size < p2offset) or
            (p2offset < p1offset and p2offset + p2size < p1offset)):
        return None
    if p1offset < p2offset:
        newbuffer = p1buffer[:p2offset - p1offset] + p2buffer + p1buffer[p2offset + p2size - p1offset:]
        newsize = len(newbuffer)
        newoffset = p1offset
    else:
        newbuffer = p2buffer + p1buffer[p2offset + p2size - p1offset:]
        newsize = len(newbuffer)
        newoffset = p2offset
    return (newoffset, newbuffer, newsize)


def _merge_patches(patches):
    merged = []
    for p2 in patches:
        new_merged = []
        for p1 in merged:
            res = _try_merge_two_patches(p1, p2)
            if res:
                p2 = res
            else:
                new_merged.append(p1)
        new_merged.append(p2)
        merged = new_merged
    return merged


@attr.s(slots=True)
class Patch:
    file_manager = attr.ib()
    offset = attr.ib()
    size = attr.ib()
    dirty_block_id = attr.ib(default=None)
    _buffer = attr.ib(default=None)

    def get_buffer(self):
        if self._buffer is None:
            if not self.dirty_block_id:
                raise RuntimeError('This patch has no buffer...')
            self._buffer = self.file_manager.local_storage.get_dirty_block(self.dirty_block_id)
        return self._buffer

    def save_as_dirty_block(self):
        if self.dirty_block_id:
            raise RuntimeError('Cannot modify already existing `%s` dirty block' % self.dirty_block_id)
        self.dirty_block_id = uuid4().hex
        key = _generate_sym_key()
        ciphered = SecretBox(key).encrypt(self._buffer)
        self.file_manager.local_storage.save_dirty_block(self.dirty_block_id, ciphered)
        return key

    @classmethod
    def build_from_dirty_block(cls, file_manager, id, offset, size, key):
        box = SecretBox(key)
        return cls(file_manager, offset, size, id, box)
