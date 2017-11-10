import attr
import trio


class FileManager:

    def __init__(self, local_storage):
        self.local_storage = local_storage
        self._files = {}

    async def get_file(self, id, rts, wts, key):
        file = self._files.get(id)
        if not file:
            raw = self.local_storage.get_file_manifest(id)
            if raw:
                file = DirtyFile.load(raw, key)
                self._files[id] = file
            else:
                # TODO: handle cache miss with async request to backend
                return None
        return file

    def get_placeholder_file(self, id):
        pass


class BaseDirtyFile:
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


class DirtyFile(BaseDirtyFile):
    def __init__(self, file_manager, id, rts, wts, key, data):
        super().__init__(file_manager)
        self.data = data
        self.is_ready = trio.Event()
        self.id = id
        self.rts = rts
        self.wts = wts
        self.key = key

    @classmethod
    def load(cls, data, key):
        pass

    def sync(self):
        self.file_manager.local_storage.save_dirty_file_manifest(self.id, )


class PlaceHolderFile(BaseDirtyFile):
    def __init__(self, file_manager, data):
        super().__init__(file_manager)
        self.data = data
