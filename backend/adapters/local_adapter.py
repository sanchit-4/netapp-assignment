import os
import hashlib
from typing import IO, Tuple
from adapters.base import StorageAdapter

class LocalStorageAdapter(StorageAdapter):
    """Adapter for storing files on the local filesystem."""

    def __init__(self, base_path: str = "/local_storage"):
        self.base_path = base_path
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)

    def put(self, key: str, data: IO[bytes]) -> Tuple[str, str]:
        file_path = os.path.join(self.base_path, key)
        md5_hash = hashlib.md5()
        
        content = data.read()
        md5_hash.update(content)
        checksum = md5_hash.hexdigest()

        with open(file_path, 'wb') as f:
            f.write(content)
            
        return file_path, checksum

    def get(self, key: str) -> IO[bytes]:
        file_path = os.path.join(self.base_path, key)
        return open(file_path, 'rb')

    def delete(self, key: str):
        file_path = os.path.join(self.base_path, key)
        os.remove(file_path)

    def metadata(self, key: str) -> dict:
        file_path = os.path.join(self.base_path, key)
        stat = os.stat(file_path)
        return {
            "size_bytes": stat.st_size,
            "last_accessed_at": stat.st_atime,
            "created_at": stat.st_ctime,
        }
    
    def get_checksum(self, key: str) -> str:
        file_path = os.path.join(self.base_path, key)
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()
