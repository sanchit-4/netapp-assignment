from abc import ABC, abstractmethod
from typing import IO, Tuple

class StorageAdapter(ABC):
    """Abstract base class for storage adapters."""

    @abstractmethod
    def put(self, key: str, data: IO[bytes]) -> Tuple[str, str]:
        """
        Upload an object to the storage tier.
        
        :param key: The unique identifier for the object.
        :param data: A file-like object containing the data to upload.
        :return: A tuple containing the path and the checksum of the stored object.
        """
        pass

    @abstractmethod
    def get(self, key: str) -> IO[bytes]:
        """
        Retrieve an object from the storage tier.
        
        :param key: The unique identifier for the object.
        :return: A file-like object containing the data.
        """
        pass

    @abstractmethod
    def delete(self, key: str):
        """
        Delete an object from the storage tier.
        
        :param key: The unique identifier for the object.
        """
        pass

    @abstractmethod
    def metadata(self, key: str) -> dict:
        """
        Get metadata about an object.
        
        :param key: The unique identifier for the object.
        :return: A dictionary of metadata.
        """
        pass

    @abstractmethod
    def get_checksum(self, key: str) -> str:
        """
        Get the checksum of an object.
        
        :param key: The unique identifier for the object.
        :return: The checksum string.
        """
        pass
