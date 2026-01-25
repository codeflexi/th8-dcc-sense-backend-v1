from app.repositories.base import BaseRepository
from app.core.config import settings
from app.core.errors import IngestionError

class StorageRepository(BaseRepository):
    def upload_bytes(self, *, storage_key: str, data: bytes, content_type: str) -> dict:
        bucket = settings.SUPABASE_STORAGE_BUCKET
        try:
            res = self.sb.storage.from_(bucket).upload(
                path=storage_key,
                file=data,
                file_options={"content-type": content_type, "upsert": "true"},
            )
            return {"bucket": bucket, "path": storage_key, "result": getattr(res, "data", None) or res}
        except Exception as e:
            raise IngestionError(f"Storage upload failed: {e}") from e

    def download_bytes(self, *, storage_key: str) -> bytes:
        bucket = settings.SUPABASE_STORAGE_BUCKET
        return self.sb.storage.from_(bucket).download(storage_key)
