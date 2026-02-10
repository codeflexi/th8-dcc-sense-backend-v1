from app.repositories.base import BaseRepository
from app.core.config import settings
from app.core.errors import IngestionError

# class DocumentOpenRepository(BaseRepository):
#     def create_signed_url(self, *, storage_key: str, expires_in: int = 3600) -> str:
#         bucket = settings.SUPABASE_STORAGE_BUCKET
#         try:
#             res = self.sb.storage.from_(bucket).create_signed_url(storage_key, expires_in)
#             data = getattr(res, "data", None) or res
#             return data.get("signedURL") or data.get("signedUrl") or data.get("signed_url") or ""
#         except Exception as e:
#             raise IngestionError(f"Create signed URL failed: {e}") from e

class DocumentOpenRepository(BaseRepository):
    
    def __init__(self, sb):
        super().__init__(sb)

    def create_signed_url(self, *, storage_key: str, expires_in: int = 3600) -> str:
        bucket = settings.SUPABASE_STORAGE_BUCKET

        if not storage_key:
            raise IngestionError("storage_key empty")

        try:
            res = self.sb.storage.from_(bucket).create_signed_url(storage_key, expires_in)
            data = getattr(res, "data", None) or res

            url = (
                data.get("signedURL")
                or data.get("signedUrl")
                or data.get("signed_url")
            )

            if not url:
                raise IngestionError(f"Supabase signed url fail: {data}")

            return url

        except Exception as e:
            raise IngestionError(f"Create signed URL failed: {e}") from e
