from supabase import create_client, Client
from app.core.config import settings
from app.core.errors import ConfigError

_client: Client | None = None

def get_supabase() -> Client:
    global _client
    if _client is not None:
        return _client
    if not settings.SUPABASE_URL or not settings.SUPABASE_SERVICE_ROLE_KEY:
        raise ConfigError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")
    _client = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)
    return _client
