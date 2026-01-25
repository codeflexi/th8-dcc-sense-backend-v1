from abc import ABC
from supabase import Client
from app.infra.supabase_client import get_supabase

class BaseRepository(ABC):
    def __init__(self):
        self.sb: Client = get_supabase()
