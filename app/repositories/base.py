from abc import ABC
from supabase import Client
from app.infra.supabase_client import get_supabase
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID
from fastapi.encoders import jsonable_encoder

class BaseRepository(ABC):
    def __init__(self,sb):
        self.sb = sb
        
    def _encode(self, payload: dict) -> dict:
        """
        Ensure Supabase never receives:
        - datetime
        - Decimal
        - UUID
        - Pydantic models
        """
        return jsonable_encoder(payload)
    
def json_safe(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, dict):
        return {k: json_safe(x) for k, x in v.items()}
    if isinstance(v, list):
        return [json_safe(x) for x in v]
    return v

