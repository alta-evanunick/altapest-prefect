from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class Office:
    office_id: int
    base_url: str
    key_lookup: str
    token_lookup: str


@dataclass(frozen=True)
class EntityMeta:
    entity_name: str
    endpoint: str
    date_fields: List[str]
    is_global: bool
    watermark_field: Optional[str] = None