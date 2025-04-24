from pydantic import BaseModel
from typing import Optional, List

class StoreConfig(BaseModel):
    enabled_languages: List[str] = ["es", "en", "pt"]
    modify_languages: List[str] = ["es"]

DEFAULT_CONFIG = StoreConfig()