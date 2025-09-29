# backend/agents/utils/generic_sql_chunker.py
"""Pass-through chunker: returns the whole script as one chunk."""

from typing import List, Dict

def process_sql_string(src: str) -> List[Dict]:
    return [{"id": "blk_001", "code": src.strip()}]
