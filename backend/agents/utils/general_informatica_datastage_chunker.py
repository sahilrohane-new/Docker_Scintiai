from typing import List, Dict

def process_info_string(src: str) -> List[Dict]:
    return [{"id": "blk_001", "code": src.strip()}]