from pathlib import Path
from typing import List, Dict
import re
import pandas as pd

# Define enhanced chunking logic
def remove_comments(code: str) -> str:
    code = re.sub(r"/\*.*?\*/", "", code, flags=re.DOTALL)
    code = re.sub(r"^\s*\*.*?;", "", code, flags=re.MULTILINE)
    return code.strip()


def extract_macros(code: str) -> List[Dict]:
    pattern = re.compile(r"(%macro\b.*?%mend\s*;)", re.IGNORECASE | re.DOTALL)
    blocks = []
    for match in pattern.finditer(code):
        blocks.append({
            "type": "macro",
            "start": match.start(),
            "end": match.end(),
            "code": match.group(1)
        })
    return blocks


def split_excluding_macros(code: str, macros: List[Dict]) -> List[str]:
    chunks = []
    last_end = 0
    for m in macros:
        if last_end < m["start"]:
            non_macro = code[last_end:m["start"]].strip()
            if non_macro:
                chunks.append(non_macro)
        chunks.append(m["code"])
        last_end = m["end"]
    if last_end < len(code):
        remainder = code[last_end:].strip()
        if remainder:
            chunks.append(remainder)
    return chunks


def smart_chunk_block(block: str, max_lines: int = 300) -> List[Dict]:
    lines = block.splitlines()
    sub_chunks = []
    current_chunk = []
    current_type = "unknown"
    safe_keywords = re.compile(r"^\s*(data|proc|%let|call\s+symput|options|%if|%do|%macro)\b", re.IGNORECASE)

    def flush_chunk():
        if current_chunk:
            chunk_text = "\n".join(current_chunk).strip()
            sub_chunks.append({
                "type": current_type,
                "code": chunk_text
            })

    for line in lines:
        match = safe_keywords.match(line)
        if match:
            flush_chunk()
            current_type = match.group(1).lower()
            current_chunk = []
        current_chunk.append(line)

    flush_chunk()
    return sub_chunks


def chunk_sas_code_v3(code: str, max_macro_lines: int = 300) -> List[Dict]:
    cleaned = remove_comments(code)
    macros = extract_macros(cleaned)
    chunks = split_excluding_macros(cleaned, macros)

    final_chunks = []
    for chunk in chunks:
        if chunk.lower().strip().startswith("%macro"):
            line_count = chunk.count("\n")
            if line_count <= max_macro_lines:
                final_chunks.append({"type": "macro", "code": chunk})
            else:
                final_chunks.extend(smart_chunk_block(chunk))
        else:
            final_chunks.extend(smart_chunk_block(chunk))

    for idx, chunk in enumerate(final_chunks, 1):
        chunk["id"] = f"blk_{idx:03d}"

    return final_chunks

def save_chunks_to_csv(chunks: List[Dict], path: str | Path):
    import csv
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id", "type", "code"],
            quoting=csv.QUOTE_ALL   # keep commas/newlines intact
        )
        writer.writeheader()
        writer.writerows(chunks)
