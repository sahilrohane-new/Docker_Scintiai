import re
from typing import List, Dict


_TERMINATOR = re.compile(r'^\s*/\s*$')
_BEGIN      = re.compile(r'\bBEGIN\b', re.I)
_END        = re.compile(r'\bEND\b',   re.I)
_HEADER_RE  = re.compile(
    r'CREATE\s+(OR\s+REPLACE\s+)?\b'
    r'(PACKAGE\s+BODY|PACKAGE|PROCEDURE|FUNCTION|TRIGGER|TYPE)\b',
    re.I
)

def remove_comments(src: str) -> str:
    """
    Remove all multi-line (/* ... */) and single-line (--) comments from PL/SQL code.
    """
    src = re.sub(r'/\*.*?\*/', '', src, flags=re.S)
    src = re.sub(r'--.*',        '', src)
    return src

def split_top_level(src: str) -> List[str]:
    """
    First-pass split on '/' delimiter **only when** not inside BEGIN…END nesting.
    Returns list of top-level blocks (still may be large).
    """
    blocks, buf, depth = [], [], 0
    for ln in src.splitlines():
        if _BEGIN.search(ln): depth += 1
        if _END.search(ln):   depth = max(depth-1, 0)
        buf.append(ln)
        if _TERMINATOR.match(ln) and depth == 0:
            blocks.append('\n'.join(buf).rstrip())
            buf.clear()
    if buf:
        blocks.append('\n'.join(buf).rstrip())
    return blocks

def safe_split(block: str, max_lines:int) -> List[str]:
    """
    If block length > max_lines, split on safe boundaries:
    - only where depth==0
    - prefer line ending with ';' or END; or terminator '/'
    """
    lines = block.splitlines()
    if len(lines) <= max_lines:
        return [block]
    out, buf, depth = [], [], 0
    for ln in lines:
        buf.append(ln)
        if _BEGIN.search(ln): depth += 1
        if _END.search(ln):   depth = max(depth-1, 0)
        long_enough = len(buf) >= max_lines
        is_good_break = depth == 0 and (
            _TERMINATOR.match(ln) or ln.strip().upper().endswith(';')
        )
        if long_enough and is_good_break:
            out.append('\n'.join(buf).rstrip())
            buf = []
    if buf:
        out.append('\n'.join(buf).rstrip())
    return out

def classify(block:str) -> str:
    m = _HEADER_RE.search(block)
    if m:
        return m.group(2).upper().replace(' ', '_')
    if block.lstrip().upper().startswith('BEGIN'):
        return 'ANONYMOUS_BLOCK'
    return 'UNKNOWN'

# public -------------------------------------------------------
def process_plsql_string(src:str, max_lines:int=200) -> List[Dict]:
    clean = remove_comments(src)
    blocks = []
    for top in split_top_level(clean):
        blocks.extend(safe_split(top, max_lines))
    return [{"id":f"blk_{i+1:03}", "code":b} for i,b in enumerate(blocks)]