import pyparsing as pp
import networkx as nx
import pandas as pd
import re
from pathlib import Path
import csv
def remove_comments(sas_code: str) -> str:
    sas_code = re.sub(r"/\*.*?\*/", "", sas_code, flags=re.DOTALL)  # Remove block comments
    sas_code = re.sub(r"^\s*\*.*?;", "", sas_code, flags=re.MULTILINE)  # Remove inline comments
    return sas_code

def define_sas_parser():
    macro_start = pp.CaselessKeyword("%MACRO") + pp.Word(pp.alphas + "_") + pp.restOfLine
    macro_end   = pp.CaselessKeyword("%MEND") + pp.Optional(pp.Word(pp.alphas + "_")) + ";"
    proc_start  = pp.CaselessKeyword("PROC") + pp.Word(pp.alphas) + pp.restOfLine
    proc_end    = pp.CaselessKeyword("RUN") + ";"
    proc_sql_end= pp.CaselessKeyword("QUIT") + ";"
    data_start  = pp.CaselessKeyword("DATA") + pp.Word(pp.alphas + "_") + pp.restOfLine

    macro_body  = pp.originalTextFor(pp.SkipTo(macro_end, include=True))
    data_body   = pp.originalTextFor(pp.SkipTo(proc_end | proc_sql_end | macro_end, include=True)) + proc_end
    proc_body   = pp.originalTextFor(pp.SkipTo(proc_end | proc_sql_end | macro_end, include=True)) + (proc_end | proc_sql_end)

    macro      = macro_start + macro_body + macro_end
    data_step  = data_start + data_body
    proc_step  = proc_start + proc_body

    return macro | data_step | proc_step

def parse_sas_code(sas_code: str) -> list[str]:
    sas_code = remove_comments(sas_code)
    parser = define_sas_parser()
    parsed_blocks = parser.searchString(sas_code)
    return [match[0] for match in parsed_blocks if match] or [sas_code]

def chunk_large_blocks(chunks: list[str], max_chunk_size: int) -> list[str]:
    sub_chunks = []
    for chunk in chunks:
        lines = chunk.split("\n")
        temp_chunk = []
        for line in lines:
            temp_chunk.append(line)
            if len(temp_chunk) >= max_chunk_size and line.strip().upper().endswith(("RUN;", "QUIT;", "%MEND;")):
                sub_chunks.append("\n".join(temp_chunk))
                temp_chunk = []
        if temp_chunk:
            sub_chunks.append("\n".join(temp_chunk))
    return sub_chunks

def build_dependency_graph(chunks: list[str]) -> nx.DiGraph:
    dag = nx.DiGraph()
    macro_references = {}
    for i, chunk in enumerate(chunks):
        dag.add_node(i, code=chunk)
        if "%MACRO" in chunk and "%MEND" in chunk:
            macro_name = re.search(r'%MACRO\s+(\w+)', chunk, re.IGNORECASE)
            if macro_name:
                macro_references[macro_name.group(1)] = i
    for i, chunk in enumerate(chunks):
        for macro_name, macro_index in macro_references.items():
            if f"%{macro_name}" in chunk and i != macro_index:
                dag.add_edge(macro_index, i)
    return dag


def split_overflow_chunks(chunk_list: list[dict], max_lines: int = 400) -> list[dict]:
    result = []
    logical_keywords = ("RUN;", "QUIT;", "%MEND;")

    for chunk in chunk_list:
        lines = chunk["code"].splitlines()
        if len(lines) <= max_lines:
            result.append(chunk)
            continue

        subchunks = []
        start = 0
        while start < len(lines):
            end = min(start + max_lines, len(lines))
            logical_end = -1

            # Search for a logical keyword between start and end
            for i in range(end - 1, start - 1, -1):
                if lines[i].strip().upper().endswith(logical_keywords):
                    logical_end = i + 1
                    break

            if logical_end == -1 or logical_end <= start:
                # Force split at max_lines if no logical end
                logical_end = end

            subchunk_lines = lines[start:logical_end]
            subchunks.append("\n".join(subchunk_lines))
            start = logical_end

        # Add all subchunks
        for j, sub in enumerate(subchunks):
            result.append({
                "id": f"{chunk['id']}_sub{j+1}",
                "code": sub.strip()
            })

    return result




def process_sas_string(sas_code: str, max_chunk_size: int = 100) -> list[dict]:
    initial_chunks = parse_sas_code(sas_code)
    sub_chunks = chunk_large_blocks(initial_chunks, max_chunk_size)
    dag = build_dependency_graph(sub_chunks)
    ordered_chunks = [dag.nodes[i]["code"] for i in nx.topological_sort(dag)] if dag.nodes else sub_chunks
    initial_result = [{"id": f"blk_{i+1:03}", "code": chunk.strip()} for i, chunk in enumerate(ordered_chunks)]
    return split_overflow_chunks(initial_result, max_lines=400)



def process_sas_file(file_path: str, max_chunk_size: int = 100) -> list[dict]:
    with open(file_path, "r", encoding="utf-8") as f:
        sas_code = f.read()
    return process_sas_string(sas_code, max_chunk_size)

def save_chunks_to_csv(chunk_list: list[dict], output_file: str):
    df = pd.DataFrame(chunk_list)
    df.to_csv(
        output_file,
        index=False,
        quoting=csv.QUOTE_ALL,
        escapechar="\\",
        doublequote=True,
        lineterminator="\n"
    )
    

