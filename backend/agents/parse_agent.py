# backend/agents/parse_agent.py
import uuid
from agents.utils.sas_chunker_new import process_sas_string, save_chunks_to_csv

from agents.utils.plsql_chunker import process_plsql_string, classify
from agents.utils.generic_sql_chunker import process_sql_string
from agents.utils.general_informatica_datastage_chunker import process_info_string


def infer_chunk_type(code: str) -> str:
    first_line = code.strip().split("\n")[0].upper()
    if first_line.startswith("%MACRO"):
        return "MACRO"
    if first_line.startswith("PROC "):
        return "PROC"
    if first_line.startswith("DATA "):
        return "DATA"
    return "UNKNOWN"

def parse_node(state: dict) -> dict:
    print("🔍 Parse Node: starting with max-line chunker")

    src_code: str = state["sas_code"]
    max_chunk_size = state.get("max_chunk_size", 100)
    source_type = state.get("source").lower()
    print(source_type)


    if source_type == "sas":
        print("SAS Applied")
        raw_chunks = process_sas_string(src_code, max_chunk_size)
        get_type   = infer_chunk_type

    
    elif source_type == "snowflake":
        print("Snowflake Applied")
        raw_chunks = process_info_string(src_code)
        get_type   = lambda _: "Snowflake"

    elif source_type == "informatica":
        print("Informatica Applied")
        raw_chunks = process_info_string(src_code)
        get_type   = lambda _: "Informatica"

    elif source_type == "datastage":
        print("Datastage Applied")
        raw_chunks = process_info_string(src_code)
        get_type   = lambda _: "Datastage"

    elif source_type == "oracle" or "plsql":
        print("Oracle/PLSQL Applied")
        raw_chunks = process_plsql_string(src_code, max_lines=200)
        get_type   = classify

    elif source_type == "cobol":
        print("COBOL Applied")
        raw_chunks = process_info_string(src_code)
        get_type   = lambda _: "COBOL"
    
    else:
        print("Else applied")
        raw_chunks = process_sql_string(src_code)
        get_type   = lambda _: "SQL"

    ast_blocks = [{
        "id":   ch["id"],
        "type": get_type(ch["code"]),
        "code": ch["code"]
    } for ch in raw_chunks]

    if not ast_blocks:          # completely empty file fallback
        ast_blocks.append({
            "id":   str(uuid.uuid4()),
            "type": "UNKNOWN",
            "code": src_code
        })


    # for block in ast_blocks:
    #     print(f"Block ID: {block['id']}, Type: {block['type']}, Code: {block['code']}")
    

    save_chunks_to_csv(ast_blocks, "ast_blocks_latest.csv")
    print(f"📦 Parse Node: produced {len(ast_blocks)} AST blocks.")

    trace = state.get("graph_trace", [])
    trace.append("parse")

    return {
        **state,
        "ast_blocks":        ast_blocks,
        "chunk_count":       len(ast_blocks),
        "sas_line_count":    src_code.count("\\n") + 1,
        "unknown_blocks":    sum(1 for b in ast_blocks if b["type"] == "UNKNOWN"),
        "logs": state.get("logs", []) + [f"Parse: {len(ast_blocks)} blocks"],
        "graph_trace": trace
    }



# # backend/agents/parse_agent.py
# import uuid
# from agents.utils.sas_chunker import chunk_sas_code_v3, save_chunks_to_csv

# def parse_node(state: dict) -> dict:
#     print("🔍 Parse Node: starting hybrid chunk/parse")

#     sas_code: str = state["sas_code"]
#     ast_blocks = []

#     raw_chunks = chunk_sas_code_v3(sas_code)

#     for chunk in raw_chunks:
#         ast_blocks.append({
#             "id":   chunk["id"],
#             "type": chunk["type"].upper(),
#             "code": chunk["code"]
#         })

#     if not ast_blocks:      # empty file
#         ast_blocks.append({
#             "id": str(uuid.uuid4()),
#             "type": "UNKNOWN",
#             "code": sas_code
#         })

#     save_chunks_to_csv(ast_blocks, "ast_blocks_latest.csv")
#     print(f"📦 Parse Node: produced {len(ast_blocks)} AST blocks.")

#     # simple stats (no macro/PROC counts now)
#     trace = state.get("graph_trace", [])
#     trace.append("parse")

#     return {
#         **state,
#         "ast_blocks":        ast_blocks,
#         "chunk_count":       len(ast_blocks),
#         "sas_line_count":    sas_code.count("\n") + 1,
#         "unknown_blocks":    sum(1 for b in ast_blocks if b["type"] == "UNKNOWN"),
#         "logs": state.get("logs", []) + [f"Parse: {len(ast_blocks)} blocks"],
#         "graph_trace": trace
#     }
