from __future__ import annotations
import uuid
from pathlib import Path
from typing import Dict, List
import pandas as pd
import json
import re

from langchain_openai       import AzureChatOpenAI
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate

# ───────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
RULE_DIR = BASE_DIR / "rule_outputs"
RULE_DIR.mkdir(exist_ok=True)

# SYSTEM_PROMPT = (
#     "You are an expert migration engineer.\n"
#     "Convert the given SAS code block to equivalent PySpark"
#     "while preserving business logic and naming.\n"
# )

# PROMPT = ChatPromptTemplate.from_messages([
#     ("system", SYSTEM_PROMPT),
#     ("user",
#      "### SAS code (chunk {chunk_id}, type={chunk_type}) ###\n"
#      "{sas_code}\n\n### PySpark equivalent ###"),
# ])

# ───────────────────────────────────────────────────────────────────
def _count_tokens(model_name: str, text: str) -> int:
    try:
        import tiktoken
        enc = tiktoken.encoding_for_model(model_name)
        return len(enc.encode(text))
    except Exception:
        return len(text.split())

def _init_llm(provider: str, cred: Dict):
    if provider == "azureopenai":
        return AzureChatOpenAI(
            azure_endpoint     = cred["openai_api_base"],
            openai_api_key     = cred["openai_api_key"],
            openai_api_version = cred["openai_api_version"],
            deployment_name    = cred["deployment_name"],
            model_name         = cred["model_name"],
            temperature        = 0.0,
        )
    if provider == "gemini":
        return ChatGoogleGenerativeAI(
            model          = cred["model_name"],
            google_api_key = cred["google_api_key"],
            temperature    = 0.0,
        )
    raise ValueError("Unsupported LLM provider")


# dynamic prompt builder -------------------------------------------------------
def _build_prompt(chunk_id: str, chunk_type: str, src_code: str,
                  source: str, target: str, ddl_type: str) -> ChatPromptTemplate:
    if target.lower() == "snowpark" and source.lower() not in ("informatica", "datastage"):
        print("Solo Snowpark Enabled")
        system_msg = (
            "You are an expert in Snowpark (Python) API.\n"
            f"Convert the following {ddl_type.upper()} code from {source.upper()} "
            "to **Snowpark Python code** using the snowflake.snowpark Session/DataFrame API. "
            "Do NOT return plain SQL; produce executable Python code that uses Snowpark constructs."
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"{src_code}\n\n### SNOWPARK PYTHON equivalent ###")
    
    elif source.lower() == "informatica" and target.lower() == "snowpark":
        print("Informatica - Snowpark ETL Prompt Enabled")
        system_msg = (
            """Convert this Informatica mapping (in XML format) into a production-grade PySpark script that writes to Snowflake, fully replicating the ETL logic. Follow these strict and detailed conversion instructions:

                **Conversion Requirements:**

                1. **Stage-to-PySpark Mapping:**
                - **Source Qualifier / Source Definition** → PySpark `read()` operations (include schema inference or explicitly define schema)
                - **Expression / Router / Filter / Aggregator** → DataFrame transformations using `.withColumn()`, `.filter()`, `.groupBy().agg()`, `when`, `expr`, etc.
                - **Joiner** → Use `.join()` with the correct join type (`inner`, `left`, etc.)
                - **Lookup Transformation** → Broadcast join or cached DataFrame join in PySpark
                - **Sequence Generator / Update Strategy** → Use PySpark logic for surrogate keys, control flow, or row-based operations
                - **Target Definition** → Write to Snowflake using `write.format("snowflake")...`

                2. **XML Element Handling:**
                - Parse `<TRANSFORMATION>` elements to extract:
                    * Type (Expression, Aggregator, Joiner, Lookup, Filter, etc.)
                    * Ports and expressions (`<TRANSFORMFIELD>`)
                    * Logic (e.g., `<EXPRESSION>`, `<CONDITION>`)
                - Parse `<INSTANCE>` and `<CONNECTOR>` to determine the data flow
                - Parse `<MAPPINGVARIABLE>` or parameters as runtime config variables in Python
                - Follow the correct pipeline order as defined by the XML structure (based on `<CONNECTOR>` source-to-target links)

                3. **Data Type Mapping:**
                - Informatica → PySpark/Snowflake:
                    * string → StringType / VARCHAR
                    * integer → IntegerType / NUMBER
                    * decimal → DecimalType / NUMBER(precision, scale)
                    * datetime → TimestampType / TIMESTAMP_NTZ
                - Add `StructType` schema definitions to input reads for better type safety and metadata clarity

                4. **Snowflake Integration:**
                - Use the Spark-Snowflake connector with `write.format("snowflake")`
                - Include all required options (`sfURL`, `sfUser`, `sfPassword`, `sfWarehouse`, `sfDatabase`, `sfSchema`)
                - Use an `sfOptions` dictionary to keep configuration clean
                - Use `mode("overwrite")` or `mode("append")` based on target table behavior in the mapping

                5. **Performance Optimization:**
                - Use `.repartition()` or `.coalesce()` for control over partitions
                - Broadcast small lookup tables using `broadcast()`
                - Cache reused intermediate DataFrames
                - Add comments explaining join strategies or performance-sensitive logic

                6. **Error Handling and Validation:**
                - Wrap risky transformation logic in try/except blocks
                - Include row count logging before and after key stages
                - Add checks for null values and unexpected datatypes
                - Log anomalies using filter + count logic or create error-handling DataFrames

                **Output Structure:**
                # 1. Read Inputs (Sources)
                df_input = spark.read...  -- use schema + file or JDBC options

                # 2. Transformations (Expression, Filter, Aggregation, Joins)
                df_transformed = df_input\
                    .withColumn("col1", expr("..."))\
                    .join(df_lookup, on="id", how="left")\
                    .groupBy("key").agg(...)

                # 3. Write to Snowflake
                df_transformed.write\
                    .format("snowflake")\
                    .options(**sfOptions)\
                    .option("dbtable", "TARGET_TABLE")\
                    .mode("overwrite")\
                    .save()

                **Special Cases Handling:**
                - **Router** → Use `.withColumn()` for routing condition and then `.filter()` to create branch logic
                - **Sequence Generator** → Use `monotonically_increasing_id()` or `row_number() over Window`
                - **Update Strategy** → Use `MERGE` logic if supported, or document in comment how the update is handled
                - **Unconnected Lookup** → Simulate using `join` and conditional logic

                **Commenting Requirements:**
                # Header comment must include:
                # - Original mapping name
                # - Conversion timestamp
                # - End-to-end summary of the data flow

                # Inline comments must document:
                # - Each transformation logic
                # - Business rules and assumptions
                # - Any simplifications or data quality concerns
                # - Join strategy or partitioning rationale

                **Validation Requirements:**
                - Add `df.count()` checks before and after transformations
                - Validate expected vs actual schema using `printSchema()`
                - Include distinct count or null count checks where data quality is key

                **Strict Output Expectations:**
                - You must return a **complete**, end-to-end, production-ready PySpark script that writes to Snowflake.
                - DO NOT ask the user to complete or infer parts — **you must generate everything fully**.
                - DO NOT leave placeholders or TODOs. Resolve and generate all logic explicitly.
                - Avoid hallucinated logic — only use logic explicitly found or inferred from XML.
                - If any assumptions are made, clearly document them in comments.
                - Output must be a fully runnable `.py` script without missing pieces.
                - Ensure the output is syntactically correct and can be executed in a PySpark environment.
                - Give proper explanation and summary changes at the end.

                **Example Pattern:**
                # From Expression Transformation: "EXP_CUSTOMER"
                df_customer = df_input\
                    .withColumn("STATUS", expr("CASE WHEN CREDIT_SCORE > 700 THEN 'GOOD' ELSE 'BAD' END"))\
                    .withColumn("LOAD_TS", current_timestamp())

                
                    Output format:

                ###OUTPUT###
                ``` pyspark output ```
                ###END_OUTPUT###

                
                ###MANUAL_INTERVENTION###
                Any assumptions or changes made during the conversion process 
                (e.g., variable types, missing components, etc.) should be noted here in the form of a list.
                Or any manual intervention required for the conversion process should be noted here.
                ###END_MANUAL_INTERVENTION###

                ###CONVERTION_PERCENTAGE###
                <NUMBER%>
                ###END_CONVERTION_PERCENTAGE###
                """
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"{src_code}\n\n### SNOWPARK equivalent ###")
    
    elif source.lower() == "informatica" and target.lower() == "snowflake":
        print("Informatica - Snowflake ETL Prompt Enabled")
        system_msg = (
            """Convert this Informatica mapping (in XML format) into a fully functional, production-ready Snowflake SQL script that replicates the complete ETL logic. Follow these detailed and strict instructions:

                **Conversion Requirements:**

                1. **Transformation Mapping:**
                - **Source Definition / Qualifier** → Use `SELECT` with fully defined column projections and type casts
                - **Expression Transformations** → Translate `<EXPRESSION>` logic into `CASE WHEN`, `COALESCE`, `CAST`, arithmetic ops
                - **Filter** → Use `WHERE` clauses
                - **Router** → Use `CASE` statements or conditional subqueries/CTEs for routing logic
                - **Joiner** → Convert to explicit `JOIN` statements with correct join type (inner, left, etc.)
                - **Lookup** → Use `LEFT JOIN` or `INNER JOIN`, based on connection logic
                - **Aggregator** → Use `GROUP BY` with aggregate functions
                - **Update Strategy** → Implement using `MERGE` statements (when applicable)
                - **Sequence Generator** → Use `SEQ8()` or `ROW_NUMBER()` as Snowflake alternatives

                2. **XML Element Handling:**
                - Parse `<TRANSFORMATION>` elements and extract:
                    * Type: EXPRESSION, JOINER, FILTER, LOOKUP, AGGREGATOR, etc.
                    * Logic from `<EXPRESSION>`, `<CONDITION>`, `<TRANSFORMFIELD>`
                - Extract input-output links from `<CONNECTOR>` and `<INSTANCE>` to construct flow
                - Parse and substitute any `<MAPPINGVARIABLE>` or runtime parameters

                3. **Data Type Mapping:**
                - Map Informatica types to Snowflake SQL types:
                    * string → VARCHAR
                    * integer → NUMBER
                    * decimal → NUMBER(precision, scale)
                    * datetime → TIMESTAMP_NTZ
                - Apply `CAST()` wherever needed for consistency or precision

                4. **SQL Code Structure:**
                Use **Common Table Expressions (CTEs)** for modularity:
                ```sql
                -- Source Read
                WITH src AS (
                    SELECT ...
                    FROM SOURCE_TABLE
                ),

                -- Transformation Logic
                exp_stage AS (
                    SELECT ... -- Derived columns using CASE, COALESCE, etc.
                    FROM src
                ),

                -- Joins or Lookups
                joined_stage AS (
                    SELECT ...
                    FROM exp_stage
                    LEFT JOIN LOOKUP_TABLE ON ...
                ),

                -- Aggregation
                agg_stage AS (
                    SELECT col1, COUNT(*) AS cnt
                    FROM joined_stage
                    GROUP BY col1
                )

                -- Final Insert
                INSERT INTO TARGET_TABLE (
                    SELECT * FROM agg_stage
                );
                Validation and Constraints:

                Validate row count using SELECT COUNT(*)

                Use IS NULL, IS DISTINCT FROM, IS NOT NULL to filter anomalies

                Log null or unexpected values using conditional logic inside CTEs or SELECT statements

                Commenting and Documentation:

                Header comments must include:
                -- Original mapping name -- Conversion date and time -- Summary of source-to-target mapping and major logic

                Inline comments required for:
                -- Expression logic -- Join logic and assumptions -- Aggregation rules and filter conditions -- Any inferred or default behavior

                Strict Output Expectations:
                    -Return the full, runnable Snowflake SQL script.
                    -DO NOT ask the user to modify or infer any parts.
                    -DO NOT leave placeholders or TODOs — all logic must be explicit and complete.
                    -Clearly document any assumptions made from XML.
                    -Avoid hallucinated logic; base everything on actual elements in the mapping.
                    -Ensure the output is syntactically correct and can be executed in Snowflake.
                    -Provide a complete SQL script that can be run in Snowflake without missing pieces.
                    -Include proper explanation and summary changes at the end.

                Example Pattern:
                -- From Expression: "EXP_CUSTOMER_SCORE"
                exp_customer AS (
                    SELECT
                        CUST_ID,
                        CASE 
                            WHEN CREDIT_SCORE > 700 THEN 'EXCELLENT'
                            WHEN CREDIT_SCORE BETWEEN 500 AND 700 THEN 'GOOD'
                            ELSE 'POOR'
                        END AS CREDIT_CATEGORY
                    FROM src
                )
                
                Output format:

                ###OUTPUT###
                ``` snowflake sql output ```
                ###END_OUTPUT###

                
                ###MANUAL_INTERVENTION###
                Any assumptions or changes made during the conversion process 
                (e.g., variable types, missing components, etc.) should be noted here in the form of a list.
                Or any manual intervention required for the conversion process should be noted here.
                ###END_MANUAL_INTERVENTION###

                ###CONVERTION_PERCENTAGE###
                <NUMBER%>
                ###END_CONVERTION_PERCENTAGE###
                """
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"{src_code}\n\n### SNOWFLAKE SQL equivalent ###")

    elif source.lower() == "informatica" and target.lower() == "matallion":
        system_msg = (
            """You are an expert in ETL migration with deep knowledge of Informatica PowerCenter and Matillion. Your task is to convert Informatica mapping XML into an equivalent, complete Matillion job in valid XML format.

                ## TASK:
                Convert the following Informatica mapping XML into a fully executable and logically equivalent Matillion job XML.

                ## CONVERSION RULES:
                1. Ensure full logical and structural equivalence — all transformations, expressions, joins, filters, lookups, aggregations, and control flows must be preserved.
                2. Map Informatica components to their Matillion counterparts:
                - Source Qualifier / Source Definition → Table Input
                - Expression → Calculator
                - Filter → Filter Component
                - Joiner → Join Component
                - Lookup → Join or API Query with comment
                - Aggregator → Aggregate Component
                - Sequence Generator → Fixed Value or Sequence Job Variable
                - Router → Multiple Filters or conditional flows
                - Update Strategy → Conditional Write or API call
                3. **DO NOT** hallucinate mappings or invent logic. If something cannot be directly translated, insert inline XML comments: <!-- TODO: Manual review required for component -->
                4. Always generate the **complete Matillion job XML**, including all necessary metadata, components, and links between them.
                5. Output must be valid and importable in Matillion without manual correction.
                6. Do not assume the user will complete or fix missing pieces — generate the full code yourself.

                ## OUTPUT FORMAT:
                Return your result using the following structure:

                Output format:

                ###OUTPUT###
                ``` matillion xml output ```
                ###END_OUTPUT###

                
                ###MANUAL_INTERVENTION###
                Any assumptions or changes made during the conversion process 
                (e.g., variable types, missing components, etc.) should be noted here in the form of a list.
                Or any manual intervention required for the conversion process should be noted here.
                ###END_MANUAL_INTERVENTION###

                ###CONVERTION_PERCENTAGE###
                <NUMBER%>
                ###END_CONVERTION_PERCENTAGE###
                
                ```
                """
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"{src_code}\n\n### Matillion equivalent ###")
        
    elif source.lower() == "informatica" and target.lower() == "dbt":
        system_msg = (
            """You are an expert data engineer responsible for converting Informatica ETL job XMLs into DBT-compatible projects.

            Your task is to analyze the following Informatica ETL job XML and generate a valid DBT project structure. This includes all relevant YAML and SQL files required to run the project with no manual edits needed.

            STRICT REQUIREMENTS:
            - Do NOT fabricate or assume logic. Only include what is clearly specified in the XML.
            - Do NOT summarize, abstract, or skip any transformations or conditions.
            - Preserve the exact transformation logic (joins, filters, calculations, derivations).
            - If logic is ambiguous or unsupported, include a comment inside the SQL file noting that the logic needs review.
            - Return only valid YAML and SQL files. No extra text or commentary.
            - Provide me whole output in my given tags, don't break or left in between.
            - Never omit any of these output markers.

            ---

            ###OUTPUT_CODE###
            (Must be the first line of the result. No markdown, quotes, or explanations.)

            Output the result as multiple file blocks in the following format:

            ### File: sources.yml
            - List all source systems and tables used in the Informatica job
            - Use DBT's `source()` config format
            - Include table and column names (if present)
            - Follow correct YAML indentation and DBT schema syntax
            - Add brief descriptions using `#` comments

            ### File: schema.yml
            - YAML model configuration for each generated DBT model
            - Add columns (if identifiable)
            - Add DBT tests like `unique`, `not_null` based on XML constraints
            - Format as valid YAML only

            ### File: <model_name>.sql
            - Implement transformation logic as valid DBT SQL models
            - Use `source('<schema>', '<table>')` syntax
            - Include all joins, filters, expressions, and derivations
            - Add SQL comments for traceability or unsupported logic
            - Format with consistent aliases and indentation
            - End with a summary block describing changes in SQL comment format

            ---

            ###END_OUTPUT_CODE###

            Then include the following required blocks:

            ###MANUAL_INTERVENTION###
            - List any assumptions, issues, or elements needing human validation
            ###END_MANUAL_INTERVENTION###

            ###CONVERTION_PERCENTAGE###
            <N%>
            ###END_CONVERTION_PERCENTAGE###
            """
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"{src_code}\n\n### DBT equivalent ###")

    elif source.lower() == "datastage" and target.lower() == "snowpark":
        print("Datastage - Snowpark ETL Prompt Enabled")
        system_msg = (
            """Convert this IBM DataStage job (in XML format) into a production-grade PySpark script that writes to Snowflake, fully replicating the ETL logic. Follow these detailed instructions:

                **Conversion Requirements:**

                1. **Structural Mapping:**
                - DataStage Sequential File/Input → PySpark DataFrame reads (with schema)
                - Transformer Stages → PySpark DataFrame transformations (withColumn, filter, join, groupBy)
                - Lookup Stages → join or broadcast joins
                - Aggregator → groupBy with agg()
                - Output Stages → write to Snowflake using `write.format("snowflake")...`
                - Preserve the flow and execution sequence as defined in the DataStage job

                2. **XML Element Handling:**
                - Parse `<Stage>` elements and identify:
                    * Type (e.g., Transformer, Aggregator, Lookup)
                    * Input/output columns and expressions
                    * Connections to other stages
                - Convert `<Derivation>` logic to PySpark expressions
                - Translate `<Link>` paths into DataFrame pipelines
                - Handle `<JobParameter>` as runtime parameters or config variables in the script

                3. **Data Type Mapping:**
                - DataStage → PySpark/Snowflake:
                    * Char/String → StringType / VARCHAR
                    * Integer → IntegerType / NUMBER
                    * Decimal → DecimalType / NUMBER(precision, scale)
                    * Date → DateType / DATE or TIMESTAMP
                    * Add explicit schema definitions for better type safety

                4. **Snowflake Integration:**
                - Use Spark-Snowflake connector for all writes:
                    * Include options for URL, user, password, role, warehouse, and database
                    * Use `sfOptions` dictionary for cleaner config
                - Partition writes by appropriate keys for performance
                - Use `mode("overwrite")` or `mode("append")` based on DataStage output stage logic

                5. **Performance Optimization:**
                - Use `.repartition()` or `.coalesce()` as needed
                - Broadcast small lookup DataFrames
                - Cache intermediate DataFrames if reused
                - Add comments where skew or join strategy matters

                6. **Error Handling and Validation:**
                - Wrap critical transformations with try/except logic
                - Add row count checks between key stages
                - Include schema and nullability validation
                - Log data anomalies using `df.filter().count()` logic or error DataFrames

                **Output Structure:**
                # 1. Read Inputs
                df_input = spark.read...  -- from SequentialFile or DB

                # 2. Transformation Logic
                df_transformed = df_input\
                    .withColumn("new_col", expr("CASE WHEN ..."))\
                    .join(df_lookup, on="id", how="left")\
                    .groupBy("dept").agg(...)

                # 3. Write to Snowflake
                df_transformed.write\
                    .format("snowflake")\
                    .options(**sfOptions)\
                    .option("dbtable", "TARGET_TABLE")\
                    .mode("overwrite")\
                    .save()

                **Special Cases Handling:**
                - For Hash/Sort stages: Use `sortWithinPartitions()` or `orderBy()`
                - For Funnel stages: Use `unionByName()` with schema alignment
                - For Job Sequences: Comment placeholders to define orchestration order
                - For nested loops or conditions: Implement using control flow in PySpark logic

                **Commenting Requirements:**
                # Header comment with:
                # - Original job name
                # - Conversion date
                # - Source-to-target summary

                # Inline comments for:
                # - Stage-level logic
                # - Transformation purpose
                # - Known assumptions or business rules
                # - Performance considerations

                **Validation Requirements:**
                - Validate row count before and after each stage
                - Assert key field data types and nullability
                - Log summary stats like distinct counts, null counts

                **Strict Output Expectations:**
                - You must return the full, complete PySpark code as a single, production-ready script.
                - Do **not** ask the user to interpret or write further parts themselves.
                - Do **not** skip or summarize any steps — avoid hallucinated logic.
                - If any assumptions are made, clearly document them in code comments.
                - Do not leave placeholders or TODOs — resolve and generate exact logic.
                - DO NOT OMIT any part of the logic. Do not abstract or summarize. You must write the **entire Snowflake PySpark script output end-to-end**.
                - Do not assume the user will modify or complete the output — YOU must generate a fully valid Snowflake PySpark script from start to end.

                **Example Pattern:**
                # Converted from Transformer Stage: "TRANSFORM_EMP"
                df_emp = df_input\
                    .withColumn("SALARY_BAND", expr("CASE WHEN SALARY > 50000 THEN 'HIGH' ELSE 'LOW' END"))\
                    .withColumn("LOAD_TS", current_timestamp())


                Output format:

                ###OUTPUT###
                ``` pyspark output ```
                ###END_OUTPUT###

                
                ###MANUAL_INTERVENTION###
                Any assumptions or changes made during the conversion process 
                (e.g., variable types, missing components, etc.) should be noted here in the form of a list.
                Or any manual intervention required for the conversion process should be noted here.
                ###END_MANUAL_INTERVENTION###

                ###CONVERTION_PERCENTAGE###
                <NUMBER%>
                ###END_CONVERTION_PERCENTAGE###
                """
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"{src_code}\n\n### SNOWPARK PYTHON equivalent ###")   
    
    elif source.lower() == "datastage" and target.lower() == "snowflake":
        print("Datastage - Snowflake ETL Prompt Enabled")
        system_msg = (
            """Convert this IBM DataStage job (in XML format) into a complete, production-grade Snowflake SQL script that fully replicates the ETL logic. Follow the detailed instructions below:

                **Conversion Requirements:**

                1. **Structural Mapping:**
                - DataStage Sequential File/Input → Snowflake External Tables or Staging Tables
                - Transformer Stages → SQL SELECT statements using CASE, CAST, expressions
                - Lookup Stages → JOINs (LEFT/INNER as appropriate)
                - Aggregator → GROUP BY with aggregate functions
                - Output Stages → INSERT or MERGE INTO Snowflake target tables
                - Preserve the exact stage sequence and dependencies as per DataStage job flow

                2. **XML Element Handling:**
                - Parse `<Stage>` elements to identify:
                    * Stage type (Transformer, Lookup, Aggregator, etc.)
                    * Input/output columns and transformation logic
                    * Data flow using `<Link>` and connections
                - Translate `<Derivation>` expressions to SQL logic
                - Use aliases and CTEs to replicate intermediate stages
                - Convert `<JobParameter>` to SQL variables using `SET` or use in CTE logic

                3. **Data Type Mapping:**
                - DataStage → Snowflake:
                    * Char/String → VARCHAR
                    * Integer → NUMBER
                    * Decimal → NUMBER(precision, scale)
                    * Date → DATE or TIMESTAMP_NTZ
                - Use `TRY_CAST()` for safe conversion where type mismatches may occur

                4. **Performance Optimization:**
                - Use `QUALIFY` with window functions for filtering
                - Include clustering keys in `CREATE TABLE` where applicable
                - Optimize JOIN strategies (e.g., use `HASH_JOIN` hints if needed)
                - Leverage CTEs to modularize complex transformation logic

                5. **Error Handling and Validation:**
                - Add row count comparisons between stages (use temp tables or `WITH` clauses)
                - Handle NULLs with COALESCE or conditional logic
                - Include TRY_CAST with default fallbacks where risk of conversion failure exists
                - Document business rule validation and data integrity logic in comments

                **Output Structure:**
                -- 1. Stage Definitions (if needed)
                -- 2. Temporary Structures (CTEs or Temp Tables)
                -- 3. Transformation Logic (SELECTs with JOINs, CASE, GROUP BY, etc.)
                -- 4. Final Load Statements (INSERT INTO / MERGE INTO)

                **Special Cases Handling:**
                - Funnel Stage: Use `UNION ALL`
                - Sort Stage: Use `ORDER BY` within CTEs
                - Filter Stage: Add `WHERE` clause conditions
                - Job Sequencer: Use SQL comments to annotate the intended execution order

                **Commenting Requirements:**
                /* Header:
                - Original DataStage job name
                - Conversion date
                - Brief summary of transformation flow */

                -- Inline comments for:
                -- Stage source (e.g., -- From Transformer: TRANSFORM_CUSTOMER)
                -- Business logic explanation
                -- Validation checks and assumptions
                -- Optimization hints or reasoning

                **Validation Requirements:**
                - Use COUNT(*) checks between logical stages
                - Assert key column data types with `IS_<TYPE>()`
                - Document assumption-driven logic in comments

                **Strict Output Expectations:**
                - You must return a complete, runnable Snowflake SQL script.
                - Do **not** instruct the user to complete or modify anything manually.
                - Avoid hallucinations or fabricated logic.
                - Do not skip, abstract, or summarize any logic.
                - Do not return placeholders or partial code — the SQL output must be end-to-end, fully resolved.
                - If assumptions are made due to missing metadata, clearly document them in the output.

                **Example Pattern:**
                -- From Transformer Stage: "TRANSFORM_SALES"
                WITH transformed_sales AS (
                    SELECT
                        CUSTOMER_ID,
                        CASE WHEN SALE_AMT > 1000 THEN 'HIGH' ELSE 'LOW' END AS SALE_CATEGORY,
                        CURRENT_TIMESTAMP() AS LOAD_TS
                    FROM @stage_path/files/
                )
                MERGE INTO TARGET_TABLE USING transformed_sales ON ...
                WHEN MATCHED THEN UPDATE ...
                WHEN NOT MATCHED THEN INSERT ...

                
                Output format:

                ###OUTPUT###
                ``` snowflake sql output ```
                ###END_OUTPUT###

                
                ###MANUAL_INTERVENTION###
                Any assumptions or changes made during the conversion process 
                (e.g., variable types, missing components, etc.) should be noted here in the form of a list.
                Or any manual intervention required for the conversion process should be noted here.
                ###END_MANUAL_INTERVENTION###

                ###CONVERTION_PERCENTAGE###
                <NUMBER%>
                ###END_CONVERTION_PERCENTAGE###
                """
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"{src_code}\n\n### SNOWFLAKE SQL equivalent ###")
    
    elif source.lower() == "datastage" and target.lower() == "matallion":
        system_msg = (
            """You are an expert in ETL tool migration and ETL code transformation with deep domain knowledge of IBM DataStage and Matillion.

                TASK:
                Convert the following IBM DataStage job configuration (in XML format) into a Matillion job configuration (also in XML format).

                RULES:
                1. Ensure 100% logical and functional equivalence — preserve all data flows, transformations, column mappings, component order, and sequencing.
                2. Accurately map IBM DataStage components to Matillion components:
                - Sequential File → Table Input or S3 Load
                - Transformer → Calculator or Python Script
                - Lookup → Join
                - Row Generator → Fixed-Value Component or similar
                3. DO NOT hallucinate or fabricate logic. If something cannot be mapped directly, insert: <!-- TODO: Manual mapping required for component :  -->
                4. Include ALL relevant nodes and attributes to ensure the output is a valid, complete, and deployable Matillion job.
                5. DO NOT OMIT any part of the logic. Do not abstract or summarize. You must write the **entire Matillion XML output end-to-end**.
                6. Do not assume the user will modify or complete the output — YOU must generate a fully valid Matillion job from start to end.
                7. Ensure the output is syntactically correct and can be imported directly into Matillion as an XML job.
                8. Structure your output cleanly using the format below.

                Output format:
                ###OUTPUT###
                ``` matillion xml output ```
                ###END_OUTPUT###

                
                ###MANUAL_INTERVENTION###
                Any assumptions or changes made during the conversion process 
                (e.g., variable types, missing components, etc.) should be noted here in the form of a list.
                Or any manual intervention required for the conversion process should be noted here.
                ###END_MANUAL_INTERVENTION###

                ###CONVERTION_PERCENTAGE###
                <NUMBER%>
                ###END_CONVERTION_PERCENTAGE###
                
        ```
        """
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"{src_code}\n\n### Matillion equivalent ###")

    elif source.lower() == "datastage" and target.lower() == "dbt":
        system_msg = (
            """You are an expert data engineer responsible for converting Datastage ETL job XMLs into DBT-compatible projects.
            Your task is to analyze the following Datastage ETL job XML and generate a valid DBT project structure. This includes all relevant YAML and SQL files required to run the project with no manual edits needed.

            STRICT REQUIREMENTS:
            - Do NOT fabricate or assume logic. Only include what is clearly specified in the XML.
            - Do NOT summarize, abstract, or skip any transformations or conditions.
            - Preserve the exact transformation logic (joins, filters, calculations, derivations).
            - If logic is ambiguous or unsupported, include a comment inside the SQL file noting that the logic needs review.
            - Return only valid YAML and SQL files. No extra text or commentary.
            - Provide me whole output in my given tags, don't break or left in between.
            - Never omit any of these output markers.


            ---

            ###OUTPUT_CODE###
            (Must be the first line of the result. No markdown, quotes, or explanations.)

            Output the result as multiple file blocks in the following format:

            ### File: sources.yml
            - List all source systems and tables used in the Datastage job
            - Use DBT's `source()` config format
            - Include table and column names (if present)
            - Follow correct YAML indentation and DBT schema syntax
            - Add brief descriptions using `#` comments

            ### File: schema.yml
            - YAML model configuration for each generated DBT model
            - Add columns (if identifiable)
            - Add DBT tests like `unique`, `not_null` based on XML constraints
            - Format as valid YAML only

            ### File: <model_name>.sql
            - Implement transformation logic as valid DBT SQL models
            - Use `source('<schema>', '<table>')` syntax
            - Include all joins, filters, expressions, and derivations
            - Add SQL comments for traceability or unsupported logic
            - Format with consistent aliases and indentation
            - End with a summary block describing changes in SQL comment format

            ---

            ###END_OUTPUT_CODE###

            Then include the following required blocks:

            ###MANUAL_INTERVENTION###
            - List any assumptions, issues, or elements needing human validation
            ###END_MANUAL_INTERVENTION###

            ###CONVERTION_PERCENTAGE###
            <N%>
            ###END_CONVERTION_PERCENTAGE###
            """
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"{src_code}\n\n### DBT equivalent ###")
    
    elif source.lower() == "sas" and target.lower() == "dbt":
        print("SAS DBT Applied")
        system_msg = (
            """You are an expert SAS-to-dbt migration engineer.\n
    Your task is to convert SAS code into production-ready **dbt** assets while preserving
    ALL business logic, column names, comments, and step order. Nothing may be omitted.\n\n

    Follow these strict rules:\n
    1) Scope & parity\n
       • Convert every SAS statement. If any line cannot be translated 1:1, include it as a commented line\n
        immediately above the closest equivalent and explain how the logic is replicated.\n
       • Preserve column names, casing, and semantics. Avoid silent type changes.\n
      • Derivations (IF/THEN, CASE logic, functions) must become SQL expressions inside the dbt model.\n\n

    2) dbt model output (SQL)\n
       • Produce a single **dbt model** with a top `{{ config(...) }}` block.\n
       • Default to `materialized='table'`. If the SAS flow is incremental/append/merge-like, use\n
         `materialized='incremental'`, set a `unique_key`, and implement `is_incremental()` guards.\n
       • Use `source()` for raw inputs (e.g., `{{ source('RAW', 'TABLE') }}`) and `ref()` for upstream models.\n
       • Use CTEs to mirror SAS steps (DATA/PROC) in the original order: one CTE per step, clearly named.\n
       • Map common SAS constructs:\n
           - DATA step SET → SELECT … FROM {{ source(...) }} (or CTE)\n
           - KEEP/DROP/RENAME → explicit column projection/aliases\n
           - WHERE/IF → WHERE / CASE WHEN\n
           - PROC SQL → direct SQL CTEs\n
           - PROC SORT → only if order is semantically required (document if only presentation)\n
           - PROC TRANSPOSE → PIVOT/UNPIVOT pattern (or macro if needed)\n
           - PROC FORMAT/value maps → mapping table (seed/ref) or CASE expression\n
           - Dates/numbers/strings → ANSI SQL where possible; document adapter-specific parts\n
       • Every SAS step must have a corresponding CTE or explicit comment explaining the handling.\n\n

    3) Tests & documentation (YAML)\n
       • Emit a `schema.yml` snippet with model + columns, descriptions inferred from comments, and tests\n
         (at minimum `not_null`, `unique` where appropriate; add `accepted_values` or `relationships` if implied).\n\n

    4) Sources (YAML)\n
      • If raw tables are referenced, emit a `sources.yml` snippet with `database`, `schema`, `tables` and optional\n
         column descriptions (infer from SAS comments). Use generic placeholders if not explicit.\n\n

    5) Macros (optional)\n
       • If a clean abstraction helps (e.g., a reusable formatting/transposition helper), emit a dbt macro snippet\n
         and call it from the model. Keep macros minimal and documented.\n\n

    6) Output packaging\n
       • Return **multiple code blocks** with clear markers so each can be written to its file by tooling:\n
           ###OUTPUT_MODEL###        (dbt model SQL)\n
           ```sql\n
           ...\n
           ```\n
           ###END_OUTPUT_MODEL###\n
           \n
           ###OUTPUT_TESTS_YML###    (schema.yml tests/docs)\n
           ```yml\n
           ...\n
           ```\n
           ###END_OUTPUT_TESTS_YML###\n
           \n
           ###OUTPUT_SOURCES_YML###  (only if raw sources exist)\n
           ```yml\n
           ...\n
          ```\n
           ###END_OUTPUT_SOURCES_YML###\n
           \n
           ###OUTPUT_MACROS###       (optional; only if used)\n
           ```sql\n
           ...\n
           ```\n
           ###END_OUTPUT_MACROS###\n\n

    7) Quality gates\n"
       • The SQL must parse. Use valid Jinja in config/ref/source/macros.\n
       • No placeholders like TODO/REVIEW unless also accompanied by a working default.\n
       • Keep comments explaining any assumption or adapter-specific behavior.\n"""
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"""{src_code}\n\n### DBT equivalent ###
            - Produce the dbt model SQL with a config block and ordered CTEs mirroring SAS steps.
            - Add a schema.yml snippet with tests and column documentation.
            - If the code reads raw tables, include a sources.yml snippet.
            - If you introduce a helper, include its macro code and call it from the model.
            Return your answer using the exact markers specified above.""")
            
    elif source.lower() == "cobol" and target.lower() == "dbt":
        print("Cobol DBT Applied")
        system_msg = (
            """You are an expert COBOL-to-dbt migration engineer.
            Convert COBOL batch logic into production-ready dbt assets while preserving
            ALL business logic, field names, comments, and step order. Do not omit any line.

            Requirements (no exceptions):
            1) Full coverage & parity
            • Every COBOL line must be either converted or carried as a nearby comment explaining why.
            • Keep names/semantics; avoid silent type changes.
            • IF/ELSE/COMPUTE → SQL expressions; PERFORM paragraphs/sections → ordered CTEs (in the same sequence).
            • File READ/WRITE → dbt sources (source()) and model outputs (ref()/materializations). No missing steps.

            2) COBOL structure mapping
            • DATA DIVISION / FD layouts:
                - 01-level items → columns; ignore FILLER unless referenced.
                - REDEFINES → implement explicitly (separate projections/CTEs or CASE) and document choice.
                - OCCURS → row explosion via a numbers/sequence pattern; document cardinality.
                - PIC mapping examples:
                    * PIC X(n)                → VARCHAR(n)
                    * PIC 9(n)[V9(m)][S]      → NUMBER(precision, scale)
                    * COMP-3 (packed decimal) → DECIMAL via helper macro (see Macros section)
                    * Character dates         → parse via macro with explicit formats
            • PROCEDURE DIVISION:
                - MOVE/COMPUTE → column projections/casts
                - IF / EVALUATE → CASE WHEN
                - PERFORM       → sequential, named CTEs mirroring paragraph order
                - READ/WRITE    → source()/ref() + materialization for final target

            3) dbt model (SQL)
            • Produce one dbt model with a top {{ config(...) }} block.
            • Default: materialized='table'. If job is incremental/append, use materialized='incremental', set unique_key,
                and wrap filters in is_incremental().
            • Use source() for raw inputs and ref() for upstream models; NEVER hardcode fully qualified names.
            • Each COBOL step becomes a CTE; the final SELECT is the model output.
            • Add explicit casts for PIC/COMP-3; do not lose sign/scale. Handle REDEFINES explicitly.

            4) Tests & documentation (YAML)
            • Emit a schema.yml snippet:
                - model description derived from COBOL comments
                - columns with descriptions inferred from 01-level names/comments
                - tests: not_null/unique/accepted_values/relationships as applicable

            5) Sources (YAML)
            • If raw files/tables are read, emit a sources.yml snippet with database, schema, tables, and column notes
                (use placeholders only when the COBOL does not specify; prefer concrete names).

            6) Macros (optional but recommended)
            • Provide macros when needed and call them from the model:
                - decode_comp3(binary_col) → DECIMAL
                - parse_cobol_date(text, picture) → DATE/TIMESTAMP
                - strip_leading_zeros(text) → TEXT
            • Document each macro (what, why, how to use).

            7) Output packaging — use EXACT markers below:
            ###OUTPUT_MODEL###
            ```sql
            -- dbt model SQL here
            ###END_OUTPUT_MODEL###

            ###OUTPUT_TESTS_YML###

            yml
            Copy code
            # schema.yml snippet here
            ###END_OUTPUT_TESTS_YML###

            ###OUTPUT_SOURCES_YML###

            yml
            Copy code
            # sources.yml snippet here (include only if sources are used)
            ###END_OUTPUT_SOURCES_YML###

            ###OUTPUT_MACROS###

            sql
            Copy code
            -- one or more dbt macros here (include only if used)
            ###END_OUTPUT_MACROS###

            Quality gates
            • SQL must parse in the target warehouse; Jinja must be valid (no undefined refs).
            • No TODOs/placeholders without a working default and an explanatory comment.
            • Document all assumptions, REDEFINES/OCCURS handling, and data type decisions in comments."""
        )
        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"""{src_code}\n\n### DBT equivalent ###
            - Produce the dbt model SQL with config block and ordered CTEs mirroring COBOL paragraph/step sequence..
            - Add a schema.yml snippet with tests and column documentation.
            - If raw datasets/tables are read, include a sources.yml snippet.
            - If packed decimals/dates or similar need decoding/parsing, include macros and CALL THEM from the model.
            Return your answer using ONLY the EXACT markers specified above.""")

    else:
        print("Else Applied in conversion")
        system_msg = (
            f"You are an expert migration engineer.\n"
            f"Convert the following {ddl_type.upper()} code from "
            f"{source.upper()} to {target.upper()} while preserving "
            f"business logic, naming, and comments."
        )

        user_msg = (
            f"### {source.upper()} code (chunk {chunk_id}, type={chunk_type}) ###\n"
            f"{src_code}\n\n"
            f"### {target.upper()} equivalent ###"
        )

    return ChatPromptTemplate.from_messages([
        ("system", system_msg),
        ("user",   user_msg),
    ])


# LLM invocation per chunk -----------------------------------------------------
def _convert_chunk(llm, blk: Dict, model_name: str,
                   source: str, target: str, ddl_type: str) -> Dict:
    prompt = _build_prompt(
        blk["id"], blk["type"], blk["code"], source, target, ddl_type
    ).format_prompt().to_messages()

    try:
        resp   = llm.invoke(prompt)
        output = resp.content.strip() or "# LLM returned empty"

        if hasattr(resp, "usage"):
            in_tok  = resp.usage.prompt_tokens
            out_tok = resp.usage.completion_tokens
        else:
            in_tok  = _count_tokens(model_name, prompt[-1].content)
            out_tok = _count_tokens(model_name, output)

        return {
            "id":            blk["id"],
            "ok":            True,
            "code":          output,
            "input_tokens":  in_tok,
            "output_tokens": out_tok,
            "total_tokens":  in_tok + out_tok,
        }
    except Exception as e:
        return {
            "id":            blk["id"],
            "ok":            False,
            "code":          f"# LLM ERROR: {e}",
            "input_tokens":  0,
            "output_tokens": 0,
            "total_tokens":  0,
        }

# ───────────────────────────────────────────────────────────────────
def llm_rule_node(state: Dict) -> Dict:
    print("🧠  LLM-Rule Node …")

    # resolve selections (defaults keep old SAS→PySpark flow alive)
    source   = state.get("source").lower()
    target   = state.get("target").lower()
    ddl_type = state.get("ddl_type").lower()

    print("LLM Input types:", source, target, ddl_type)
    ast_blocks: List[Dict] = state.get("ast_blocks", [])
    provider               = state["llm_provider"]
    cred                   = state["llm_cred"]
    model_name             = cred.get("model_name", "").lower()
    print("Done..............")

    llm  = _init_llm(provider, cred)
    rows, status = [], []
    total_in = total_out = 0

    code_lookup = {b["id"]: b["code"] for b in ast_blocks}

    def extract_numeric_part(chunk_id: str) -> int:
        matches = re.findall(r'\d+', chunk_id)
        return int(matches[0]) if matches else -1

    for blk in ast_blocks:
        res = _convert_chunk(llm, blk, model_name, source, target, ddl_type)
        # print("Chunk: ", res["code"])
        rows.append(res)
        status.append({
            "id":            res["id"],
            "ok":            res["ok"],
            "input_tokens":  res["input_tokens"],
            "output_tokens": res["output_tokens"],
            "total_tokens":  res["total_tokens"],
        })
        total_in  += res["input_tokens"]
        total_out += res["output_tokens"]

    rows.sort(key=lambda r: extract_numeric_part(r["id"]))

    out_col = f"output_{target}_code"
    
    if target.lower() == "pyspark":
        out_col = "output_pyspark_code"

    csv_path = Path(state.get("rule_csv",
                   RULE_DIR / f"rule_llm_{uuid.uuid4().hex}.csv"))
    state["rule_csv"] = str(csv_path)
    pd.DataFrame({
        "id":                  [r["id"] for r in rows],
        "success":             [r["ok"] for r in rows],
        "input_source_code":      [code_lookup[r["id"]] for r in rows],
        out_col             : [r["code"] for r in rows],
        "input_tokens":        [r["input_tokens"] for r in rows],
        "output_tokens":       [r["output_tokens"] for r in rows],
        "total_tokens":        [r["total_tokens"] for r in rows],
    }).to_csv(csv_path, index=False)

    tok = state.get("token_usage", {})
    tok["llm"] = {
        "input":  total_in,
        "output": total_out,
        "total":  total_in + total_out,
        "model":  model_name,
    }
    state["token_usage"] = tok

    # ✅ Write LLM token usage to JSON for optimizer
    LLM_TOKEN_JSON = RULE_DIR / "llm_token_usage.json"
    if "llm" in tok:
        with open(LLM_TOKEN_JSON, "w") as f:
            json.dump({"llm": tok["llm"]}, f)

    successes  = [r for r in rows if r["ok"]]
    failed_ids = [r["id"] for r in rows if not r["ok"]]

    return {
        **state,
        "pyspark_chunks": successes,
        "failed_chunks":  failed_ids,
        "chunk_status":   status,
        "logs": state.get("logs", []) + [
            f"LLM converted {len(successes)} chunks; failed {len(failed_ids)}"
        ],
        "token_usage": tok
    }
