#!/usr/bin/env python3
"""
Snowflake → BigQuery SQL Migration Script
==========================================
Converts all .sql files under metadata_model/db/
into BigQuery-compatible SQL under metadata_model/db_bq/

Usage:
    python snowflake_to_bigquery_migrator.py

    OR specify custom paths:
    python snowflake_to_bigquery_migrator.py --src /path/to/db --dst /path/to/db_bq
"""

import os
import re
import sys
import shutil
import argparse
from pathlib import Path

# Force UTF-8 output on Windows consoles
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ─────────────────────────────────────────────────────────────
# CONFIGURATION — Edit these if your project/dataset changes
# ─────────────────────────────────────────────────────────────
BQ_PROJECT = "prj-s-3384-datachef-qa"
BQ_DATASET = "everest_analysis"

# Snowflake database name (used in all object references)
SF_DATABASE = "EVEREST_ANALYSIS_ASCENT_PR"

# Folder paths — change these to point at a different set of scripts.
# Can also be overridden at runtime: --src /path/to/db --dst /path/to/db_bq
SRC_FOLDER = "metadata_model/db"
DST_FOLDER = "metadata_model/db_bq"

# Snowflake schemas → BigQuery table prefix mapping
# Pattern: SF_DATABASE.SCHEMA.TABLE → BQ_PROJECT.BQ_DATASET.SCHEMA_TABLE
#
# IMPORTANT: List longer/more-specific names BEFORE shorter ones that share a
# prefix (e.g. "BR_REFERENCE_DATA" before "BR_INACT") so the regex alternation
# always tries the longer match first. The helper _schemas_re() handles this
# automatically by sorting longest-first.
SCHEMA_MAPPINGS = [
    "BR_TODAY",
    "RF_TODAY",
    "BR_REFERENCE_DATA",
    "RF_REFERENCE_DATA",
    "BR_INACT",
    "RF_INACT",
]


def _schemas_re() -> str:
    """
    Build a regex alternation from SCHEMA_MAPPINGS sorted longest-first.
    Sorting prevents a shorter name (e.g. BR_INACT) from shadowing a longer
    one that shares the same prefix if one is ever added to the list.
    """
    return "|".join(re.escape(s) for s in sorted(SCHEMA_MAPPINGS, key=len, reverse=True))


# ─────────────────────────────────────────────────────────────
# CONVERSION FUNCTIONS
# ─────────────────────────────────────────────────────────────

def convert_object_references(sql: str) -> str:
    """
    Converts Snowflake 3-part names to BigQuery names.

    Handles both quoted and unquoted forms:
      "EVEREST_ANALYSIS_ASCENT_PR"."BR_TODAY"."MY_TABLE"
      EVEREST_ANALYSIS_ASCENT_PR.BR_TODAY.MY_TABLE

    → `prj-s-3384-datachef-qa.everest_analysis.BR_TODAY_MY_TABLE`
    """

    def replace_ref(match):
        schema = match.group(1).strip('"').upper()
        table  = match.group(2).strip('"').upper()
        return f"`{BQ_PROJECT}.{BQ_DATASET}.{schema}_{table}`"

    schemas_pattern = _schemas_re()

    # 1) Quoted form: "DB"."SCHEMA"."TABLE"
    pattern_quoted = (
        r'"' + re.escape(SF_DATABASE) + r'"'
        + r'\s*\.\s*'
        + r'"(' + schemas_pattern + r')"'
        + r'\s*\.\s*'
        + r'"([^"]+)"'
    )
    sql = re.sub(pattern_quoted, replace_ref, sql, flags=re.IGNORECASE)

    # 2) Unquoted form: DB.SCHEMA.TABLE
    pattern_unquoted = (
        re.escape(SF_DATABASE)
        + r'\s*\.\s*'
        + r'(' + schemas_pattern + r')'
        + r'\s*\.\s*'
        + r'([A-Za-z0-9_]+)'
    )
    sql = re.sub(pattern_unquoted, replace_ref, sql, flags=re.IGNORECASE)

    return sql


def convert_create_view(sql: str) -> str:
    """
    Converts CREATE OR REPLACE VIEW with Snowflake 3-part name
    to BigQuery 3-part backtick name.

    Snowflake: CREATE OR REPLACE VIEW DB.SCHEMA.VIEW_NAME
    BigQuery:  CREATE OR REPLACE VIEW `project.dataset.SCHEMA_VIEW_NAME`

    Also handles SECURE views (BigQuery has no SECURE keyword — it is stripped).
    """
    schemas_pattern = _schemas_re()

    def replace_view(match):
        # Strip SECURE keyword — BigQuery doesn't support it
        prefix = re.sub(r'\bSECURE\s+', '', match.group(1), flags=re.IGNORECASE)
        schema = match.group(2).strip('"').upper()
        view   = match.group(3).strip('"').upper()
        return f"{prefix}`{BQ_PROJECT}.{BQ_DATASET}.{schema}_{view}`"

    pattern = (
        r'(CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?VIEW\s+)'
        + r'"?' + re.escape(SF_DATABASE) + r'"?'
        + r'\s*\.\s*'
        + r'"?(' + schemas_pattern + r')"?'
        + r'\s*\.\s*'
        + r'"?([A-Za-z0-9_]+)"?'
    )
    sql = re.sub(pattern, replace_view, sql, flags=re.IGNORECASE)

    return sql


def convert_comment_syntax(sql: str) -> str:
    """
    Snowflake: COMMENT = 'some text'  (after CREATE VIEW / TABLE line)
    BigQuery:  OPTIONS(description = 'some text')

    BigQuery doesn't support inline COMMENT= in DDL statements.
    """
    pattern = r"COMMENT\s*=\s*('(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\")"

    def replace_comment(match):
        return f"OPTIONS(description = {match.group(1)})"

    return re.sub(pattern, replace_comment, sql, flags=re.IGNORECASE)


def convert_snowflake_table_clauses(sql: str) -> tuple:
    """
    Strips Snowflake-specific TABLE DDL clauses that are INVALID in BigQuery
    and would cause a parse error if left in the output.

    Each removed clause is replaced with a comment so the intent is preserved.
    Returns (converted_sql, list_of_warnings).
    """
    warnings = []

    # Clauses that must be removed (they cause BQ parse errors)
    removals = [
        (
            r'DATA_RETENTION_TIME_IN_DAYS\s*=\s*\d+',
            'DATA_RETENTION_TIME_IN_DAYS removed — '
            'set table expiration via BigQuery OPTIONS(expiration_timestamp=...) or the BQ API/console',
        ),
        (
            r'CHANGE_TRACKING\s*=\s*\w+',
            'CHANGE_TRACKING removed — not supported in BigQuery',
        ),
        (
            r'COPY\s+GRANTS',
            'COPY GRANTS removed — not supported in BigQuery',
        ),
    ]

    for pattern, message in removals:
        def make_replacer(msg):
            def replacer(match):
                warnings.append(msg)
                return f"-- REMOVED (not supported in BigQuery): {match.group(0)}"
            return replacer
        sql = re.sub(pattern, make_replacer(message), sql, flags=re.IGNORECASE)

    # Flag TRANSIENT / VOLATILE tables — need manual rewrite
    if re.search(r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TRANSIENT|VOLATILE)\s+TABLE\b', sql, re.IGNORECASE):
        warnings.append(
            'TRANSIENT/VOLATILE TABLE detected — BigQuery has no equivalent; '
            'use a regular table with a short expiration if needed'
        )

    return sql, warnings


def flag_snowflake_specific_syntax(sql: str) -> tuple:
    """
    Detects Snowflake-specific constructs that cannot be auto-converted.
    Appends -- TODO: REVIEW comments so developers know exactly where to look.

    Checks are ordered: most critical / most common first.
    Only the FIRST matching check on each line gets a TODO tag; all matches
    are recorded in the returned warnings list for the file-level summary.
    """
    warnings = []

    checks = [
        # ── Type casting ─────────────────────────────────────────────────────
        # :: (PostgreSQL-style cast) is very common in Snowflake and completely
        # unsupported in BigQuery — must use CAST(expr AS type) instead.
        (
            r'::[A-Za-z][A-Za-z0-9_]*(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?',
            ':: Snowflake cast syntax not in BigQuery — use CAST(expr AS type)'
        ),
        (r'\bTRY_CAST\s*\(',        'TRY_CAST() not in BigQuery — use SAFE_CAST()'),

        # ── Control flow ──────────────────────────────────────────────────────
        (r'\bQUALIFY\b',            'QUALIFY not in BigQuery — rewrite as subquery with ROW_NUMBER()/RANK()'),
        (r'\bILIKE\b',              'ILIKE not in BigQuery — use LOWER() + LIKE or REGEXP_CONTAINS()'),
        (r'\bIFF\s*\(',             'IFF() not in BigQuery — use IF()'),
        (r'\bNVL2\s*\(',            'NVL2() not in BigQuery — use IF(expr IS NOT NULL, val1, val2)'),
        (r'\bNVL\s*\(',             'NVL() not in BigQuery — use IFNULL() or COALESCE()'),
        (r'\bZEROIFNULL\s*\(',      'ZEROIFNULL() not in BigQuery — use IFNULL(expr, 0)'),
        (r'\bNULLIFZERO\s*\(',      'NULLIFZERO() not in BigQuery — use NULLIF(expr, 0)'),
        (r'\bDECODE\s*\(',          'DECODE() not in BigQuery — rewrite as CASE WHEN'),

        # ── Date / time functions ─────────────────────────────────────────────
        (r'\bGETDATE\s*\(',         'GETDATE() not in BigQuery — use CURRENT_TIMESTAMP()'),
        (r'\bDATEADD\s*\(',         'DATEADD() not in BigQuery — use DATE_ADD() or TIMESTAMP_ADD()'),
        (r'\bDATEDIFF\s*\(',        'DATEDIFF() not in BigQuery — use DATE_DIFF() or TIMESTAMP_DIFF()'),
        (r'\bTIMESTAMPADD\s*\(',    'TIMESTAMPADD() not in BigQuery — use TIMESTAMP_ADD(ts, INTERVAL n unit)'),
        (r'\bTIMESTAMPDIFF\s*\(',   'TIMESTAMPDIFF() not in BigQuery — use TIMESTAMP_DIFF(end, start, unit) note arg order'),
        (
            r'\bTO_TIMESTAMP(?:_NTZ|_TZ|_LTZ)?\s*\(',
            'TO_TIMESTAMP*() not in BigQuery — use PARSE_TIMESTAMP() or TIMESTAMP()'
        ),
        (r'\bTO_DATE\s*\(',         'TO_DATE() not in BigQuery — use DATE() for ISO strings or PARSE_DATE() for custom format'),
        (r'\bCONVERT_TIMEZONE\s*\(','CONVERT_TIMEZONE() not in BigQuery — use DATETIME(ts, timezone) or AT TIME ZONE'),
        (r'\bDATE_FROM_PARTS\s*\(', 'DATE_FROM_PARTS() not in BigQuery — use DATE(year, month, day)'),
        (r'\bTIME_FROM_PARTS\s*\(', 'TIME_FROM_PARTS() not in BigQuery — use TIME(hour, minute, second)'),
        (
            r'\bTIMESTAMP_FROM_PARTS\s*\(',
            'TIMESTAMP_FROM_PARTS() not in BigQuery — use DATETIME(year, month, day, hour, min, sec)'
        ),

        # ── String functions ──────────────────────────────────────────────────
        (r'\bTO_VARCHAR\s*\(',      'TO_VARCHAR() not in BigQuery — use CAST(... AS STRING)'),
        (r'\bSPLIT_PART\s*\(',      'SPLIT_PART() not in BigQuery — use SPLIT(str, delim)[SAFE_OFFSET(n-1)]'),
        (r'\bSTRTOK\s*\(',          'STRTOK() not in BigQuery — use SPLIT(str, delim)[SAFE_OFFSET(n-1)]'),
        (r'\bCHARINDEX\s*\(',       'CHARINDEX() not in BigQuery — use STRPOS(string, substring)'),
        (r'\bCONTAINS\s*\(',        'CONTAINS() not in BigQuery — use INSTR(str, substr) > 0 or REGEXP_CONTAINS()'),
        (r'\bREGEXP_SUBSTR\s*\(',   'REGEXP_SUBSTR() not in BigQuery — use REGEXP_EXTRACT() (note: different argument order)'),
        (r'\bREGEXP_REPLACE\s*\(',  'REGEXP_REPLACE() — verify arg count; BQ only takes 3 args (str, pattern, replacement)'),

        # ── Aggregation ───────────────────────────────────────────────────────
        (r'\bLISTAGG\s*\(',         'LISTAGG() not in BigQuery — use STRING_AGG(col, delimiter)'),
        (r'\bBOOLOR_AGG\s*\(',      'BOOLOR_AGG() not in BigQuery — use LOGICAL_OR()'),
        (r'\bBOOLAND_AGG\s*\(',     'BOOLAND_AGG() not in BigQuery — use LOGICAL_AND()'),

        # ── Array / semi-structured ───────────────────────────────────────────
        (r'\bFLATTEN\s*\(',         'FLATTEN() not in BigQuery — use UNNEST()'),
        (r'\bARRAY_SIZE\s*\(',      'ARRAY_SIZE() not in BigQuery — use ARRAY_LENGTH()'),
        (r'\bARRAY_CONSTRUCT\s*\(', 'ARRAY_CONSTRUCT() not in BigQuery — use [...] literal or ARRAY() constructor'),
        (r'\bARRAY_SLICE\s*\(',     'ARRAY_SLICE() not in BigQuery — rewrite using UNNEST with OFFSET'),
        (r'\bOBJECT_CONSTRUCT\s*\(','OBJECT_CONSTRUCT() not in BigQuery — use JSON_OBJECT() or TO_JSON_STRING()'),
        (r'\bGET_PATH\s*\(',        'GET_PATH() not in BigQuery — use JSON_VALUE() or JSON_EXTRACT()'),

        # ── SELECT modifiers ──────────────────────────────────────────────────
        # Snowflake: SELECT * EXCLUDE (col)  /  SELECT * REPLACE (expr AS col)
        (r'\bEXCLUDE\s*\(',         'SELECT EXCLUDE not in BigQuery — explicitly list all required columns'),
        (r'\bSELECT\b.*\bREPLACE\s*\(', 'SELECT REPLACE not in BigQuery — use expressions in the SELECT list'),

        # ── Hierarchical / procedural ─────────────────────────────────────────
        (r'\bCONNECT\s+BY\b',       'CONNECT BY not in BigQuery — rewrite using a recursive CTE'),
        (r'\bSTART\s+WITH\b',       'START WITH (hierarchical query) not in BigQuery — use recursive CTE'),
        (r'\bPIVOT\s*\(',           'PIVOT syntax differs in BigQuery — verify column list and aggregation'),
        (r'\bUNPIVOT\s*\(',         'UNPIVOT syntax differs in BigQuery — verify syntax'),
        (r'\bMERGE\b',              'MERGE syntax differs in BigQuery — verify WHEN clause and table aliases'),

        # ── Sampling ──────────────────────────────────────────────────────────
        (r'\b(?:TABLESAMPLE|SAMPLE)\s*\(', 'SAMPLE/TABLESAMPLE syntax differs in BigQuery'),

        # ── Sequences / identity ──────────────────────────────────────────────
        (r'\bSEQ\d+\b',             'Snowflake sequence (SEQ<n>) not in BigQuery — use row_number() or auto-increment'),

        # ── Positional column references ──────────────────────────────────────
        # Snowflake allows $1, $2 to reference columns by position
        (r'\$\d+\b',                'Positional column reference ($1, $2...) not in BigQuery — use column names'),

        # ── Template variables ────────────────────────────────────────────────
        (r'\$\{[^}]+\}',            'Jinja/template variable — verify it resolves correctly in BigQuery context'),
    ]

    lines = sql.split('\n')
    new_lines = []

    for line in lines:
        # Skip lines that are already SQL comments — avoids flagging commented-out code
        # and lines we already annotated in a previous pass.
        stripped = line.lstrip()
        if stripped.startswith('--') or '-- TODO' in line:
            new_lines.append(line)
            continue

        flagged = False
        for pattern, message in checks:
            if re.search(pattern, line, re.IGNORECASE):
                new_lines.append(line + f'  -- TODO: REVIEW — {message}')
                flagged = True
                warnings.append(message)
                break   # one TODO per line keeps output readable; all hits go to summary
        if not flagged:
            new_lines.append(line)

    return '\n'.join(new_lines), warnings


def add_migration_header(sql: str, source_file: str, warnings: list) -> str:
    """Prepends a migration header to the converted file."""
    warn_block = ""
    if warnings:
        # dict.fromkeys preserves first-seen order while deduplicating
        unique_warnings = list(dict.fromkeys(warnings))
        warn_list = '\n'.join(f'--   ⚠  {w}' for w in unique_warnings)
        warn_block = f"--\n-- ⚠  WARNINGS — Items needing manual review:\n{warn_list}\n"

    header = f"""\
-- ============================================================
-- MIGRATED: Snowflake → BigQuery
-- Source   : {source_file}
-- Project  : {BQ_PROJECT}
-- Dataset  : {BQ_DATASET}
{warn_block}-- ============================================================

"""
    return header + sql


def convert_sql(sql: str, source_file: str) -> tuple:
    """Master conversion function — applies all transformations in order."""
    sql = convert_create_view(sql)
    sql = convert_object_references(sql)
    sql = convert_comment_syntax(sql)
    sql, table_warnings = convert_snowflake_table_clauses(sql)  # must run before flag check
    sql, flag_warnings  = flag_snowflake_specific_syntax(sql)
    warnings = table_warnings + flag_warnings
    sql = add_migration_header(sql, source_file, warnings)
    return sql, warnings


# ─────────────────────────────────────────────────────────────
# FILE PROCESSING
# ─────────────────────────────────────────────────────────────

def process_directory(src_dir: Path, dst_dir: Path):
    """
    Walks src_dir, converts each .sql file,
    mirrors folder structure into dst_dir.
    """
    total_files         = 0
    converted           = 0
    files_with_warnings = []
    files_with_tables   = []   # tracks files that contain CREATE TABLE (not VIEW)

    # Matches CREATE TABLE / CREATE OR REPLACE TABLE / CREATE TRANSIENT TABLE etc.
    _CREATE_TABLE_RE = re.compile(
        r'\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TRANSIENT\s+|VOLATILE\s+)?TABLE\b',
        re.IGNORECASE
    )

    print(f"\n{'='*60}")
    print(f" Snowflake → BigQuery Migration")
    print(f"{'='*60}")
    print(f" Source : {src_dir}")
    print(f" Target : {dst_dir}")
    print(f"{'='*60}\n")

    for root, dirs, files in os.walk(src_dir):
        # Skip hidden folders (e.g. .git)
        dirs[:] = [d for d in dirs if not d.startswith('.')]

        sql_files = [f for f in files if f.lower().endswith('.sql')]
        if not sql_files:
            continue

        # Mirror the folder structure
        rel_root   = Path(root).relative_to(src_dir)
        target_dir = dst_dir / rel_root
        target_dir.mkdir(parents=True, exist_ok=True)

        for filename in sql_files:
            total_files += 1
            src_file = Path(root) / filename
            dst_file = target_dir / filename

            try:
                with open(src_file, 'r', encoding='utf-8', errors='replace') as f:
                    original_sql = f.read()

                converted_sql, warnings = convert_sql(
                    original_sql,
                    str(src_file.relative_to(src_dir.parent))
                )

                with open(dst_file, 'w', encoding='utf-8') as f:
                    f.write(converted_sql)

                # Detect CREATE TABLE in the original source (before conversion)
                has_table = bool(_CREATE_TABLE_RE.search(original_sql))
                if has_table:
                    # Capture the exact DDL keyword found for the summary message
                    m = _CREATE_TABLE_RE.search(original_sql)
                    files_with_tables.append((str(rel_root / filename), m.group(0).upper()))

                status = "✓" if not warnings else "⚠"
                tag    = " [TABLE]" if has_table else ""
                print(f"  {status}  {rel_root / filename}{tag}")

                if warnings:
                    files_with_warnings.append((str(rel_root / filename), warnings))

                converted += 1

            except Exception as e:
                print(f"  ✗  ERROR processing {filename}: {e}")

    # ── Summary ──────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f" DONE: {converted}/{total_files} files converted")
    print(f" Output: {dst_dir}")

    if files_with_warnings:
        print(f"\n ⚠  {len(files_with_warnings)} file(s) need manual review:")
        for fname, warns in files_with_warnings:
            print(f"\n   📄 {fname}")
            for w in list(dict.fromkeys(warns)):   # ordered dedup
                print(f"      → {w}")
    else:
        print("\n ✅ No Snowflake-specific issues detected!")

    # ── CREATE TABLE notice ───────────────────────────────────
    if files_with_tables:
        print(f"\n{'='*60}")
        print(f" 🗂  CREATE TABLE DETECTED — {len(files_with_tables)} file(s)")
        print(f"    These scripts create physical tables, not views.")
        print(f"    Please verify: partitioning, clustering, expiration,")
        print(f"    schema types, and any table options before deploying.")
        print()
        for fname, ddl_type in files_with_tables:
            print(f"   📋 {fname}  ({ddl_type})")

    print(f"\n{'='*60}\n")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Migrate Snowflake SQL templates to BigQuery'
    )
    parser.add_argument(
        '--src',
        default=SRC_FOLDER,
        help=f'Source folder (default: {SRC_FOLDER})'
    )
    parser.add_argument(
        '--dst',
        default=DST_FOLDER,
        help=f'Destination folder (default: {DST_FOLDER})'
    )
    args = parser.parse_args()

    src_dir = Path(args.src).resolve()
    dst_dir = Path(args.dst).resolve()

    if not src_dir.exists():
        print(f"\n❌ ERROR: Source directory not found: {src_dir}")
        print(f"   Run this script from your project root, or use --src to specify the path.")
        return

    # Back up existing output directory before overwriting
    if dst_dir.exists():
        backup = Path(str(dst_dir) + '_backup')
        print(f"⚠  {dst_dir} already exists. Backing up to {backup}")
        if backup.exists():
            shutil.rmtree(backup)
        shutil.copytree(dst_dir, backup)
        shutil.rmtree(dst_dir)

    dst_dir.mkdir(parents=True, exist_ok=True)
    process_directory(src_dir, dst_dir)


if __name__ == '__main__':
    main()
