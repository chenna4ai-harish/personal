"""Comprehensive static analysis checks for snowflake_to_bigquery_migrator.py"""
from collections import Counter
from snowflake_to_bigquery_migrator import (
    SCHEMA_MAPPINGS, SCHEMA_OVERRIDES, _schemas_re,
    convert_object_references, convert_create_view,
    convert_comment_syntax, convert_snowflake_table_clauses,
    flag_snowflake_specific_syntax, convert_sql,
)

passed = 0
failed = 0

def ok(label, result, detail=""):
    global passed
    passed += 1
    print(f"  PASS  {label}" + (f" — {detail}" if detail else ""))

def fail(label, detail=""):
    global failed
    failed += 1
    print(f"  FAIL  {label}" + (f" — {detail}" if detail else ""))

# ── CONFIG SANITY ─────────────────────────────────────────────────────────────
print("\n=== CONFIG SANITY ===")

missing = [k for k in SCHEMA_OVERRIDES if k not in SCHEMA_MAPPINGS]
if missing:
    fail("SCHEMA_OVERRIDES keys all in SCHEMA_MAPPINGS", f"{missing} not in SCHEMA_MAPPINGS — override will never fire")
else:
    ok("SCHEMA_OVERRIDES keys all in SCHEMA_MAPPINGS", str(list(SCHEMA_OVERRIDES.keys())))

parts = _schemas_re().split("|")
lengths = [len(p.replace("\\", "")) for p in parts]
if lengths == sorted(lengths, reverse=True):
    ok("Schema regex longest-first", _schemas_re())
else:
    fail("Schema regex longest-first", f"lengths={lengths}")

# ── OBJECT REFERENCE CONVERSION ───────────────────────────────────────────────
print("\n=== OBJECT REFERENCE CONVERSION ===")

# BR_REFERENCE_DATA → own dataset, no prefix
sql = "SELECT * FROM EVEREST_ANALYSIS_ASCENT_PR.BR_REFERENCE_DATA.DUNS_SEGMENT"
out = convert_object_references(sql)
exp = "`prj-s-3384-datachef-qa.br_reference_data.DUNS_SEGMENT`"
if exp in out:
    ok("BR_REFERENCE_DATA → own dataset, no schema prefix", exp)
else:
    fail("BR_REFERENCE_DATA → own dataset", f"got: {out}")

# BR_TODAY → default dataset with schema prefix
sql = "SELECT * FROM EVEREST_ANALYSIS_ASCENT_PR.BR_TODAY.US_BR_COUNTS_CURRENT"
out = convert_object_references(sql)
exp = "`prj-s-3384-datachef-qa.everest_analysis.BR_TODAY_US_BR_COUNTS_CURRENT`"
if exp in out:
    ok("BR_TODAY → default dataset with schema prefix", exp)
else:
    fail("BR_TODAY → default dataset", f"got: {out}")

# RF_TODAY → default dataset
sql = "SELECT * FROM EVEREST_ANALYSIS_ASCENT_PR.RF_TODAY.RF_INACT_NONBRANCH_DUNS"
out = convert_object_references(sql)
exp = "`prj-s-3384-datachef-qa.everest_analysis.RF_TODAY_RF_INACT_NONBRANCH_DUNS`"
if exp in out:
    ok("RF_TODAY → default dataset with schema prefix", exp)
else:
    fail("RF_TODAY → default dataset", f"got: {out}")

# Quoted 3-part form
sql = 'SELECT * FROM "EVEREST_ANALYSIS_ASCENT_PR"."RF_TODAY"."MY_TABLE"'
out = convert_object_references(sql)
exp = "`prj-s-3384-datachef-qa.everest_analysis.RF_TODAY_MY_TABLE`"
if exp in out:
    ok("Quoted 3-part form converted", exp)
else:
    fail("Quoted 3-part form", f"got: {out}")

# Mixed case in source (case-insensitive matching)
sql = "SELECT * FROM everest_analysis_ascent_pr.br_today.my_table"
out = convert_object_references(sql)
exp = "`prj-s-3384-datachef-qa.everest_analysis.BR_TODAY_MY_TABLE`"
if exp in out:
    ok("Case-insensitive matching (lowercase source)", exp)
else:
    fail("Case-insensitive matching", f"got: {out}")

# Unknown schema — must NOT be converted (no false positives)
sql = "SELECT * FROM EVEREST_ANALYSIS_ASCENT_PR.SOME_OTHER_SCHEMA.MY_TABLE"
out = convert_object_references(sql)
if "backtick" not in out and "`" not in out:
    ok("Unknown schema left unchanged (no false positive)", out.strip())
else:
    fail("Unknown schema should not be converted", f"got: {out}")

# ── CREATE VIEW CONVERSION ────────────────────────────────────────────────────
print("\n=== CREATE VIEW CONVERSION ===")

sql = "CREATE OR REPLACE VIEW EVEREST_ANALYSIS_ASCENT_PR.BR_TODAY.MY_VIEW"
out = convert_create_view(sql)
exp = "`prj-s-3384-datachef-qa.everest_analysis.BR_TODAY_MY_VIEW`"
if exp in out:
    ok("CREATE OR REPLACE VIEW converted", exp)
else:
    fail("CREATE OR REPLACE VIEW", f"got: {out}")

sql = "CREATE VIEW EVEREST_ANALYSIS_ASCENT_PR.RF_TODAY.MY_VIEW"
out = convert_create_view(sql)
exp = "`prj-s-3384-datachef-qa.everest_analysis.RF_TODAY_MY_VIEW`"
if exp in out:
    ok("CREATE VIEW (without OR REPLACE) converted", exp)
else:
    fail("CREATE VIEW without OR REPLACE", f"got: {out}")

sql = "CREATE OR REPLACE SECURE VIEW EVEREST_ANALYSIS_ASCENT_PR.RF_TODAY.V_TEST"
out = convert_create_view(sql)
if "SECURE" not in out and "`prj-s-3384-datachef-qa.everest_analysis.RF_TODAY_V_TEST`" in out:
    ok("SECURE keyword stripped from view", out.strip())
else:
    fail("SECURE VIEW", f"got: {out}")

# ── COMMENT SYNTAX ────────────────────────────────────────────────────────────
print("\n=== COMMENT SYNTAX ===")

sql = "COMMENT = 'My description'"
out = convert_comment_syntax(sql)
if "OPTIONS(description = 'My description')" in out:
    ok("COMMENT = '' → OPTIONS(description=)", out.strip())
else:
    fail("COMMENT syntax", f"got: {out}")

sql = 'COMMENT = "double quotes"'
out = convert_comment_syntax(sql)
if 'OPTIONS(description = "double quotes")' in out:
    ok("COMMENT with double quotes converted", out.strip())
else:
    fail("COMMENT double quotes", f"got: {out}")

# ── SNOWFLAKE TABLE CLAUSES ───────────────────────────────────────────────────
print("\n=== SNOWFLAKE TABLE CLAUSES ===")

sql = "CREATE OR REPLACE TABLE T\nDATA_RETENTION_TIME_IN_DAYS = 7\nAS SELECT 1"
out, warns = convert_snowflake_table_clauses(sql)
if "-- REMOVED" in out and any("DATA_RETENTION_TIME_IN_DAYS" in w for w in warns):
    ok("DATA_RETENTION_TIME_IN_DAYS stripped + warned", warns[0][:70])
else:
    fail("DATA_RETENTION_TIME_IN_DAYS", f"out={out}, warns={warns}")

sql = "CREATE OR REPLACE TABLE T\nCHANGE_TRACKING = TRUE\nAS SELECT 1"
out, warns = convert_snowflake_table_clauses(sql)
if "-- REMOVED" in out and any("CHANGE_TRACKING" in w for w in warns):
    ok("CHANGE_TRACKING stripped + warned", warns[0][:70])
else:
    fail("CHANGE_TRACKING", f"warns={warns}")

sql = "CREATE OR REPLACE TABLE T COPY GRANTS AS SELECT 1"
out, warns = convert_snowflake_table_clauses(sql)
if "-- REMOVED" in out and any("COPY GRANTS" in w for w in warns):
    ok("COPY GRANTS stripped + warned", warns[0][:70])
else:
    fail("COPY GRANTS", f"warns={warns}")

sql = "CREATE OR REPLACE TRANSIENT TABLE T AS SELECT 1"
out, warns = convert_snowflake_table_clauses(sql)
if any("TRANSIENT" in w for w in warns):
    ok("TRANSIENT TABLE flagged", warns[0][:70])
else:
    fail("TRANSIENT TABLE", f"warns={warns}")

# ── SNOWFLAKE SYNTAX FLAGS ─────────────────────────────────────────────────────
print("\n=== SNOWFLAKE SYNTAX FLAGS ===")

flag_checks = [
    ("col::VARCHAR(100)",                           "::"),
    ("SELECT * QUALIFY ROW_NUMBER()=1",             "QUALIFY"),
    ("WHERE name ILIKE '%foo%'",                    "ILIKE"),
    ("IFF(x>0, 1, 0)",                              "IFF"),
    ("NVL(a, b)",                                   "NVL"),
    ("NVL2(a, b, c)",                               "NVL2"),
    ("ZEROIFNULL(col)",                             "ZEROIFNULL"),
    ("NULLIFZERO(col)",                             "NULLIFZERO"),
    ("DECODE(x, 1, 'one', 'other')",                "DECODE"),
    ("GETDATE()",                                   "GETDATE"),
    ("DATEADD(day, 1, dt)",                         "DATEADD"),
    ("DATEDIFF(day, d1, d2)",                       "DATEDIFF"),
    ("TIMESTAMPADD(day, 1, ts)",                    "TIMESTAMPADD"),
    ("TIMESTAMPDIFF(day, t1, t2)",                  "TIMESTAMPDIFF"),
    ("TO_TIMESTAMP_NTZ(col)",                       "TO_TIMESTAMP"),
    ("TO_DATE(col, 'YYYY-MM-DD')",                  "TO_DATE"),
    ("CONVERT_TIMEZONE('UTC', ts)",                 "CONVERT_TIMEZONE"),
    ("DATE_FROM_PARTS(2023, 1, 1)",                 "DATE_FROM_PARTS"),
    ("TO_VARCHAR(col)",                             "TO_VARCHAR"),
    ("SPLIT_PART(col, ',', 1)",                     "SPLIT_PART"),
    ("STRTOK(col, ',', 1)",                         "STRTOK"),
    ("CHARINDEX('x', col)",                         "CHARINDEX"),
    ("CONTAINS(col, 'x')",                          "CONTAINS"),
    ("REGEXP_SUBSTR(col, 'pat')",                   "REGEXP_SUBSTR"),
    ("LISTAGG(col, ',')",                           "LISTAGG"),
    ("BOOLOR_AGG(flag)",                            "BOOLOR_AGG"),
    ("BOOLAND_AGG(flag)",                           "BOOLAND_AGG"),
    ("FLATTEN(INPUT => col)",                       "FLATTEN"),
    ("ARRAY_SIZE(arr)",                             "ARRAY_SIZE"),
    ("ARRAY_CONSTRUCT(1,2,3)",                      "ARRAY_CONSTRUCT"),
    ("OBJECT_CONSTRUCT('k', v)",                    "OBJECT_CONSTRUCT"),
    ("CONNECT BY PRIOR id = parent_id",             "CONNECT BY"),
    ("PIVOT(SUM(v) FOR col IN ('a','b'))",          "PIVOT"),
    ("MERGE INTO t USING s ON t.id=s.id",           "MERGE"),
    ("SELECT $1, $2 FROM t",                        "positional $1"),
    ("SAMPLE(100 rows)",                            "SAMPLE"),
    ("SEQ1.NEXTVAL",                                "SEQ"),
]

for snippet, label in flag_checks:
    _, warns = flag_snowflake_specific_syntax(snippet)
    if warns:
        ok(f"{label} flagged", warns[0][:65])
    else:
        fail(f"{label} NOT flagged — missing check", snippet)

# SQL comment lines must NOT be flagged
sql = "-- NVL() ILIKE QUALIFY are all here"
_, warns = flag_snowflake_specific_syntax(sql)
if not warns:
    ok("Comment lines skipped (no false positive)", "no warns raised")
else:
    fail("Comment lines skipped", f"got warns: {warns}")

# Already-flagged lines (-- TODO present) must not get double-flagged
sql = "NVL(a,b)  -- TODO: REVIEW — something"
_, warns = flag_snowflake_specific_syntax(sql)
if not warns:
    ok("Already-flagged lines not double-flagged", "no warns raised")
else:
    fail("Double-flagging check", f"got warns: {warns}")

# ── NO DOUBLE-CONVERSION ──────────────────────────────────────────────────────
print("\n=== NO DOUBLE CONVERSION ===")

sql = ("CREATE OR REPLACE VIEW EVEREST_ANALYSIS_ASCENT_PR.BR_TODAY.MY_VIEW\n"
       "AS SELECT * FROM EVEREST_ANALYSIS_ASCENT_PR.BR_TODAY.MY_TABLE\n"
       "JOIN EVEREST_ANALYSIS_ASCENT_PR.BR_REFERENCE_DATA.DUNS_SEGMENT s ON 1=1")
out, _ = convert_sql(sql, "test.sql")
v = out.count("`prj-s-3384-datachef-qa.everest_analysis.BR_TODAY_MY_VIEW`")
t = out.count("`prj-s-3384-datachef-qa.everest_analysis.BR_TODAY_MY_TABLE`")
r = out.count("`prj-s-3384-datachef-qa.br_reference_data.DUNS_SEGMENT`")
if v == 1 and t == 1 and r == 1:
    ok("No double-conversion: view=1, table=1, ref_data=1 each", f"view={v} table={t} ref_data={r}")
else:
    fail("Double conversion detected", f"view={v}, table={t}, ref_data={r}\n{out}")

# ── WARNING DEDUPLICATION ORDER ───────────────────────────────────────────────
print("\n=== WARNING DEDUP ORDER ===")

# Use separate lines so each pattern gets its own TODO tag independently
sql = "NVL(a, b)\nNVL(c, d)\nIFF(x, y, z)"
_, warns = flag_snowflake_specific_syntax(sql)
unique = list(dict.fromkeys(warns))
# NVL appears on earlier lines → should come first after dedup
if len(unique) == 2 and unique[0].startswith("NVL") and unique[1].startswith("IFF"):
    ok("Dedup preserves first-seen order", str([w[:30] for w in unique]))
else:
    fail("Dedup order", f"raw warns={warns}, unique={unique}")

# ── SUMMARY ───────────────────────────────────────────────────────────────────
print(f"\n{'='*55}")
print(f"  TOTAL: {passed + failed} checks   PASSED: {passed}   FAILED: {failed}")
print(f"{'='*55}")
