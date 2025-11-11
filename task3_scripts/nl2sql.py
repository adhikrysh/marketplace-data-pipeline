# nl2sql.py instructions
# pip install openai mysql-connector-python python-dotenv 
# pip install -U openai "httpx<0.28"
#vchange SCHEMA_FILE

# e.g. run once docker is up: python nl2sql.py "average hours from placed to shipped per item"

import os, json, textwrap
from typing import Tuple, Dict, Any, List

import mysql.connector
from mysql.connector import errorcode
from openai import OpenAI

# --- Config (uses your compose defaults) ---
DB_HOST = '127.0.0.1' #lh
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "password")
DB_NAME = os.getenv("DB_NAME", "marketplace")
SCHEMA_FILE = "/Users/drc/proj/whynot/mysql-docker/initdb/schema.sql"


client = OpenAI(
    api_key="REDACTED_API_KEY",     
    organization="REDACTED_ORG_ID"  
)

SYSTEM_PROMPT = """You are a senior data engineer that converts natural language into strict MySQL 8.4 SQL.
Rules:
- Use only the tables/columns provided in SCHEMA.
- Prefer sargable predicates; avoid functions on indexed columns in WHERE/JOIN.
- Prefer ANSI join syntax; qualify columns (t.col).
- Return a single SQL statement. No comments, no markdown.
- If the request is ambiguous, pick a reasonable interpretation and say so in 'notes'.
- **Return output as strict JSON with keys {"sql": string, "notes": string}.**  <-- add this line (must include the word JSON)
"""


# ---------- DB helpers ----------
def get_conn():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS,
        database=DB_NAME, autocommit=True
    )

def fetch_live_schema() -> str:
    """Build a compact schema snapshot from INFORMATION_SCHEMA: tables, columns, PKs, FKs."""
    cn = get_conn()
    cur = cn.cursor(dictionary=True)

    # Tables & columns
    cur.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """, (DB_NAME,))
    cols = cur.fetchall()

    # PKs
    cur.execute("""
        SELECT k.TABLE_NAME, k.COLUMN_NAME, k.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
          ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
         AND t.TABLE_SCHEMA = k.TABLE_SCHEMA
        WHERE t.TABLE_SCHEMA = %s AND t.CONSTRAINT_TYPE='PRIMARY KEY'
        ORDER BY k.TABLE_NAME, k.ORDINAL_POSITION
    """, (DB_NAME,))
    pks = cur.fetchall()

    # FKs
    cur.execute("""
        SELECT
          k.TABLE_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
        WHERE k.TABLE_SCHEMA = %s AND k.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY k.TABLE_NAME, k.COLUMN_NAME
    """, (DB_NAME,))
    fks = cur.fetchall()

    cur.close(); cn.close()

    # Shape to a readable snapshot
    pk_map: Dict[str, List[str]] = {}
    for r in pks:
        pk_map.setdefault(r["TABLE_NAME"], []).append(r["COLUMN_NAME"])

    fk_lines: Dict[str, List[str]] = {}
    for r in fks:
        fk_lines.setdefault(r["TABLE_NAME"], []).append(
            f"FOREIGN KEY ({r['COLUMN_NAME']}) REFERENCES {r['REFERENCED_TABLE_NAME']}({r['REFERENCED_COLUMN_NAME']})"
        )

    out_lines: List[str] = []
    current = None
    for r in cols:
        t = r["TABLE_NAME"]
        if t != current:
            if current is not None:
                # close previous table with PK/FKs if present
                if current in pk_map:
                    out_lines.append(f"  PRIMARY KEY ({', '.join(pk_map[current])})")
                if current in fk_lines:
                    for fk in fk_lines[current]:
                        out_lines.append(f"  {fk}")
                out_lines.append(");")
                out_lines.append("")
            out_lines.append(f"TABLE {t} (")
            current = t
        nullable = "" if r["IS_NULLABLE"] == "NO" else " NULL"
        out_lines.append(f"  {r['COLUMN_NAME']} {r['DATA_TYPE']}{nullable},")

    if current is not None:
        last_t = current
        if last_t in pk_map:
            out_lines.append(f"  PRIMARY KEY ({', '.join(pk_map[last_t])})")
        if last_t in fk_lines:
            for fk in fk_lines[last_t]:
                out_lines.append(f"  {fk}")
        out_lines.append(");")

    return "\n".join(out_lines)

def read_schema_file(path: str = SCHEMA_FILE) -> str:
    if not os.path.exists(path):
        return "/* schema.sql not found */"
    with open(path, "r", encoding="utf-8") as f:
        txt = f.read()
    # Keep it short-ish to avoid context bloat
    return textwrap.shorten(txt, width=20000, placeholder="\n/* ...truncated... */")

# ---------- Model call ----------
def model_nl2sql(user_query: str, schema_snapshot: str):
    resp = client.chat.completions.create(
        model="gpt-5",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"SCHEMA:\n{schema_snapshot}\n\nQUERY:\n{user_query}"}
        ],
    )

    content = resp.choices[0].message.content  # should be JSON string
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        parsed = {"sql": "", "notes": f"Non-JSON response: {content[:200]}..."}
    if "sql" not in parsed:
        parsed["sql"] = ""
    return parsed


# ---------- Explain plan ----------
def explain_sql(sql: str) -> str:
    """Run EXPLAIN FORMAT=JSON on the candidate SQL. We do not execute the query itself."""
    cn = get_conn()
    cur = cn.cursor()
    try:
        cur.execute(f"EXPLAIN FORMAT=JSON {sql}")
        row = cur.fetchone()
        # MySQL returns a single JSON column
        plan_json = row[0] if row else None
        return plan_json or "(no plan returned)"
    finally:
        cur.close(); cn.close()

# ---------- Public function ----------
def nl2sql(query: str) -> Dict[str, Any]:
    # 1) Try live schema
    try:
        schema = fetch_live_schema()
        source = "live"
    except mysql.connector.Error:
        # 2) Fallback to file
        schema = read_schema_file()
        source = "schema.sql"

    # 3) Model
    out = model_nl2sql(query, schema)
    sql = out.get("sql", "").strip()
    notes = out.get("notes", "").strip()

    # 4) Optional safety: allow EXPLAIN on SELECT/INSERT/UPDATE/DELETE.
    # If you want to restrict to SELECT only, uncomment next two lines.
    if not sql.upper().lstrip().startswith("SELECT"):
         return {"sql": sql, "notes": notes + " (EXPLAIN skipped: non-SELECT)", "explain_plan": None, "schema_source": source}

    # 5) Plan
    try:
        plan = explain_sql(sql)
    except mysql.connector.Error as e:
        plan = f"(EXPLAIN error: {e})"

    return {"sql": sql, "notes": notes, "explain_plan": plan, "schema_source": source}

# ---------- CLI demo ----------
if __name__ == "__main__":
    import sys
    q = " ".join(sys.argv[1:]) or "top 5 sellers by delivered revenue per category"
    result = nl2sql(q)
    print(json.dumps(result, indent=2))
