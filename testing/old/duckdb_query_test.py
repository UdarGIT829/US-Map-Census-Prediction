# duckdb_query_tester.py
# Minimal script to paste a DuckDB query and verify results.

import time
import duckdb

# ---- user knobs ----
DB_PATH = "./acs_cache.duckdb"
QUERY = """
SELECT * FROM acs5_profile
WHERE geo_level='state' AND year=2023 AND state='06'
LIMIT 5;
"""
HEAD_N = 10         # how many rows to print from the top

def _normalize(q: str) -> str:
    # Strip leading/trailing whitespace and any trailing semicolon(s)
    q = q.strip()
    while q.endswith(";"):
        q = q[:-1].rstrip()
    return q

if __name__ == "__main__":
    sql = _normalize(QUERY)
    print(f"DB: {DB_PATH}")
    print("=== SQL ===")
    print(sql)
    print("===========\n")

    con = duckdb.connect(DB_PATH)
    try:
        t0 = time.perf_counter()
        rel = con.sql(sql)  # returns a DuckDB relation
        # Relation has schema info even before materialization
        cols = rel.columns
        types = rel.types  # list of DuckDB type strings

        # Materialize results
        rows = rel.fetchall()
        dt = (time.perf_counter() - t0) * 1000.0

        # Print schema
        if cols:
            print("Schema:")
            for c, t in zip(cols, types):
                print(f"  {c}: {t}")
        else:
            print("(No columns)")

        # Preview rows
        n = len(rows)
        print(f"\nRows: {n}   (elapsed: {dt:.1f} ms)\n")

        show_n = min(HEAD_N, n)
        if show_n > 0:
            print(f"Top {show_n} rows:")
            # Pretty-ish fixed-width print
            col_widths = [max(len(str(c)), 12) for c in cols]
            def fmt_row(r):
                return " | ".join(str(v).ljust(w) for v, w in zip(r, col_widths))
            # header
            print(fmt_row(cols))
            print("-+-".join("-"*w for w in col_widths))
            for r in rows[:show_n]:
                print(fmt_row(r))
        else:
            print("(No rows returned)")

    except Exception as e:
        print("\n[ERROR] Query failed:")
        print(e)
    finally:
        con.close()
