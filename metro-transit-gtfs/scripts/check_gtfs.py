# AI Code 

from pathlib import Path
import duckdb

project_dir = Path(__file__).resolve().parents[1]
db_path = project_dir / "metro_transit.duckdb"

con = duckdb.connect(str(db_path), read_only=True)

tables = con.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'main'
    ORDER BY table_name
""").fetchall()

print("\nGTFS tables and row counts\n")

for (table_name,) in tables:
    row_count = con.execute(
        f"SELECT COUNT(*) FROM {table_name}"
    ).fetchone()[0]
    print(f"{table_name:<20} {row_count:>12,}")

print("\nFirst 25 routes\n")

routes = con.execute("""
    SELECT
        route_id,
        route_short_name,
        route_long_name,
        route_type
    FROM routes
    ORDER BY route_short_name
    LIMIT 25
""").fetchall()

for route in routes:
    print(route)

con.close()