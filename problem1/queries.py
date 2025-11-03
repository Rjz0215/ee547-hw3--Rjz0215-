#!/usr/bin/env python3
import argparse, json, sys
import psycopg2
from psycopg2.extras import RealDictCursor

QUERIES = {
    "Q1": {
        "description": "List all stops on Route 20 in order",
        "sql": """
            SELECT s.stop_name AS stop_name,
                   ls.sequence_number AS sequence,
                   ls.time_offset_minutes AS time_offset
            FROM line_stops ls
            JOIN lines l ON l.line_id = ls.line_id
            JOIN stops s ON s.stop_id = ls.stop_id
            WHERE l.line_name = %s
            ORDER BY ls.sequence_number
        """,
        "params": ["Route 20"],
    },
    "Q2": {
        "description": "Trips during morning rush (7-9 AM)",
        "sql": """
            SELECT t.trip_code, l.line_name, t.scheduled_departure
            FROM trips t
            JOIN lines l ON l.line_id = t.line_id
            WHERE (t.scheduled_departure::time) >= TIME '07:00:00'
              AND (t.scheduled_departure::time) <  TIME '09:00:00'
            ORDER BY t.scheduled_departure
        """,
        "params": [],
    },
    "Q3": {
        "description": "Transfer stops (stops on 2+ routes)",
        "sql": """
            SELECT s.stop_name, COUNT(DISTINCT ls.line_id) AS line_count
            FROM line_stops ls
            JOIN stops s ON s.stop_id = ls.stop_id
            GROUP BY s.stop_id, s.stop_name
            HAVING COUNT(DISTINCT ls.line_id) >= 2
            ORDER BY line_count DESC, s.stop_name
        """,
        "params": [],
    },
    "Q4": {
        "description": "Complete route for a specific trip in order",
        "sql": """
            SELECT t.trip_code, l.line_name, s.stop_name,
                   ls.sequence_number AS sequence,
                   ls.time_offset_minutes AS time_offset
            FROM trips t
            JOIN lines l ON l.line_id = t.line_id
            JOIN line_stops ls ON ls.line_id = t.line_id
            JOIN stops s ON s.stop_id = ls.stop_id
            WHERE t.trip_code = %s
            ORDER BY ls.sequence_number
        """,
        "params": ["T0001"],
    },
    "Q5": {
        "description": "Routes serving both Wilshire / Veteran and Le Conte / Broxton",
        "sql": """
            SELECT l.line_name
            FROM lines l
            JOIN line_stops ls ON ls.line_id = l.line_id
            JOIN stops s ON s.stop_id = ls.stop_id
            WHERE s.stop_name IN ('Wilshire / Veteran','Le Conte / Broxton')
            GROUP BY l.line_id, l.line_name
            HAVING COUNT(DISTINCT s.stop_name) = 2
            ORDER BY l.line_name
        """,
        "params": [],
    },
    "Q6": {
        "description": "Average ridership by line",
        "sql": """
            SELECT l.line_name,
                   AVG(se.passengers_on + se.passengers_off)::numeric(10,2) AS avg_passengers
            FROM stop_events se
            JOIN trips t ON t.trip_code = se.trip_code
            JOIN lines l ON l.line_id = t.line_id
            GROUP BY l.line_name
            ORDER BY l.line_name
        """,
        "params": [],
    },
    "Q7": {
        "description": "Top 10 busiest stops by total activity",
        "sql": """
            SELECT s.stop_name,
                   SUM(se.passengers_on + se.passengers_off) AS total_activity
            FROM stop_events se
            JOIN stops s ON s.stop_id = se.stop_id
            GROUP BY s.stop_id, s.stop_name
            ORDER BY total_activity DESC, s.stop_name
            LIMIT 10
        """,
        "params": [],
    },
    "Q8": {
        "description": "Count delays by line (> 2 minutes late)",
        "sql": """
            SELECT l.line_name,
                   COUNT(*) AS delay_count
            FROM stop_events se
            JOIN trips t ON t.trip_code = se.trip_code
            JOIN lines l ON l.line_id = t.line_id
            WHERE se.actual_time > se.scheduled_time + INTERVAL '2 minutes'
            GROUP BY l.line_name
            ORDER BY delay_count DESC, l.line_name
        """,
        "params": [],
    },
    "Q9": {
        "description": "Trips with 3+ delayed stops (> 2 minutes late)",
        "sql": """
            SELECT se.trip_code,
                   COUNT(*) AS delayed_stop_count
            FROM stop_events se
            WHERE se.actual_time > se.scheduled_time + INTERVAL '2 minutes'
            GROUP BY se.trip_code
            HAVING COUNT(*) >= 3
            ORDER BY delayed_stop_count DESC, se.trip_code
        """,
        "params": [],
    },
    "Q10": {
        "description": "Stops with above-average ridership (by total boardings)",
        "sql": """
            WITH totals AS (
                SELECT s.stop_id, s.stop_name, SUM(se.passengers_on) AS total_boardings
                FROM stop_events se
                JOIN stops s ON s.stop_id = se.stop_id
                GROUP BY s.stop_id, s.stop_name
            ),
            avg_total AS (
                SELECT AVG(total_boardings) AS avg_boardings FROM totals
            )
            SELECT t.stop_name, t.total_boardings
            FROM totals t, avg_total a
            WHERE t.total_boardings > a.avg_boardings
            ORDER BY t.total_boardings DESC, t.stop_name
        """,
        "params": [],
    },
}

def connect(args):
    return psycopg2.connect(
        host=args.host, dbname=args.dbname, user=args.user,
        password=args.password, port=args.port,
    )

def run_query(conn, key, params):
    spec = QUERIES[key]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(spec["sql"], params)
        rows = cur.fetchall()
    return spec["description"], rows

def main():
    ap = argparse.ArgumentParser(description="Run predefined queries against the transit DB")
    ap.add_argument("--host", default="db")
    ap.add_argument("--port", default="5432")
    ap.add_argument("--dbname", required=True)
    ap.add_argument("--user", default="transit")
    ap.add_argument("--password", default="transit123")
    ap.add_argument("--query", choices=sorted(QUERIES.keys()))
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--format", choices=["text","json"], default="text")
    args = ap.parse_args()

    if not args.query and not args.all:
        print("Specify --query Qn or --all", file=sys.stderr); sys.exit(2)

    conn = connect(args)
    try:
        keys = sorted(QUERIES.keys()) if args.all else [args.query]
        outs = []
        for k in keys:
            desc, rows = run_query(conn, k, QUERIES[k]["params"])
            if args.format == "json":
                outs.append({"query": k, "description": desc, "results": rows, "count": len(rows)})
            else:
                print(f"[{k}] {desc}")
                for r in rows[:10]:
                    print("  " + ", ".join(f"{kk}={r[kk]}" for kk in r.keys()))
                print(f"  ({len(rows)} rows)\n")
        if args.format == "json":
            print(json.dumps(outs[0] if not args.all else outs, indent=2, default=str))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
