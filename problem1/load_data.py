import psycopg2
import csv
import os
import argparse
from psycopg2.extras import execute_batch

def connect(args):
    return psycopg2.connect(
        host=args.host,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        port=args.port
    )

def run_schema(conn, schema_path):
    with conn.cursor() as cur, open(schema_path, encoding="utf-8") as f:
        cur.execute(f.read())
    conn.commit()

def load_lines(conn, path):
    with conn.cursor() as cur, open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [(r["line_name"], r["vehicle_type"]) for r in reader]
        execute_batch(cur, """
            INSERT INTO lines (line_name, vehicle_type)
            VALUES (%s, %s)
            ON CONFLICT (line_name) DO NOTHING
        """, rows)
        cur.execute("SELECT line_id, line_name FROM lines")
        mapping = {name: lid for lid, name in cur.fetchall()}
    conn.commit()
    return len(rows), mapping

def load_stops(conn, path):
    with conn.cursor() as cur, open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [(r["stop_name"], float(r["latitude"]), float(r["longitude"])) for r in reader]
        execute_batch(cur, """
            INSERT INTO stops (stop_name, latitude, longitude)
            VALUES (%s, %s, %s)
            ON CONFLICT (stop_name) DO NOTHING
        """, rows)
        cur.execute("SELECT stop_id, stop_name FROM stops")
        mapping = {name: sid for sid, name in cur.fetchall()}
    conn.commit()
    return len(rows), mapping

def load_line_stops(conn, path, line_map, stop_map):
    with conn.cursor() as cur, open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            lid = line_map.get(r["line_name"])
            sid = stop_map.get(r["stop_name"])
            if lid and sid:
                rows.append((lid, sid, int(r["sequence"]), int(r["time_offset"])))
        execute_batch(cur, """
            INSERT INTO line_stops (line_id, stop_id, sequence_number, time_offset_minutes)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (line_id, stop_id) DO UPDATE
            SET sequence_number = EXCLUDED.sequence_number,
                time_offset_minutes = EXCLUDED.time_offset_minutes
        """, rows)
    conn.commit()
    return len(rows)

def load_trips(conn, path, line_map):
    with conn.cursor() as cur, open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            lid = line_map.get(r["line_name"])
            if lid:
                rows.append((r["trip_id"], lid, r["scheduled_departure"], r["vehicle_id"]))
        execute_batch(cur, """
            INSERT INTO trips (trip_code, line_id, scheduled_departure, vehicle_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (trip_code) DO NOTHING
        """, rows)
    conn.commit()
    return len(rows)

def load_stop_events(conn, path, stop_map):
    with conn.cursor() as cur, open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            sid = stop_map.get(r["stop_name"])
            if sid:
                rows.append((r["trip_id"], sid, r["scheduled"], r["actual"], int(r["passengers_on"]), int(r["passengers_off"])))
        execute_batch(cur, """
            INSERT INTO stop_events (trip_code, stop_id, scheduled_time, actual_time, passengers_on, passengers_off)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, rows)
    conn.commit()
    return len(rows)

def resolve_data_dir(base_dir):
    candidates = [base_dir, "./data", "data", "./datasets", "datasets", "./metro_data", "metro_data"]
    for d in candidates:
        d = os.path.abspath(d)
        if os.path.exists(d) and os.path.isdir(d):
            return d
    alt = os.path.join(os.getcwd(), "data")
    if os.path.exists(alt):
        return alt
    raise FileNotFoundError(f"No valid data directory found (checked: {candidates})")

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="db")
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--dbname", default="transit")
    ap.add_argument("--user", default="transit")
    ap.add_argument("--password", default="transit123")
    ap.add_argument("--schema", default="schema.sql")
    ap.add_argument("--datadir", default="/app/data")
    return ap.parse_args()

def main():
    args = parse_args()
    args.datadir = resolve_data_dir(args.datadir)
    conn = connect(args)
    print(f"Connected to {args.dbname}@{args.host}")
    print("Creating schema...")
    run_schema(conn, args.schema)
    print("Tables created: lines, stops, line_stops, trips, stop_events")
    try:
        lines_csv = os.path.join(args.datadir, "lines.csv")
        stops_csv = os.path.join(args.datadir, "stops.csv")
        line_stops_csv = os.path.join(args.datadir, "line_stops.csv")
        trips_csv = os.path.join(args.datadir, "trips.csv")
        stop_events_csv = os.path.join(args.datadir, "stop_events.csv")
        c1, line_map = load_lines(conn, lines_csv)
        c2, stop_map = load_stops(conn, stops_csv)
        c3 = load_line_stops(conn, line_stops_csv, line_map, stop_map)
        c4 = load_trips(conn, trips_csv, line_map)
        c5 = load_stop_events(conn, stop_events_csv, stop_map)
        total = c1 + c2 + c3 + c4 + c5
        print(f"\nTotal: {total} rows loaded")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("\nError occurred, rolled back.")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    main()
