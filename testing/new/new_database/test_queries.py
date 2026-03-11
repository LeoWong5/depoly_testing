"""
test_queries.py — Test suite for query.py

Tested methods (all actively used by the webserver):
  1. get_stop_by_code
  2. get_stops_in_area
  3. get_stops_batch
  4. get_route_stops_batch
  5. get_route_polylines_batch
  6. get_active_jp_for_stop
  7. get_upcoming_arrivals_batch

Sections:
  1 – Row-count parity:  DB table counts == CSV line counts
  2 – Spot-check values: sample rows from CSV match DB query results
  3 – get_stop_by_code:  valid, invalid, SQL injection, edge cases
  4 – get_stops_in_area: normal, empty result, pole, large radius
  5 – get_stops_batch:   normal, partial hits, empty list, duplicates
  6 – get_route_stops_batch:   normal, empty, invalid UIDs
  7 – get_route_polylines_batch: normal, empty, fallback (no waypoints)
  8 – get_active_jp_for_stop:  per-day, invalid inputs, DOW boundaries
  9 – get_upcoming_arrivals_batch: normal, empty, DOW/time edge cases
 10 – Cross-table FK consistency
"""

import csv
import os
import sys
from pathlib import Path

dev_path = Path(__file__).parent.parent
sys.path.insert(0, str(dev_path))

from new_database.query import TransportDatabase

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()
NPTG_DIR   = SCRIPT_DIR.parent / "new_preprocessor" / "CSV_NPTG"
BUS_DIR    = SCRIPT_DIR.parent / "new_preprocessor" / "CSV_BUS"

# ── Helpers ──────────────────────────────────────────────────────────────────
passed = 0
failed = 0

def _csv_row_count(csv_dir: Path, filename: str) -> int:
    p = csv_dir / filename
    with open(p, newline="", encoding="utf-8") as f:
        return sum(1 for _ in f) - 1


def _csv_rows(csv_dir: Path, filename: str, max_rows: int = 0) -> list[dict]:
    p = csv_dir / filename
    with open(p, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if max_rows:
            return [next(reader) for _ in range(max_rows)]
        return list(reader)


def _db_count(db: TransportDatabase, table: str) -> int:
    cur = db.conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM [{table}]")
    return cur.fetchone()[0]


def _check(label: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  PASS  {label}")
    else:
        failed += 1
        msg = f"  FAIL  {label}"
        if detail:
            msg += f"  —  {detail}"
        print(msg)


def section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


# ═══════════════════════════════════════════════════════════════════════════════
#  1. Row-count parity  (CSV rows == DB table rows)
# ═══════════════════════════════════════════════════════════════════════════════
def test_row_counts(db: TransportDatabase):
    section("1. Row-count parity — CSV vs DB")

    nptg_tables = [
        ("region.csv",     "Region",     NPTG_DIR),
        ("authority.csv",  "Authority",  NPTG_DIR),
        ("district.csv",   "District",   NPTG_DIR),
        ("locality.csv",   "Locality",   NPTG_DIR),
        ("stop_area.csv",  "Stop_Area",  NPTG_DIR),
        ("stop_point.csv", "Stop_Point", NPTG_DIR),
    ]
    bus_tables = [
        ("operator.csv",                       "Operator",                        BUS_DIR),
        ("garage.csv",                          "Garage",                          BUS_DIR),
        ("serviced_organisation.csv",           "Serviced_Organisation",           BUS_DIR),
        ("serviced_organisation_date_range.csv","Serviced_Organisation_Date_Range",BUS_DIR),
        ("service.csv",                         "Service",                         BUS_DIR),
        ("line.csv",                            "Line",                            BUS_DIR),
        ("route.csv",                           "Route",                           BUS_DIR),
        ("route_section.csv",                   "Route_Section",                   BUS_DIR),
        ("bus_stop_point.csv",                  "Bus_Stop_Point",                  BUS_DIR),
        ("route_link.csv",                      "Route_Link",                      BUS_DIR),
        ("route_location.csv",                  "Route_Location",                  BUS_DIR),
        ("journey_pattern.csv",                 "Journey_Pattern",                 BUS_DIR),
        ("journey_pattern_section.csv",         "Journey_Pattern_Section",         BUS_DIR),
        ("journey_pattern_link.csv",            "Journey_Pattern_Link",            BUS_DIR),
        ("vehicle_journey.csv",                 "Vehicle_Journey",                 BUS_DIR),
        ("days_of_week.csv",                    "Days_Of_Week",                    BUS_DIR),
        ("special_days_operation.csv",          "Special_Days_Operation",          BUS_DIR),
        ("bank_holiday_operation.csv",          "Bank_Holiday_Operation",          BUS_DIR),
        ("vehicle_journey_link.csv",            "Vehicle_Journey_Link",            BUS_DIR),
    ]

    for csv_name, table, csv_dir in nptg_tables + bus_tables:
        csv_cnt = _csv_row_count(csv_dir, csv_name)
        db_cnt  = _db_count(db, table)
        _check(
            f"{table:40s} CSV={csv_cnt:>10,}  DB={db_cnt:>10,}",
            csv_cnt == db_cnt,
            f"diff={db_cnt - csv_cnt:+,}" if csv_cnt != db_cnt else "",
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  2. Spot-check values — CSV rows vs DB
# ═══════════════════════════════════════════════════════════════════════════════
def test_spot_checks(db: TransportDatabase):
    section("2. Spot-check values — CSV row vs DB query")

    # ── Stop_Point by ATCO code ──────────────────────────────────────────
    csv_sp = _csv_rows(NPTG_DIR, "stop_point.csv", max_rows=5)
    for cs in csv_sp:
        atco = cs["atco_code"]
        ds = db.get_stop_by_code(atco)
        _check(f"Stop {atco} exists", ds is not None)
        if ds:
            _check(
                f"Stop {atco} name",
                ds["SP_name"] == cs["desc_common_name"],
                f'CSV="{cs["desc_common_name"]}" DB="{ds["SP_name"]}"',
            )
            csv_lat = cs["place_latitude"]
            db_lat  = str(ds["SP_latitude"]) if ds["SP_latitude"] else ""
            _check(
                f"Stop {atco} latitude",
                csv_lat == db_lat or (csv_lat and db_lat and abs(float(csv_lat) - float(db_lat)) < 0.0001),
                f'CSV="{csv_lat}" DB="{db_lat}"',
            )

    # ── Vehicle_Journey ──────────────────────────────────────────────────
    csv_vj = _csv_rows(BUS_DIR, "vehicle_journey.csv", max_rows=5)
    cur = db.conn.cursor()
    for cv in csv_vj:
        uid = cv["uid"]
        cur.execute("SELECT departure_time, VJ_code, JP_UID FROM Vehicle_Journey WHERE VJ_UID = ?", (uid,))
        row = cur.fetchone()
        _check(f"VJ UID={uid} exists", row is not None)
        if row:
            _check(
                f"VJ UID={uid} departure_time",
                row[0] == cv["departure_time"],
                f'CSV="{cv["departure_time"]}" DB="{row[0]}"',
            )

    # ── Days_Of_Week — verify bitmask ────────────────────────────────────
    csv_dow = _csv_rows(BUS_DIR, "days_of_week.csv", max_rows=5)
    for cd in csv_dow:
        uid = cd["uid"]
        cur.execute("SELECT DOW_days FROM Days_Of_Week WHERE DOW_UID = ?", (uid,))
        row = cur.fetchone()
        _check(f"DOW UID={uid} exists", row is not None)
        if row:
            day_keys = ["monday", "tuesday", "wednesday", "thursday",
                        "friday", "saturday", "sunday"]
            expected = sum(
                1 << i for i, k in enumerate(day_keys)
                if cd[k].lower() == "true"
            )
            _check(
                f"DOW UID={uid} bitmask",
                row[0] == expected,
                f"expected={expected} DB={row[0]}",
            )

    # ── Journey_Pattern_Link ─────────────────────────────────────────────
    csv_jpl = _csv_rows(BUS_DIR, "journey_pattern_link.csv", max_rows=5)
    for cj in csv_jpl:
        uid = cj["uid"]
        cur.execute(
            "SELECT JPL_from_point_atco_code, JPL_to_point_atco_code, JPL_run_time "
            "FROM Journey_Pattern_Link WHERE JPL_UID = ?", (uid,))
        row = cur.fetchone()
        _check(f"JPL UID={uid} exists", row is not None)
        if row:
            _check(
                f"JPL UID={uid} from_atco",
                row[0] == cj["from_point_point_id"],
                f'CSV="{cj["from_point_point_id"]}" DB="{row[0]}"',
            )
            _check(
                f"JPL UID={uid} to_atco",
                row[1] == cj["to_point_point_id"],
                f'CSV="{cj["to_point_point_id"]}" DB="{row[1]}"',
            )


# ═══════════════════════════════════════════════════════════════════════════════
#  3. get_stop_by_code — valid, invalid, SQL injection, edge cases
# ═══════════════════════════════════════════════════════════════════════════════
def test_get_stop_by_code(db: TransportDatabase):
    section("3. get_stop_by_code")

    # Valid stop (grab one from DB)
    cur = db.conn.cursor()
    cur.execute("SELECT SP_atco_code FROM Stop_Point LIMIT 1")
    row = cur.fetchone()
    if row:
        atco = row[0]
        result = db.get_stop_by_code(atco)
        _check(f"valid code '{atco}' → dict", result is not None and isinstance(result, dict))
        if result:
            _check("  has SP_atco_code key", "SP_atco_code" in result)
            _check("  has SP_name key", "SP_name" in result)
            _check("  has SP_latitude key", "SP_latitude" in result)

    # Invalid codes
    _check("nonexistent code → None", db.get_stop_by_code("XXXXXXXXXX") is None)
    _check("empty string → None", db.get_stop_by_code("") is None)
    _check("all zeros → None", db.get_stop_by_code("0000000000") is None)

    # SQL injection attempts
    injections = [
        "'; DROP TABLE Region; --",
        "1 OR 1=1",
        "' UNION SELECT * FROM Region --",
        "Robert'); DROP TABLE Stop_Point;--",
    ]
    for inj in injections:
        result = db.get_stop_by_code(inj)
        _check(f"SQL-injection → None  ({inj[:30]}…)", result is None)

    # Verify tables intact after injections
    cur.execute("SELECT COUNT(*) FROM Stop_Point")
    _check("Stop_Point table intact after injections", cur.fetchone()[0] > 0)

    # Unicode
    _check("unicode stop code → None", db.get_stop_by_code("日本語テスト") is None)

    # Very long string
    _check("1000-char string → None", db.get_stop_by_code("A" * 1000) is None)


# ═══════════════════════════════════════════════════════════════════════════════
#  4. get_stops_in_area — normal, empty, pole, large radius
# ═══════════════════════════════════════════════════════════════════════════════
def test_get_stops_in_area(db: TransportDatabase):
    section("4. get_stops_in_area")

    # Lancaster area — should have stops
    result = db.get_stops_in_area(54.0476, -2.8015, 1.0)
    _check("Lancaster 1km → non-empty list", isinstance(result, list) and len(result) > 0)
    if result:
        _check("  first result has SP_atco_code", "SP_atco_code" in result[0])
        _check("  first result has SP_latitude", "SP_latitude" in result[0])

    # Larger radius should return more stops
    result_5km = db.get_stops_in_area(54.0476, -2.8015, 5.0)
    _check("5km ≥ 1km stops", len(result_5km) >= len(result))

    # Middle of ocean — no stops
    _check("ocean (0,0) → []", db.get_stops_in_area(0.0, 0.0, 0.001) == [])

    # North pole
    _check("north pole (90,0) → []", db.get_stops_in_area(90.0, 0.0, 0.001) == [])

    # South pole
    _check("south pole (-90,0) → []", db.get_stops_in_area(-90.0, 0.0, 0.001) == [])

    # Zero radius
    result_zero = db.get_stops_in_area(54.0476, -2.8015, 0.0)
    _check("radius=0 → [] or very few", isinstance(result_zero, list))

    # Negative radius — bounding box inverts, should return empty
    result_neg = db.get_stops_in_area(54.0476, -2.8015, -1.0)
    _check("negative radius → []", result_neg == [])

    # Very large radius — should not crash
    result_big = db.get_stops_in_area(54.0476, -2.8015, 10000.0)
    _check("10000km radius → list (no crash)", isinstance(result_big, list))


# ═══════════════════════════════════════════════════════════════════════════════
#  5. get_stops_batch — normal, partial, empty, duplicates
# ═══════════════════════════════════════════════════════════════════════════════
def test_get_stops_batch(db: TransportDatabase):
    section("5. get_stops_batch")

    # Get a few real ATCO codes
    cur = db.conn.cursor()
    cur.execute("SELECT SP_atco_code FROM Stop_Point LIMIT 3")
    real_codes = [r[0] for r in cur.fetchall()]

    if len(real_codes) >= 2:
        # Normal batch
        result = db.get_stops_batch(real_codes)
        _check("batch of real codes → all found", len(result) == len(real_codes))
        for code in real_codes:
            _check(f"  {code} in result", code in result)

        # Mixed: some real, some fake
        mixed = [real_codes[0], "FAKECODE999", real_codes[1]]
        result = db.get_stops_batch(mixed)
        _check("mixed batch → only real codes returned", len(result) == 2)
        _check("  fake code absent", "FAKECODE999" not in result)

    # Empty list
    _check("empty list → {}", db.get_stops_batch([]) == {})

    # All fake codes
    result = db.get_stops_batch(["FAKE1", "FAKE2", "FAKE3"])
    _check("all fake codes → {}", result == {})

    # Duplicate codes
    if real_codes:
        result = db.get_stops_batch([real_codes[0], real_codes[0]])
        _check("duplicate codes → still just 1 entry", len(result) == 1)

    # SQL injection in batch
    result = db.get_stops_batch(["'; DROP TABLE Region; --"])
    _check("SQL injection in batch → {}", result == {})
    cur.execute("SELECT COUNT(*) FROM Region")
    _check("Region table intact after batch injection", cur.fetchone()[0] > 0)


# ═══════════════════════════════════════════════════════════════════════════════
#  6. get_route_stops_batch — normal, empty, invalid UIDs
# ═══════════════════════════════════════════════════════════════════════════════
def test_get_route_stops_batch(db: TransportDatabase):
    section("6. get_route_stops_batch")

    # Find a JP with links
    cur = db.conn.cursor()
    cur.execute("""
        SELECT jps.JP_UID, COUNT(*) as cnt
        FROM Journey_Pattern_Section jps
        JOIN Journey_Pattern_Link jpl ON jpl.JPS_UID = jps.JPS_UID
        GROUP BY jps.JP_UID
        ORDER BY cnt DESC
        LIMIT 2
    """)
    rows = cur.fetchall()

    if len(rows) >= 2:
        jp1, cnt1 = rows[0]
        jp2, cnt2 = rows[1]
        print(f"  (Using JP_UID={jp1} with {cnt1} links, JP_UID={jp2} with {cnt2} links)")

        # Single JP
        result = db.get_route_stops_batch([jp1])
        _check(f"single JP {jp1} → {len(result.get(jp1, []))} stops", len(result.get(jp1, [])) > 0)
        if result.get(jp1):
            s = result[jp1][0]
            _check("  has atco_code", "atco_code" in s)
            _check("  has SP_name", "SP_name" in s)
            _check("  has sequence", "sequence" in s)
            _check("  has SP_latitude", "SP_latitude" in s)

        # Batch of 2
        result = db.get_route_stops_batch([jp1, jp2])
        _check("batch of 2 JPs → both present", jp1 in result and jp2 in result)

        # Verify ordering: sequence numbers should be ascending
        stops = result.get(jp1, [])
        if len(stops) >= 2:
            seqs = [s["sequence"] for s in stops]
            _check("  stops ordered by sequence", seqs == sorted(seqs))

        # Mix of real and fake
        result = db.get_route_stops_batch([jp1, 999999999])
        _check("real + fake UID → real has data, fake is empty",
               len(result.get(jp1, [])) > 0 and result.get(999999999, []) == [])

    # Empty list
    _check("empty list → {}", db.get_route_stops_batch([]) == {})

    # All invalid UIDs (avoid 0 which may be a valid UID)
    result = db.get_route_stops_batch([-1, -999, 999999999])
    _check("all invalid UIDs → all empty lists",
           all(v == [] for v in result.values()))

    # Very large UID
    result = db.get_route_stops_batch([2**62])
    _check("2^62 UID → empty list", result.get(2**62, []) == [])


# ═══════════════════════════════════════════════════════════════════════════════
#  7. get_route_polylines_batch — normal, empty, fallback
# ═══════════════════════════════════════════════════════════════════════════════
def test_get_route_polylines_batch(db: TransportDatabase):
    section("7. get_route_polylines_batch")

    # Find a JP with Route_Location data
    cur = db.conn.cursor()
    cur.execute("""
        SELECT jps.JP_UID, COUNT(rl.RLOC_UID) AS loc_cnt
        FROM Journey_Pattern_Section jps
        JOIN Journey_Pattern_Link jpl ON jpl.JPS_UID = jps.JPS_UID
        JOIN Route_Location rl ON rl.RLIN_UID = jpl.RLIN_UID
        GROUP BY jps.JP_UID
        ORDER BY loc_cnt DESC
        LIMIT 1
    """)
    row = cur.fetchone()

    if row:
        jp_uid  = row[0]
        loc_cnt = row[1]
        print(f"  (Using JP_UID={jp_uid} with {loc_cnt} Route_Location rows)")

        result = db.get_route_polylines_batch([jp_uid])
        poly = result.get(jp_uid, [])
        _check(f"polyline for JP {jp_uid} is non-empty", len(poly) > 0)
        if poly:
            _check("  first point has 'lat' and 'long'", "lat" in poly[0] and "long" in poly[0])
            all_finite = all(
                isinstance(p.get("lat"), (int, float)) and
                isinstance(p.get("long"), (int, float))
                for p in poly
            )
            _check("  all points have numeric lat/long", all_finite)

    # JP with links but NO Route_Location (stop-only fallback)
    cur.execute("""
        SELECT jps.JP_UID
        FROM Journey_Pattern_Section jps
        JOIN Journey_Pattern_Link jpl ON jpl.JPS_UID = jps.JPS_UID
        LEFT JOIN Route_Location rl ON rl.RLIN_UID = jpl.RLIN_UID
        GROUP BY jps.JP_UID
        HAVING COUNT(rl.RLOC_UID) = 0
        LIMIT 1
    """)
    row_no_loc = cur.fetchone()
    if row_no_loc:
        jp_no = row_no_loc[0]
        result = db.get_route_polylines_batch([jp_no])
        poly_no = result.get(jp_no, [])
        _check(f"JP {jp_no} (no waypoints) → polyline from stops only", len(poly_no) > 0)
    else:
        print("  SKIP  All JPs have Route_Location data")

    # Empty list
    _check("empty list → {}", db.get_route_polylines_batch([]) == {})

    # Invalid UIDs
    result = db.get_route_polylines_batch([-1, 999999999])
    _check("invalid UIDs → empty polylines",
           all(v == [] for v in result.values()))

    # Batch: mix of real (with waypoints) and real (without)
    if row and row_no_loc:
        result = db.get_route_polylines_batch([row[0], row_no_loc[0]])
        _check("mixed batch → both JPs have polylines",
               len(result.get(row[0], [])) > 0 and len(result.get(row_no_loc[0], [])) > 0)


# ═══════════════════════════════════════════════════════════════════════════════
#  8. get_active_jp_for_stop — per-day, invalid inputs, DOW boundaries
# ═══════════════════════════════════════════════════════════════════════════════
def test_get_active_jp_for_stop(db: TransportDatabase):
    section("8. get_active_jp_for_stop")

    # Find a stop that has Vehicle_Journeys with DOW data
    cur = db.conn.cursor()
    cur.execute("""
        SELECT jpl.JPL_from_point_atco_code AS atco
        FROM Journey_Pattern_Link jpl
        INNER JOIN Journey_Pattern_Section jps ON jpl.JPS_UID = jps.JPS_UID
        INNER JOIN Journey_Pattern jp ON jps.JP_UID = jp.JP_UID
        INNER JOIN Vehicle_Journey vj ON vj.JP_UID = jp.JP_UID
        INNER JOIN Days_Of_Week dow ON dow.VJ_UID = vj.VJ_UID
        WHERE dow.DOW_days > 0
        LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        print("  SKIP  No Vehicle_Journey with DOW data found")
        return

    test_atco = row[0]
    print(f"  (Using stop {test_atco})")

    # Per-day test
    found_any = False
    for day in range(7):
        result = db.get_active_jp_for_stop(test_atco, day)
        _check(f"day={day} → {len(result):>4} (line, JP) pairs", isinstance(result, list))
        if result:
            found_any = True
            _check(f"  day={day} first row has 'line_name'",
                   "line_name" in result[0] and result[0]["line_name"])
            _check(f"  day={day} first row has 'JP_UID'",
                   "JP_UID" in result[0] and isinstance(result[0]["JP_UID"], int))

    _check("at least one day returns results", found_any)

    # Consistency: returned JP_UIDs actually serve this stop
    all_results = db.get_active_jp_for_stop(test_atco, 0)
    for r in all_results[:3]:
        stops = db.get_route_stops_batch([r["JP_UID"]])
        atco_codes = [s["atco_code"] for s in stops.get(r["JP_UID"], [])]
        _check(
            f"  JP {r['JP_UID']} passes through {test_atco}",
            test_atco in atco_codes,
            f"stops: {atco_codes[:3]}…" if test_atco not in atco_codes else "",
        )

    # ── Invalid inputs ────────────────────────────────────────────────────
    _check("nonexistent stop → []", db.get_active_jp_for_stop("NONEXISTENT_STOP", 0) == [])
    _check("empty string → []", db.get_active_jp_for_stop("", 0) == [])
    _check("whitespace-only → []", db.get_active_jp_for_stop("   ", 0) == [])
    _check("day_of_week=-1 → []", db.get_active_jp_for_stop(test_atco, -1) == [])
    _check("day_of_week=7 → []", db.get_active_jp_for_stop(test_atco, 7) == [])
    _check("day_of_week=99 → []", db.get_active_jp_for_stop(test_atco, 99) == [])

    # SQL injection
    _check("SQL injection → []",
           db.get_active_jp_for_stop("'; DROP TABLE Region; --", 0) == [])
    cur.execute("SELECT COUNT(*) FROM Region")
    _check("Region table intact after injection", cur.fetchone()[0] > 0)

    # DOW boundary: Monday=0 and Sunday=6
    mon = db.get_active_jp_for_stop(test_atco, 0)
    sun = db.get_active_jp_for_stop(test_atco, 6)
    _check("Monday result is list", isinstance(mon, list))
    _check("Sunday result is list", isinstance(sun, list))


# ═══════════════════════════════════════════════════════════════════════════════
#  9. get_upcoming_arrivals_batch — normal, empty, DOW/time edge cases
# ═══════════════════════════════════════════════════════════════════════════════
def test_get_upcoming_arrivals_batch(db: TransportDatabase):
    section("9. get_upcoming_arrivals_batch")

    # Find stops that have vehicle journeys
    cur = db.conn.cursor()
    cur.execute("""
        SELECT DISTINCT jpl.JPL_from_point_atco_code
        FROM Journey_Pattern_Link jpl
        INNER JOIN Journey_Pattern_Section jps ON jpl.JPS_UID = jps.JPS_UID
        INNER JOIN Vehicle_Journey vj ON vj.JP_UID = jps.JP_UID
        INNER JOIN Days_Of_Week dow ON dow.VJ_UID = vj.VJ_UID
        WHERE dow.DOW_days > 0
        LIMIT 5
    """)
    test_codes = [r[0] for r in cur.fetchall()]
    if not test_codes:
        print("  SKIP  No stops with vehicle journeys found")
        return

    print(f"  (Using {len(test_codes)} stops: {test_codes[:3]}…)")

    # Normal query — Monday morning
    result = db.get_upcoming_arrivals_batch(test_codes, 0, "08:00:00")
    _check("batch query returns dict", isinstance(result, dict))
    _check("all requested codes present as keys", all(c in result for c in test_codes))
    has_arrivals = any(len(v) > 0 for v in result.values())
    _check("at least one stop has arrivals", has_arrivals)

    if has_arrivals:
        # Check arrival structure
        for code, arrivals in result.items():
            if arrivals:
                a = arrivals[0]
                _check(f"  {code} arrival has departure_time", "departure_time" in a)
                _check(f"  {code} arrival has line_name", "line_name" in a)
                _check(f"  {code} arrival has operator_name", "operator_name" in a)
                break

    # Each day of week
    for day in range(7):
        result = db.get_upcoming_arrivals_batch(test_codes[:1], day, "12:00:00")
        _check(f"day={day} → list returned for stop", isinstance(result.get(test_codes[0]), list))

    # Time edge cases
    result_midnight = db.get_upcoming_arrivals_batch(test_codes[:1], 0, "00:00:00")
    _check("midnight query → dict", isinstance(result_midnight, dict))

    result_2359 = db.get_upcoming_arrivals_batch(test_codes[:1], 0, "23:59:59")
    _check("23:59:59 query → dict", isinstance(result_2359, dict))

    # Empty list of stops
    _check("empty stops → {}", db.get_upcoming_arrivals_batch([], 0, "08:00:00") == {})

    # All fake codes
    result = db.get_upcoming_arrivals_batch(["FAKE1", "FAKE2"], 0, "08:00:00")
    _check("fake codes → keys present but empty lists",
           all(result.get(c) == [] for c in ["FAKE1", "FAKE2"]))

    # Mix of real and fake
    mixed = [test_codes[0], "FAKESTOP999"]
    result = db.get_upcoming_arrivals_batch(mixed, 0, "08:00:00")
    _check("mixed batch → fake stop has empty arrivals",
           result.get("FAKESTOP999") == [])

    # SQL injection in stop codes
    result = db.get_upcoming_arrivals_batch(["'; DROP TABLE Region; --"], 0, "08:00:00")
    _check("SQL injection in batch → no crash", isinstance(result, dict))
    cur.execute("SELECT COUNT(*) FROM Region")
    _check("Region table intact after batch injection", cur.fetchone()[0] > 0)

    # Single stop — verify result keys match request
    single = db.get_upcoming_arrivals_batch([test_codes[0]], 0, "10:00:00")
    _check("single stop → exactly 1 key", len(single) == 1 and test_codes[0] in single)


# ═══════════════════════════════════════════════════════════════════════════════
#  10. Cross-table FK consistency
# ═══════════════════════════════════════════════════════════════════════════════
def test_fk_consistency(db: TransportDatabase):
    section("10. Cross-table FK consistency")
    cur = db.conn.cursor()

    fk_checks = [
        ("Authority → Region",
         "SELECT COUNT(*) FROM Authority a LEFT JOIN Region r ON a.Reg_code = r.Reg_code WHERE r.Reg_code IS NULL"),
        ("Locality → Authority",
         "SELECT COUNT(*) FROM Locality l LEFT JOIN Authority a ON l.Aut_admin_area_code = a.Aut_admin_area_code WHERE a.Aut_admin_area_code IS NULL"),
        ("Locality → District",
         "SELECT COUNT(*) FROM Locality l LEFT JOIN District d ON l.DIS_nptg_code = d.DIS_nptg_code WHERE d.DIS_nptg_code IS NULL"),
        ("Service → Operator",
         "SELECT COUNT(*) FROM Service s LEFT JOIN Operator o ON s.OPE_UID = o.OPE_UID WHERE o.OPE_UID IS NULL"),
        ("Line → Service",
         "SELECT COUNT(*) FROM Line l LEFT JOIN Service s ON l.SER_UID = s.SER_UID WHERE s.SER_UID IS NULL"),
        ("Vehicle_Journey → Journey_Pattern",
         "SELECT COUNT(*) FROM Vehicle_Journey vj LEFT JOIN Journey_Pattern jp ON vj.JP_UID = jp.JP_UID WHERE jp.JP_UID IS NULL"),
        ("Days_Of_Week → Vehicle_Journey",
         "SELECT COUNT(*) FROM Days_Of_Week d LEFT JOIN Vehicle_Journey vj ON d.VJ_UID = vj.VJ_UID WHERE vj.VJ_UID IS NULL"),
        ("VJL → Vehicle_Journey",
         "SELECT COUNT(*) FROM Vehicle_Journey_Link vjl LEFT JOIN Vehicle_Journey vj ON vjl.VJ_UID = vj.VJ_UID WHERE vj.VJ_UID IS NULL"),
        ("VJL → Journey_Pattern_Link",
         "SELECT COUNT(*) FROM Vehicle_Journey_Link vjl LEFT JOIN Journey_Pattern_Link jpl ON vjl.JPL_UID = jpl.JPL_UID WHERE jpl.JPL_UID IS NULL"),
        ("Bus_Stop_Point → Stop_Point",
         "SELECT COUNT(*) FROM Bus_Stop_Point bsp LEFT JOIN Stop_Point sp ON bsp.SP_atco_code = sp.SP_atco_code WHERE sp.SP_atco_code IS NULL"),
    ]

    for label, sql in fk_checks:
        cur.execute(sql)
        orphans = cur.fetchone()[0]
        _check(f"{label}: 0 orphans", orphans == 0, f"found {orphans}")

# ═══════════════════════════════════════════════════════════════════════════════
#  11. Extreme Batch Limits (SQLite 999 Variable Limit)
# ═══════════════════════════════════════════════════════════════════════════════
def test_sqlite_variable_limits(db: TransportDatabase):
    section("11. Extreme Batch Limits (SQLite Variables)")
    
    # Generate a massive list of fake codes exceeding the typical 999 SQLite limit
    massive_batch = [f"FAKE_ATCO_{i}" for i in range(1500)]
    
    try:
        result = db.get_stops_batch(massive_batch)
        _check("1500 item batch → Handled without 'too many variables' crash", True)
        _check("1500 item batch → Returns empty dict for fake codes", result == {})
    except Exception as e:
        _check("1500 item batch → Handled without 'too many variables' crash", False, f"Crashed: {e}")

    try:
        result_arrivals = db.get_upcoming_arrivals_batch(massive_batch, 0, "12:00:00")
        _check("1500 item arrivals batch → Handled without crash", True)
    except Exception as e:
        _check("1500 item arrivals batch → Handled without crash", False, f"Crashed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  12. Malformed Timestrings & Type Safety
# ═══════════════════════════════════════════════════════════════════════════════
def test_malformed_inputs(db: TransportDatabase):
    section("12. Malformed Inputs & Type Safety")
    
    # Grab a valid stop
    cur = db.conn.cursor()
    cur.execute("SELECT SP_atco_code FROM Stop_Point LIMIT 1")
    row = cur.fetchone()
    if not row:
        print("  SKIP  No stops available for malformed input tests.")
        return
    valid_stop = row[0]

    # Test bad time strings in upcoming arrivals
    bad_times = ["99:99:99", "not-a-time", "", "12:00", "25:00:00"]
    for bt in bad_times:
        result = db.get_upcoming_arrivals_batch([valid_stop], 0, bt)
        _check(f"Bad time format '{bt}' → returns safely (no crash)", isinstance(result, dict))
        
    # Test type mismatches
    try:
        # Passing an int instead of a string for ATCO code
        result = db.get_stop_by_code(123456) 
        _check("Int passed as ATCO code → handled safely", True)
    except Exception as e:
        _check("Int passed as ATCO code → handled safely", False, str(e))


# ═══════════════════════════════════════════════════════════════════════════════
#  13. Connection Lifecycle & Context Manager
# ═══════════════════════════════════════════════════════════════════════════════
def test_connection_lifecycle():
    section("13. Connection Lifecycle & State")
    
    db_test = TransportDatabase()
    
    # Test connecting twice
    try:
        db_test.connect()
        db_test.connect()  # Should not crash if already connected
        _check("Multiple connect() calls → handled safely", True)
    except Exception as e:
        _check("Multiple connect() calls → handled safely", False, str(e))
        
    # Test querying after manual close
    db_test.close()
    try:
        # According to your code, get_stop_by_code should auto-reconnect if self.conn is None
        result = db_test.get_stop_by_code("FAKE_CODE")
        _check("Auto-reconnects if queried after close()", db_test.conn is not None)
    except Exception as e:
        _check("Auto-reconnects if queried after close()", False, str(e))
        
    # Test double close
    try:
        db_test.close()
        db_test.close()
        _check("Multiple close() calls → handled safely", True)
    except Exception as e:
        _check("Multiple close() calls → handled safely", False, str(e))


# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    print("\n" + "=" * 60)
    print("  QUERY TEST SUITE")
    print("=" * 60)

    with TransportDatabase() as db:
        test_row_counts(db)
        test_spot_checks(db)
        test_get_stop_by_code(db)
        test_get_stops_in_area(db)
        test_get_stops_batch(db)
        test_get_route_stops_batch(db)
        test_get_route_polylines_batch(db)
        test_get_active_jp_for_stop(db)
        test_get_upcoming_arrivals_batch(db)
        test_fk_consistency(db)
        test_sqlite_variable_limits(db)
        test_malformed_inputs(db)
        test_connection_lifecycle() # Doesn't take 'db' because it creates its own to test state

    print(f"\n{'=' * 60}")
    print(f"  RESULTS:  {passed} passed,  {failed} failed,  {passed + failed} total")
    print(f"{'=' * 60}\n")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
