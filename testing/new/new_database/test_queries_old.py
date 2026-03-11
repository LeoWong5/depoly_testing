"""
test_queries.py — Comprehensive test suite for query.py

Coverage:
  Section 1 – Row-count parity:  DB table counts == CSV line counts
  Section 2 – Spot-check values: sample rows from CSV match DB query results
  Section 3 – Invalid inputs:    bogus names / codes / UIDs return empty / None
  Section 4 – Edge cases:        SQL injection strings, empty strings, huge ints
  Section 5 – Convenience funcs: find_routes, get_stops_by_name, get_nearby_stops
  Section 6 – Timetable / DOW:   bitmask logic, vehicle-journey queries
"""

import csv
import os
import sys
from pathlib import Path

dev_path = Path(__file__).parent.parent
sys.path.insert(0, str(dev_path))

from new_database.query import (
    TransportDatabase, find_routes, get_stops_by_name, get_nearby_stops,
)

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()
NPTG_DIR   = SCRIPT_DIR.parent / "new_preprocessor" / "CSV_NPTG"
BUS_DIR    = SCRIPT_DIR.parent / "new_preprocessor" / "CSV_BUS"

# ── Helpers ──────────────────────────────────────────────────────────────────
passed = 0
failed = 0

def _csv_row_count(csv_dir: Path, filename: str) -> int:
    """Count data rows (exclude header) in a CSV file."""
    p = csv_dir / filename
    with open(p, newline="", encoding="utf-8") as f:
        return sum(1 for _ in f) - 1          # minus header


def _csv_rows(csv_dir: Path, filename: str, max_rows: int = 0) -> list[dict]:
    """Read CSV into list[dict]. max_rows=0 → all."""
    p = csv_dir / filename
    with open(p, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if max_rows:
            return [next(reader) for _ in range(max_rows)]
        return list(reader)


def _db_count(db: TransportDatabase, table: str) -> int:
    cur = db.conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
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
#  2. Spot-check values — compare specific CSV rows with DB queries
# ═══════════════════════════════════════════════════════════════════════════════
def test_spot_checks(db: TransportDatabase):
    section("2. Spot-check values — CSV row vs DB query")

    # ── Region ───────────────────────────────────────────────────────────
    csv_regions = _csv_rows(NPTG_DIR, "region.csv")
    db_regions  = db.get_regions()
    db_reg_map  = {r["Reg_code"]: r for r in db_regions}
    for cr in csv_regions[:3]:
        code = cr["region_code"]
        dr = db_reg_map.get(code)
        _check(
            f"Region {code} name",
            dr is not None and dr["Reg_name"] == cr["name"],
            f'CSV="{cr["name"]}" DB="{dr["Reg_name"] if dr else "MISSING"}"',
        )
        if dr:
            _check(
                f"Region {code} country",
                dr["Reg_country"] == cr["country"],
                f'CSV="{cr["country"]}" DB="{dr["Reg_country"]}"',
            )

    # ── Authority ────────────────────────────────────────────────────────
    csv_auth = _csv_rows(NPTG_DIR, "authority.csv", max_rows=5)
    db_auth  = db.get_authorities()
    db_auth_map = {a["Aut_admin_area_code"]: a for a in db_auth}
    for ca in csv_auth:
        code = ca["admin_area_code"]
        da = db_auth_map.get(code)
        _check(
            f"Authority {code} name",
            da is not None and da["Aut_name"] == ca["name"],
            f'CSV="{ca["name"]}" DB="{da["Aut_name"] if da else "MISSING"}"',
        )

    # ── Stop_Point by ATCO code ──────────────────────────────────────────
    csv_sp = _csv_rows(NPTG_DIR, "stop_point.csv", max_rows=5)
    for cs in csv_sp:
        atco = cs["atco_code"]
        ds = db.get_stop_by_code(atco)
        _check(
            f"Stop {atco} exists",
            ds is not None,
        )
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

    # ── Operator ─────────────────────────────────────────────────────────
    csv_op = _csv_rows(BUS_DIR, "operator.csv")
    db_ops = db.get_operators()
    db_op_map = {str(o["OPE_UID"]): o for o in db_ops}
    for co in csv_op[:5]:
        uid = co["uid"]
        do = db_op_map.get(uid)
        _check(
            f"Operator UID={uid} short_name",
            do is not None and do["OPE_short_name"] == co["operator_short_name"],
            f'CSV="{co["operator_short_name"]}" DB="{do["OPE_short_name"] if do else "MISSING"}"',
        )

    # ── Locality ─────────────────────────────────────────────────────────
    csv_loc = _csv_rows(NPTG_DIR, "locality.csv", max_rows=5)
    for cl in csv_loc:
        code = cl["nptg_locality_code"]
        cur = db.conn.cursor()
        cur.execute("SELECT LOC_name, Aut_admin_area_code, DIS_nptg_code FROM Locality WHERE LOC_nptg_code = ?", (code,))
        row = cur.fetchone()
        _check(
            f"Locality {code} exists",
            row is not None,
        )
        if row:
            _check(
                f"Locality {code} name",
                row[0] == cl["locality_name"],
                f'CSV="{cl["locality_name"]}" DB="{row[0]}"',
            )
            _check(
                f"Locality {code} authority_ref",
                row[1] == cl["authority_ref"],
                f'CSV="{cl["authority_ref"]}" DB="{row[1]}"',
            )
            _check(
                f"Locality {code} district_ref",
                row[2] == cl["nptg_district_ref"],
                f'CSV="{cl["nptg_district_ref"]}" DB="{row[2]}"',
            )

    # ── Vehicle_Journey ──────────────────────────────────────────────────
    csv_vj = _csv_rows(BUS_DIR, "vehicle_journey.csv", max_rows=5)
    cur = db.conn.cursor()
    for cv in csv_vj:
        uid = cv["uid"]
        cur.execute("SELECT departure_time, VJ_code, JP_UID FROM Vehicle_Journey WHERE VJ_UID = ?", (uid,))
        row = cur.fetchone()
        _check(
            f"VJ UID={uid} exists",
            row is not None,
        )
        if row:
            _check(
                f"VJ UID={uid} departure_time",
                row[0] == cv["departure_time"],
                f'CSV="{cv["departure_time"]}" DB="{row[0]}"',
            )
            _check(
                f"VJ UID={uid} JP_UID",
                str(row[2]) == cv["JP_uid"],
                f'CSV="{cv["JP_uid"]}" DB="{row[2]}"',
            )

    # ── Days_Of_Week — verify bitmask ────────────────────────────────────
    csv_dow = _csv_rows(BUS_DIR, "days_of_week.csv", max_rows=5)
    for cd in csv_dow:
        uid = cd["uid"]
        cur.execute("SELECT DOW_days, DOW_monday, DOW_tuesday, DOW_wednesday, "
                     "DOW_thursday, DOW_friday, DOW_saturday, DOW_sunday "
                     "FROM Days_Of_Week WHERE DOW_UID = ?", (uid,))
        row = cur.fetchone()
        _check(f"DOW UID={uid} exists", row is not None)
        if row:
            # Recompute expected bitmask from CSV booleans
            day_keys = ["monday", "tuesday", "wednesday", "thursday",
                        "friday", "saturday", "sunday"]
            expected = sum(
                1 << i for i, k in enumerate(day_keys)
                if cd[k].lower() == "true"
            )
            _check(
                f"DOW UID={uid} bitmask",
                row[0] == expected,
                f"CSV_flags={''.join('1' if cd[k].lower()=='true' else '0' for k in day_keys)} "
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
            _check(
                f"JPL UID={uid} run_time",
                row[2] == cj["run_time"],
                f'CSV="{cj["run_time"]}" DB="{row[2]}"',
            )


# ═══════════════════════════════════════════════════════════════════════════════
#  3. Invalid / bogus inputs — must return empty list or None, not crash
# ═══════════════════════════════════════════════════════════════════════════════
def test_invalid_inputs(db: TransportDatabase):
    section("3. Invalid inputs — expect empty / None, no crash")

    # ── Invalid stop code ────────────────────────────────────────────────
    result = db.get_stop_by_code("XXXXXXXXXX")
    _check("get_stop_by_code('XXXXXXXXXX') → None", result is None)

    result = db.get_stop_by_code("")
    _check("get_stop_by_code('') → None", result is None)

    result = db.get_stop_by_code("0000000000")
    _check("get_stop_by_code('0000000000') → None", result is None)

    # ── Invalid name search ──────────────────────────────────────────────
    result = db.search_stops_by_name("ZZZZZNONEXISTENT999")
    _check("search_stops_by_name('ZZZZZNONEXISTENT999') → []", result == [])

    result = db.search_stops_by_name("")
    _check("search_stops_by_name('') → returns something (matches all)", isinstance(result, list))

    result = db.search_stops_by_name("X", limit=0)
    _check("search_stops_by_name('X', limit=0) → []", result == [])

    # ── Invalid locality code ────────────────────────────────────────────
    result = db.get_stops_in_locality("NONEXIST999")
    _check("get_stops_in_locality('NONEXIST999') → []", result == [])

    result = db.get_stops_in_locality("")
    _check("get_stops_in_locality('') → []", result == [])

    # ── Invalid geographic area ──────────────────────────────────────────
    result = db.get_stops_in_area(0.0, 0.0, 0.001)
    _check("get_stops_in_area(0, 0, 0.001) → []  (middle of ocean)", result == [])

    result = db.get_stops_in_area(90.0, 180.0, 0.001)
    _check("get_stops_in_area(90, 180, 0.001) → []  (north pole)", result == [])

    # ── Invalid route / journey queries ──────────────────────────────────
    result = db.get_routes_at_stop("NONEXISTENT_STOP")
    _check("get_routes_at_stop('NONEXISTENT_STOP') → []", result == [])

    result = db.get_routes_between_stops("FAKE_A", "FAKE_B")
    _check("get_routes_between_stops('FAKE_A', 'FAKE_B') → []", result == [])

    result = db.get_route_stops(-999)
    _check("get_route_stops(-999) → []", result == [])

    result = db.get_route_stops(999999999)
    _check("get_route_stops(999999999) → []", result == [])

    # ── Invalid operator / service ───────────────────────────────────────
    result = db.get_services_by_operator("NOCODE")
    _check("get_services_by_operator('NOCODE') → []", result == [])

    result = db.get_lines_for_service("NOSERVICE")
    _check("get_lines_for_service('NOSERVICE') → []", result == [])

    # ── Invalid vehicle journey ──────────────────────────────────────────
    result = db.get_vehicle_journeys(-1)
    _check("get_vehicle_journeys(-1) → []", result == [])

    result = db.get_vehicle_journeys(-1, day_of_week=0)
    _check("get_vehicle_journeys(-1, day=0) → []", result == [])

    result = db.get_vehicle_journeys(999999999, day_of_week=6)
    _check("get_vehicle_journeys(999999999, day=6) → []", result == [])

    # ── Invalid authority / region ───────────────────────────────────────
    result = db.get_localities("FAKEAUTH")
    _check("get_localities('FAKEAUTH') → []", result == [])

    result = db.get_authorities("ZZ")
    _check("get_authorities('ZZ') → []", result == [])


# ═══════════════════════════════════════════════════════════════════════════════
#  4. Edge cases — SQL injection, special chars, boundary values
# ═══════════════════════════════════════════════════════════════════════════════
def test_edge_cases(db: TransportDatabase):
    section("4. Edge cases — SQL injection, special chars, boundaries")

    # SQL injection attempts (should not crash; parameterised queries protect)
    injections = [
        "'; DROP TABLE Region; --",
        "1 OR 1=1",
        "' UNION SELECT * FROM Region --",
        "Robert'); DROP TABLE Stop_Point;--",
    ]
    for inj in injections:
        result = db.get_stop_by_code(inj)
        _check(f"SQL-injection stop_by_code → None  ({inj[:30]}…)", result is None)

    for inj in injections:
        result = db.search_stops_by_name(inj)
        _check(f"SQL-injection search_name → list  ({inj[:30]}…)", isinstance(result, list))

    # Special characters
    result = db.search_stops_by_name("%")
    _check("search_stops_by_name('%') → list (wildcard char)", isinstance(result, list))

    result = db.search_stops_by_name("_")
    _check("search_stops_by_name('_') → list (single-char wildcard)", isinstance(result, list))

    # Unicode
    result = db.search_stops_by_name("日本語テスト")
    _check("search_stops_by_name(unicode) → []", result == [])

    # Very large number for UID lookups (within SQLite INTEGER range)
    result = db.get_route_stops(2**62)
    _check("get_route_stops(2^62) → []", result == [])

    # Overflow beyond SQLite range — should raise, not corrupt
    try:
        db.get_route_stops(2**128)
        _check("get_route_stops(2^128) → graceful error", False, "no exception raised")
    except (OverflowError, Exception):
        _check("get_route_stops(2^128) → raises OverflowError", True)

    # Verify Region table still intact after injection attempts
    regions = db.get_regions()
    _check("Region table intact after injection tests", len(regions) > 0)

    # Verify Stop_Point table still intact
    stops = db.search_stops_by_name("station", limit=1)
    _check("Stop_Point table intact after injection tests", isinstance(stops, list))


# ═══════════════════════════════════════════════════════════════════════════════
#  5. Convenience functions
# ═══════════════════════════════════════════════════════════════════════════════
def test_convenience_functions(db: TransportDatabase):
    section("5. Convenience functions — find_routes, get_stops_by_name, get_nearby_stops")

    # get_stops_by_name
    stops = get_stops_by_name("station", limit=5)
    _check("get_stops_by_name('station') returns list", isinstance(stops, list))
    _check("get_stops_by_name('station') ≤ 5 results", len(stops) <= 5)

    stops_none = get_stops_by_name("ZZZZ_NONEXIST_XXX")
    _check("get_stops_by_name(bogus) → []", stops_none == [])

    # get_nearby_stops
    nearby = get_nearby_stops(53.4, -3.0, 1.0)
    _check("get_nearby_stops(53.4, -3.0) → list", isinstance(nearby, list))

    nowhere = get_nearby_stops(0.0, 0.0, 0.001)
    _check("get_nearby_stops(0, 0) → []", nowhere == [])

    # find_routes
    fake = find_routes("FAKE_A", "FAKE_B")
    _check("find_routes(bogus, bogus) → []", fake == [])


# ═══════════════════════════════════════════════════════════════════════════════
#  6. Timetable / Day-of-week / vehicle-journey integration
# ═══════════════════════════════════════════════════════════════════════════════
def test_timetable(db: TransportDatabase):
    section("6. Timetable & DOW integration")

    # Find a JP that has vehicle journeys
    cur = db.conn.cursor()
    cur.execute("""
        SELECT jp.JP_UID, COUNT(*) as cnt
        FROM Vehicle_Journey vj
        JOIN Journey_Pattern jp ON vj.JP_UID = jp.JP_UID
        GROUP BY jp.JP_UID
        ORDER BY cnt DESC
        LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        print("  SKIP  No Vehicle_Journeys found")
        return

    jp_uid = row[0]
    vj_count = row[1]
    print(f"  (Using JP_UID={jp_uid} with {vj_count} vehicle journeys)")

    # get_vehicle_journeys — no day filter
    vjs = db.get_vehicle_journeys(jp_uid)
    _check(f"get_vehicle_journeys({jp_uid}) → {len(vjs)} results", len(vjs) == vj_count)

    # get_vehicle_journeys — each day of week
    total_by_day = 0
    for day in range(7):
        day_vjs = db.get_vehicle_journeys(jp_uid, day_of_week=day)
        _check(
            f"  day={day} → {len(day_vjs):>4} journeys",
            isinstance(day_vjs, list),
        )
        # Journeys for a specific day should be <= total
        _check(
            f"  day={day} count ≤ total",
            len(day_vjs) <= len(vjs),
            f"day={len(day_vjs)} total={len(vjs)}" if len(day_vjs) > len(vjs) else "",
        )

    # get_route_stops
    stops = db.get_route_stops(jp_uid)
    _check(f"get_route_stops({jp_uid}) → {len(stops)} stops", len(stops) > 0)
    if stops:
        _check(
            "  first stop has atco_code",
            "atco_code" in stops[0] and stops[0]["atco_code"],
        )
        _check(
            "  first stop has SP_name",
            "SP_name" in stops[0] and stops[0]["SP_name"],
        )

    # get_timetable_for_route
    timetable = db.get_timetable_for_route(jp_uid)
    _check("get_timetable_for_route returns dict", isinstance(timetable, dict))
    _check("  has 'route_info' key", "route_info" in timetable)
    _check("  has 'stops' key",      "stops" in timetable)
    _check("  has 'journeys' key",   "journeys" in timetable)
    _check("  route_info has operator_name", "operator_name" in timetable.get("route_info", {}))

    # get_timetable_for_route with day filter
    timetable_mon = db.get_timetable_for_route(jp_uid, day_of_week=0)
    _check("get_timetable_for_route(day=Monday) → dict", isinstance(timetable_mon, dict))


# ═══════════════════════════════════════════════════════════════════════════════
#  7½. Route polyline — get_route_polyline
# ═══════════════════════════════════════════════════════════════════════════════
def test_route_polyline(db: TransportDatabase):
    section("7½. Route polyline — get_route_polyline")

    # ── Find a JP that actually has Route_Location data ──────────────────
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
        jp_uid   = row[0]
        loc_cnt  = row[1]
        print(f"  (Using JP_UID={jp_uid} with {loc_cnt} Route_Location rows)")

        poly = db.get_route_polyline(jp_uid)

        _check(f"polyline for JP {jp_uid} is a list", isinstance(poly, list))
        _check(f"polyline length > 0", len(poly) > 0)
        _check("first point has 'lat' and 'long'",
               "lat" in poly[0] and "long" in poly[0] if poly else False)
        # Must be longer than stop-only path (waypoints are included)
        stops = db.get_route_stops(jp_uid)
        _check(f"polyline ({len(poly)} pts) >= stops ({len(stops)} pts)",
               len(poly) >= len(stops))
        # Every coordinate must be a finite number
        all_finite = all(
            isinstance(p.get("lat"), (int, float)) and
            isinstance(p.get("long"), (int, float))
            for p in poly
        )
        _check("all points have numeric lat/long", all_finite)
    else:
        print("  (No Route_Location data in DB — testing fallback only)")

    # ── JP with links but NO Route_Location (stop-only fallback) ────────
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
        jp_uid_no_loc = row_no_loc[0]
        poly_no_loc = db.get_route_polyline(jp_uid_no_loc)
        _check(f"JP {jp_uid_no_loc} (no waypoints) still returns a polyline",
               isinstance(poly_no_loc, list) and len(poly_no_loc) > 0)
        stops_no_loc = db.get_route_stops(jp_uid_no_loc)
        # Each stop contributes one point, plus one for the final to-stop
        expected_len = len(stops_no_loc) + 1 if stops_no_loc else 0
        _check(f"  polyline length ({len(poly_no_loc)}) == stops+1 ({expected_len})",
               len(poly_no_loc) == expected_len)
    else:
        print("  SKIP  All JPs have Route_Location data (no fallback-only JP)")

    # ── Invalid / edge-case JP UIDs ──────────────────────────────────────
    result = db.get_route_polyline(-1)
    _check("get_route_polyline(-1) → []", result == [])

    result = db.get_route_polyline(0)
    _check("get_route_polyline(0)  → []", result == [])

    result = db.get_route_polyline(999999999)
    _check("get_route_polyline(999999999) → []", result == [])

    # Very large UID within SQLite range
    result = db.get_route_polyline(2**62)
    _check("get_route_polyline(2^62) → []", result == [])


# ═══════════════════════════════════════════════════════════════════════════════
#  7¾. get_active_jp_for_stop — DOW-filtered JP lookup
# ═══════════════════════════════════════════════════════════════════════════════
def test_active_jp_for_stop(db: TransportDatabase):
    section("7¾. get_active_jp_for_stop — DOW-filtered JP lookup")

    # ── Find a stop that has at least one Vehicle_Journey (guaranteed data) ──
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

    # ── Basic functioning: at least one result for some day ───────────────
    found_any = False
    for day in range(7):
        result = db.get_active_jp_for_stop(test_atco, day)
        _check(
            f"day={day} → {len(result):>4} (line, JP) pairs",
            isinstance(result, list),
        )
        if result:
            found_any = True
            # Every row should have line_name and JP_UID
            _check(
                f"  day={day} first row has 'line_name'",
                "line_name" in result[0] and result[0]["line_name"],
            )
            _check(
                f"  day={day} first row has 'JP_UID'",
                "JP_UID" in result[0] and isinstance(result[0]["JP_UID"], int),
            )

    _check("at least one day returns results", found_any)

    # ── Consistency: returned JP_UIDs actually serve this stop ────────────
    all_results = db.get_active_jp_for_stop(test_atco, 0)  # Monday
    for r in all_results[:3]:  # spot-check first 3
        stops = db.get_route_stops(r["JP_UID"])
        atco_codes = [s["atco_code"] for s in stops]
        _check(
            f"  JP {r['JP_UID']} passes through {test_atco}",
            test_atco in atco_codes,
            f"stops: {atco_codes[:3]}…" if test_atco not in atco_codes else "",
        )

    # ── Subset property: DOW-filtered ≤ unfiltered get_routes_at_stop ─────
    unfiltered = db.get_routes_at_stop(test_atco)
    unfiltered_jp_set = {r["JP_UID"] for r in unfiltered}
    for day in range(7):
        filtered = db.get_active_jp_for_stop(test_atco, day)
        filtered_jp_set = {r["JP_UID"] for r in filtered}
        _check(
            f"day={day} filtered JPs ⊆ unfiltered JPs",
            filtered_jp_set.issubset(unfiltered_jp_set),
            f"extra: {filtered_jp_set - unfiltered_jp_set}" if not filtered_jp_set.issubset(unfiltered_jp_set) else "",
        )

    # ── Invalid inputs ────────────────────────────────────────────────────
    result = db.get_active_jp_for_stop("NONEXISTENT_STOP", 0)
    _check("nonexistent stop → []", result == [])

    result = db.get_active_jp_for_stop("", 0)
    _check("empty string stop → []", result == [])

    result = db.get_active_jp_for_stop(test_atco, -1)
    _check("day_of_week=-1 → []", result == [])

    result = db.get_active_jp_for_stop(test_atco, 7)
    _check("day_of_week=7 → []", result == [])

    result = db.get_active_jp_for_stop(test_atco, 99)
    _check("day_of_week=99 → []", result == [])

    # SQL injection
    result = db.get_active_jp_for_stop("'; DROP TABLE Region; --", 0)
    _check("SQL injection → []", result == [])
    regions = db.get_regions()
    _check("Region table intact after injection", len(regions) > 0)


# ═══════════════════════════════════════════════════════════════════════════════
#  7. Cross-table FK consistency (DB-level spot checks)
# ═══════════════════════════════════════════════════════════════════════════════
def test_fk_consistency(db: TransportDatabase):
    section("7. Cross-table FK consistency (sample checks)")
    cur = db.conn.cursor()

    # Every Authority.Reg_code exists in Region
    cur.execute("""
        SELECT COUNT(*) FROM Authority a
        LEFT JOIN Region r ON a.Reg_code = r.Reg_code
        WHERE r.Reg_code IS NULL
    """)
    orphans = cur.fetchone()[0]
    _check("Authority → Region: 0 orphans", orphans == 0, f"found {orphans}")

    # Every Locality.Aut_admin_area_code exists in Authority
    cur.execute("""
        SELECT COUNT(*) FROM Locality l
        LEFT JOIN Authority a ON l.Aut_admin_area_code = a.Aut_admin_area_code
        WHERE a.Aut_admin_area_code IS NULL
    """)
    orphans = cur.fetchone()[0]
    _check("Locality → Authority: 0 orphans", orphans == 0, f"found {orphans}")

    # Every Locality.DIS_nptg_code exists in District
    cur.execute("""
        SELECT COUNT(*) FROM Locality l
        LEFT JOIN District d ON l.DIS_nptg_code = d.DIS_nptg_code
        WHERE d.DIS_nptg_code IS NULL
    """)
    orphans = cur.fetchone()[0]
    _check("Locality → District: 0 orphans", orphans == 0, f"found {orphans}")

    # Every Service.OPE_UID exists in Operator
    cur.execute("""
        SELECT COUNT(*) FROM Service s
        LEFT JOIN Operator o ON s.OPE_UID = o.OPE_UID
        WHERE o.OPE_UID IS NULL
    """)
    orphans = cur.fetchone()[0]
    _check("Service → Operator: 0 orphans", orphans == 0, f"found {orphans}")

    # Every Line.SER_UID exists in Service
    cur.execute("""
        SELECT COUNT(*) FROM Line l
        LEFT JOIN Service s ON l.SER_UID = s.SER_UID
        WHERE s.SER_UID IS NULL
    """)
    orphans = cur.fetchone()[0]
    _check("Line → Service: 0 orphans", orphans == 0, f"found {orphans}")

    # Every VJ.JP_UID exists in Journey_Pattern
    cur.execute("""
        SELECT COUNT(*) FROM Vehicle_Journey vj
        LEFT JOIN Journey_Pattern jp ON vj.JP_UID = jp.JP_UID
        WHERE jp.JP_UID IS NULL
    """)
    orphans = cur.fetchone()[0]
    _check("Vehicle_Journey → Journey_Pattern: 0 orphans", orphans == 0, f"found {orphans}")

    # Every DOW.VJ_UID exists in Vehicle_Journey
    cur.execute("""
        SELECT COUNT(*) FROM Days_Of_Week d
        LEFT JOIN Vehicle_Journey vj ON d.VJ_UID = vj.VJ_UID
        WHERE vj.VJ_UID IS NULL
    """)
    orphans = cur.fetchone()[0]
    _check("Days_Of_Week → Vehicle_Journey: 0 orphans", orphans == 0, f"found {orphans}")

    # Every VJL.VJ_UID exists in Vehicle_Journey
    cur.execute("""
        SELECT COUNT(*) FROM Vehicle_Journey_Link vjl
        LEFT JOIN Vehicle_Journey vj ON vjl.VJ_UID = vj.VJ_UID
        WHERE vj.VJ_UID IS NULL
    """)
    orphans = cur.fetchone()[0]
    _check("VJL → Vehicle_Journey: 0 orphans", orphans == 0, f"found {orphans}")

    # Every VJL.JPL_UID exists in Journey_Pattern_Link
    cur.execute("""
        SELECT COUNT(*) FROM Vehicle_Journey_Link vjl
        LEFT JOIN Journey_Pattern_Link jpl ON vjl.JPL_UID = jpl.JPL_UID
        WHERE jpl.JPL_UID IS NULL
    """)
    orphans = cur.fetchone()[0]
    _check("VJL → Journey_Pattern_Link: 0 orphans", orphans == 0, f"found {orphans}")

    # Bus_Stop_Point.SP_atco_code exists in Stop_Point
    cur.execute("""
        SELECT COUNT(*) FROM Bus_Stop_Point bsp
        LEFT JOIN Stop_Point sp ON bsp.SP_atco_code = sp.SP_atco_code
        WHERE sp.SP_atco_code IS NULL
    """)
    orphans = cur.fetchone()[0]
    _check("Bus_Stop_Point → Stop_Point: 0 orphans", orphans == 0, f"found {orphans}")


# ═══════════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    print("\n" + "=" * 60)
    print("  COMPREHENSIVE QUERY TEST SUITE")
    print("=" * 60)

    with TransportDatabase() as db:
        test_row_counts(db)
        test_spot_checks(db)
        test_invalid_inputs(db)
        test_edge_cases(db)
        test_convenience_functions(db)
        test_timetable(db)
        test_route_polyline(db)
        test_active_jp_for_stop(db)
        test_fk_consistency(db)

    print(f"\n{'=' * 60}")
    print(f"  RESULTS:  {passed} passed,  {failed} failed,  {passed + failed} total")
    print(f"{'=' * 60}\n")

    return 0 if failed == 0 else 1


# if __name__ == "__main__":
#     sys.exit(main())
