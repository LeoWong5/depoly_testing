"""
load_data.py — Load pre-filtered CSV_NPTG and CSV_BUS into SQLite.

The CSV data is already validated by filter.py / detector.py.
This script only does column-rename, index→code mapping where the CSV
stores positional indices, sentinel→NULL conversion, and bulk INSERT.
NO FK re-filtering is performed.
"""

import sqlite3
import os

import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
DB_PATH     = os.path.join(SCRIPT_DIR, "nptg_naptan.db")
NPTG_DIR    = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "new_preprocessor", "CSV_NPTG"))
BUS_DIR     = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "new_preprocessor", "CSV_BUS"))


# ── Helpers ──────────────────────────────────────────────────────────────────

def _read(name: str, csv_dir: str) -> pd.DataFrame | None:
    path = os.path.join(csv_dir, name)
    if not os.path.exists(path):
        print(f"  [MISSING] {path}")
        return None
    df = pd.read_csv(path, keep_default_na=False, dtype=str, low_memory=False)
    return df


def _to_sql(conn: sqlite3.Connection, df: pd.DataFrame | None, table: str) -> None:
    if df is None or df.empty:
        print(f"  Skip {table}: no data")
        return
    # Replace empty strings / "None" / "nan" with real None (NULL)
    df = df.replace({"": None, "None": None, "nan": None})
    df.to_sql(table, conn, if_exists="append", index=False, chunksize=5000)
    print(f"  Loaded {len(df):>10,} rows into {table}")


def _sentinel_to_null(df: pd.DataFrame, col: str, sentinels: tuple = ("-1",)) -> None:
    """In-place: replace sentinel strings with None."""
    df.loc[df[col].isin(sentinels), col] = None


def _expand_range(parent_df: pd.DataFrame, uid_col: str,
                  start_col: str, end_col: str) -> dict:
    """Build child_uid → parent_uid dict from parent's [start, end] ranges."""
    mapping: dict = {}
    for _, row in parent_df.iterrows():
        s_str, e_str = row[start_col], row[end_col]
        if s_str in (None, "", "-1") or e_str in (None, "", "-1"):
            continue
        parent_uid = row[uid_col]
        for child_uid in range(int(s_str), int(e_str) + 1):
            mapping[child_uid] = parent_uid
    return mapping


# ═══════════════════════════════════════════════════════════════════════════════
#  NPTG loading
# ═══════════════════════════════════════════════════════════════════════════════

def _load_nptg(conn: sqlite3.Connection) -> None:
    print("\n══════════════════════════════════════════════")
    print("  Loading NPTG …")
    print("══════════════════════════════════════════════\n")

    region_df     = _read("region.csv",     NPTG_DIR)
    authority_df  = _read("authority.csv",  NPTG_DIR)
    district_df   = _read("district.csv",   NPTG_DIR)
    locality_df   = _read("locality.csv",   NPTG_DIR)
    stop_area_df  = _read("stop_area.csv",  NPTG_DIR)
    stop_point_df = _read("stop_point.csv", NPTG_DIR)

    # ── 1. Region ────────────────────────────────────────────────────────
    if region_df is not None:
        region_out = region_df.rename(columns={
            "region_code": "Reg_code",
            "name":        "Reg_name",
            "country":     "Reg_country",
        })[["Reg_code", "Reg_name", "Reg_country"]]
        _to_sql(conn, region_out, "Region")

    # ── 2. Authority  (parent_index → Region.region_code) ────────────────
    if authority_df is not None and region_df is not None:
        authority_df = authority_df.copy()
        idx_to_reg = dict(enumerate(region_df["region_code"]))
        authority_df["Reg_code"] = (
            authority_df["parent_index"].astype(int).map(idx_to_reg)
        )
        authority_out = authority_df.rename(columns={
            "admin_area_code": "Aut_admin_area_code",
            "atco_area_code":  "Aut_atco_area_code",
            "name":            "Aut_name",
            "short_name":      "Aut_short_name",
            "national":        "Aut_national",
        })[["Aut_admin_area_code", "Reg_code", "Aut_atco_area_code",
            "Aut_name", "Aut_short_name", "Aut_national"]]
        _to_sql(conn, authority_out, "Authority")

    # ── 3. District  (parent_index → Authority.admin_area_code) ──────────
    if district_df is not None and authority_df is not None:
        district_df = district_df.copy()
        idx_to_auth = dict(enumerate(authority_df["admin_area_code"]))
        district_df["Aut_admin_area_code"] = (
            district_df["parent_index"].astype(int).map(idx_to_auth)
        )
        district_out = district_df.rename(columns={
            "nptg_district_code": "DIS_nptg_code",
            "name":               "DIS_name",
        })[["DIS_nptg_code", "Aut_admin_area_code", "DIS_name"]]
        _to_sql(conn, district_out, "District")

    # ── 4. Locality ──────────────────────────────────────────────────────
    #   authority_ref = admin_area_code  (NOT an index)
    #   nptg_district_ref = district_code  (NOT an index)
    #   parent_nptg_locality_ref = self-referencing locality code (nullable)
    if locality_df is not None:
        locality_df = locality_df.copy()
        locality_df.loc[
            locality_df["parent_nptg_locality_ref"].isin(["", "None", "nan"]),
            "parent_nptg_locality_ref",
        ] = None

        locality_out = locality_df.rename(columns={
            "nptg_locality_code":       "LOC_nptg_code",
            "parent_nptg_locality_ref": "LOC_parent_locality_ref",
            "authority_ref":            "Aut_admin_area_code",
            "nptg_district_ref":        "DIS_nptg_code",
            "locality_name":            "LOC_name",
            "qualifier_name":           "LOC_qualifier_name",
            "locality_type":            "LOC_type",
            "longitude":                "LOC_longitude",
            "latitude":                 "LOC_latitude",
        })[["LOC_nptg_code", "LOC_parent_locality_ref", "Aut_admin_area_code",
            "DIS_nptg_code", "LOC_name", "LOC_qualifier_name", "LOC_type",
            "LOC_longitude", "LOC_latitude"]]

        # Self-referencing FK → insert in passes (roots first, then children)
        roots = locality_out[locality_out["LOC_parent_locality_ref"].isna()].copy()
        _to_sql(conn, roots, "Locality")

        remaining = locality_out[locality_out["LOC_parent_locality_ref"].notna()].copy()
        cur = conn.cursor()
        for pass_num in range(1, 10):
            existing = {r[0] for r in cur.execute("SELECT LOC_nptg_code FROM Locality").fetchall()}
            can_insert = remaining[remaining["LOC_parent_locality_ref"].isin(existing)]
            if can_insert.empty:
                break
            _to_sql(conn, can_insert, "Locality")
            remaining = remaining[~remaining["LOC_nptg_code"].isin(can_insert["LOC_nptg_code"])]

        if len(remaining) > 0:
            print(f"  ⚠️  Locality orphans (dangling parent ref): {len(remaining)}")

    # ── 5. Stop_Area ─────────────────────────────────────────────────────
    if stop_area_df is not None:
        stop_area_out = stop_area_df.rename(columns={
            "stop_area_code":     "SA_code",
            "nptg_authority_ref": "Aut_admin_area_code",
            "name":               "SA_name",
            "stop_area_type":     "SA_type",
            "longitude":          "SA_longitude",
            "latitude":           "SA_latitude",
        })[["SA_code", "Aut_admin_area_code", "SA_name", "SA_type",
            "SA_longitude", "SA_latitude"]]
        _to_sql(conn, stop_area_out, "Stop_Area")

    # ── 6. Stop_Point ───────────────────────────────────────────────────
    #   nptg_authority_ref = admin_area_code (NOT an index)
    if stop_point_df is not None and authority_df is not None:
        stop_point_df = stop_point_df.copy()
        admin_to_atco = dict(zip(
            authority_df["admin_area_code"],
            authority_df["atco_area_code"],
        ))
        stop_point_df["Aut_atco_area_code"] = (
            stop_point_df["nptg_authority_ref"].map(admin_to_atco)
        )
        stop_point_df.loc[
            stop_point_df["stop_areas_ref"].isin(["", "None", "nan"]),
            "stop_areas_ref",
        ] = None

        stop_point_out = stop_point_df.rename(columns={
            "atco_code":               "SP_atco_code",
            "nptg_authority_ref":      "Aut_admin_area_code",
            "naptan_code":             "SP_naptan_code",
            "plus_bus_zone_ref":       "SP_plus_bus_zone_ref",
            "stop_areas_ref":          "SA_code",
            "stop_start_date":         "SP_start_date",
            "desc_common_name":        "SP_name",
            "desc_landmark":           "SP_landmark",
            "desc_street":             "SP_street",
            "desc_indicator":          "SP_indicator",
            "place_nptg_locality_ref": "LOC_nptg_code",
            "place_longitude":         "SP_longitude",
            "place_latitude":          "SP_latitude",
            "place_missing_data":      "SP_missing_data",
            "stop_stop_type":          "SP_stop_type",
            "stop_bus_stop_type":      "SP_bus_stop_type",
            "stop_timing_status":      "SP_timing_status",
            "stop_compass_point":      "SP_compass_point",
            "stop_degrees":            "SP_degrees",
        })[["SP_atco_code", "Aut_atco_area_code", "Aut_admin_area_code",
            "SA_code", "LOC_nptg_code", "SP_naptan_code", "SP_plus_bus_zone_ref",
            "SP_start_date", "SP_name", "SP_landmark", "SP_street", "SP_indicator",
            "SP_missing_data", "SP_longitude", "SP_latitude", "SP_stop_type",
            "SP_bus_stop_type", "SP_timing_status", "SP_compass_point", "SP_degrees"]]

        _to_sql(conn, stop_point_out, "Stop_Point")

    print()


# ═══════════════════════════════════════════════════════════════════════════════
#  Bus timetable loading
# ═══════════════════════════════════════════════════════════════════════════════

def _load_bus(conn: sqlite3.Connection) -> None:
    print("══════════════════════════════════════════════")
    print("  Loading Bus Timetable …")
    print("══════════════════════════════════════════════\n")

    operator_df   = _read("operator.csv",   BUS_DIR)
    garage_df     = _read("garage.csv",     BUS_DIR)
    so_df         = _read("serviced_organisation.csv", BUS_DIR)
    sodr_df       = _read("serviced_organisation_date_range.csv", BUS_DIR)
    service_df    = _read("service.csv",    BUS_DIR)
    line_df       = _read("line.csv",       BUS_DIR)
    route_df      = _read("route.csv",      BUS_DIR)
    route_sec_df  = _read("route_section.csv",  BUS_DIR)
    route_link_df = _read("route_link.csv",     BUS_DIR)
    route_loc_df  = _read("route_location.csv", BUS_DIR)
    bus_sp_df     = _read("bus_stop_point.csv",  BUS_DIR)
    jp_df         = _read("journey_pattern.csv",         BUS_DIR)
    jps_df        = _read("journey_pattern_section.csv", BUS_DIR)
    jpl_df        = _read("journey_pattern_link.csv",    BUS_DIR)
    vj_df         = _read("vehicle_journey.csv",       BUS_DIR)
    vjl_df        = _read("vehicle_journey_link.csv",  BUS_DIR)
    dow_df        = _read("days_of_week.csv",          BUS_DIR)
    sdo_df        = _read("special_days_operation.csv", BUS_DIR)
    bho_df        = _read("bank_holiday_operation.csv", BUS_DIR)

    # ── 1. Operator ──────────────────────────────────────────────────────
    if operator_df is not None:
        _to_sql(conn, operator_df.rename(columns={
            "uid": "OPE_UID",
            "id":  "OPE_id",
            "national_operator_code": "OPE_national_code",
            "operator_code":          "OPE_code",
            "operator_short_name":    "OPE_short_name",
            "licence_number":         "OPE_licence_number",
            "operator_name_on_licence": "OPE_licence_name",
            "trading_name":           "OPE_trading_name",
        })[["OPE_UID", "OPE_id", "OPE_national_code", "OPE_code",
            "OPE_short_name", "OPE_licence_number", "OPE_licence_name",
            "OPE_trading_name"]], "Operator")

    # ── 2. Garage ────────────────────────────────────────────────────────
    if garage_df is not None:
        _to_sql(conn, garage_df.rename(columns={
            "uid": "GAR_UID",
            "parent_operator_uid": "OPE_UID",
            "garage_code": "GAR_code",
            "garage_name": "GAR_name",
            "longitude":   "GAR_longitude",
            "latitude":    "GAR_latitude",
        })[["GAR_UID", "OPE_UID", "GAR_code", "GAR_name",
            "GAR_longitude", "GAR_latitude"]], "Garage")

    # ── 3. Serviced Organisation ─────────────────────────────────────────
    if so_df is not None:
        _to_sql(conn, so_df.rename(columns={
            "uid": "SO_UID",
            "organisation_code": "SO_code",
            "name": "SO_name",
        })[["SO_UID", "SO_code", "SO_name"]], "Serviced_Organisation")

    # ── 4. Serviced Organisation Date Range ──────────────────────────────
    if sodr_df is not None:
        _to_sql(conn, sodr_df.rename(columns={
            "uid": "SODR_UID",
            "organisation_uid": "SO_UID",
            "start_date":  "SODR_start_date",
            "end_date":    "SODR_end_date",
            "description": "SODR_description",
        })[["SODR_UID", "SO_UID", "SODR_start_date", "SODR_end_date",
            "SODR_description"]], "Serviced_Organisation_Date_Range")

    # ── 5. Service ───────────────────────────────────────────────────────
    if service_df is not None:
        _to_sql(conn, service_df.rename(columns={
            "uid": "SER_UID",
            "operator_uid":  "OPE_UID",
            "service_code":  "SER_service_code",
            "start_date":    "SER_start_date",
            "end_date":      "SER_end_date",
            "operator_ref":  "OPE_code",
            "origin":        "SER_origin",
            "destination":   "SER_destination",
        })[["SER_UID", "OPE_UID", "SER_service_code", "SER_start_date",
            "SER_end_date", "OPE_code", "SER_origin", "SER_destination"]],
            "Service")

    # ── 6. Line ──────────────────────────────────────────────────────────
    if line_df is not None:
        _to_sql(conn, line_df.rename(columns={
            "uid": "LIN_UID",
            "parent_service_uid":  "SER_UID",
            "line_id":             "LIN_id",
            "line_name":           "LIN_name",
            "out_bound_origin":    "LIN_out_bound_orig",
            "out_bound_destination": "LIN_out_bound_dest",
            "out_bound_description": "LIN_out_bound_desc",
            "in_bound_origin":     "LIN_in_bound_orig",
            "in_bound_destination": "LIN_in_bound_dest",
            "in_bound_description": "LIN_in_bound_desc",
            "parent_service_code": "SER_service_code",
        })[["LIN_UID", "SER_UID", "LIN_id", "LIN_name",
            "LIN_out_bound_orig", "LIN_out_bound_dest", "LIN_out_bound_desc",
            "LIN_in_bound_orig", "LIN_in_bound_dest", "LIN_in_bound_desc",
            "SER_service_code"]], "Line")

    # ── 7. Route ─────────────────────────────────────────────────────────
    if route_df is not None:
        _to_sql(conn, route_df.rename(columns={
            "uid": "ROU_UID",
            "route_id":     "ROU_id",
            "private_code": "ROU_private_code",
            "description":  "ROU_description",
        })[["ROU_UID", "ROU_id", "ROU_private_code", "ROU_description"]],
            "Route")

    # ── 8. Route_Section  (parent = Route via range index) ───────────────
    if route_sec_df is not None and route_df is not None:
        sec_to_route = _expand_range(
            route_df, "uid",
            "route_section_start_index", "route_section_end_index",
        )
        route_sec_df = route_sec_df.copy()
        route_sec_df["ROU_UID"] = (
            route_sec_df["uid"].astype(int).map(sec_to_route)
        )
        _to_sql(conn, route_sec_df.rename(columns={
            "uid": "RSEC_UID",
            "section_id": "RSEC_id",
        })[["RSEC_UID", "ROU_UID", "RSEC_id"]], "Route_Section")

    # ── 9. Bus_Stop_Point ────────────────────────────────────────────────
    if bus_sp_df is not None:
        _to_sql(conn, bus_sp_df.rename(columns={
            "uid": "Bus_SP_UID",
            "stop_point_ref": "SP_atco_code",
            "common_name":    "Bus_SP_name",
        })[["Bus_SP_UID", "SP_atco_code", "Bus_SP_name"]], "Bus_Stop_Point")

    # ── 10. Route_Link  (parent = Route_Section via range index) ─────────
    if route_link_df is not None and route_sec_df is not None:
        link_to_sec = _expand_range(
            route_sec_df, "uid",
            "start_route_link_index", "end_route_link_index",
        )
        route_link_df = route_link_df.copy()
        route_link_df["RSEC_UID"] = (
            route_link_df["uid"].astype(int).map(link_to_sec)
        )
        _to_sql(conn, route_link_df.rename(columns={
            "uid": "RLIN_UID",
            "link_id":              "RLIN_id",
            "from_stop_point_ref":  "from_SP_atco_code",
            "to_stop_point_ref":    "to_SP_atco_code",
            "from_bus_stop_point_uid": "from_Bus_SP_uid",
            "to_bus_stop_point_uid":   "to_Bus_SP_uid",
            "distance":             "RLIN_distance",
            "global_seq":           "RLIN_global_seq",
        })[["RLIN_UID", "RSEC_UID", "from_SP_atco_code", "to_SP_atco_code",
            "from_Bus_SP_uid", "to_Bus_SP_uid", "RLIN_id", "RLIN_distance",
            "RLIN_global_seq"]], "Route_Link")

    # ── 11. Route_Location  (parent = Route_Link via range index) ────────
    if route_loc_df is not None and route_link_df is not None:
        loc_to_link = _expand_range(
            route_link_df, "uid",
            "start_route_location_index", "end_route_location_index",
        )
        route_loc_df = route_loc_df.copy()
        route_loc_df["RLIN_UID"] = (
            route_loc_df["uid"].astype(int).map(loc_to_link)
        )
        _to_sql(conn, route_loc_df.rename(columns={
            "uid": "RLOC_UID",
            "location_id": "RLOC_id",
            "longitude":   "RLOC_longitude",
            "latitude":    "RLOC_latitude",
            "global_seq":  "RLOC_global_seq",
        })[["RLOC_UID", "RLIN_UID", "RLOC_id", "RLOC_longitude",
            "RLOC_latitude", "RLOC_global_seq"]], "Route_Location")

    # ── 12. Journey_Pattern ──────────────────────────────────────────────
    if jp_df is not None:
        _to_sql(conn, jp_df.rename(columns={
            "uid": "JP_UID",
            "operator_uid":        "OPE_UID",
            "parent_service_uid":  "SER_UID",
            "route_uid":           "ROU_UID",
            "journey_pattern_id":  "JP_id",
            "destination_display": "JP_dest_display",
            "direction":           "JP_direction",
            "description":         "JP_description",
        })[["JP_UID", "OPE_UID", "SER_UID", "ROU_UID",
            "JP_id", "JP_dest_display", "JP_direction", "JP_description"]],
            "Journey_Pattern")

    # ── 13. Journey_Pattern_Section  (parent = JP via start/end uid) ─────
    if jps_df is not None and jp_df is not None:
        sec_to_jp = _expand_range(
            jp_df, "uid",
            "JP_section_start_uid", "JP_section_end_uid",
        )
        jps_df = jps_df.copy()
        jps_df["JP_UID"] = jps_df["uid"].astype(int).map(sec_to_jp)
        _to_sql(conn, jps_df.rename(columns={
            "uid": "JPS_UID",
            "section_id": "JPS_id",
        })[["JPS_UID", "JP_UID", "JPS_id"]], "Journey_Pattern_Section")

    # ── 14. Journey_Pattern_Link  (parent = JPS via range index) ─────────
    if jpl_df is not None and jps_df is not None:
        link_to_jps = _expand_range(
            jps_df, "uid",
            "start_JP_link_index", "end_JP_link_index",
        )
        jpl_df = jpl_df.copy()
        jpl_df["JPS_UID"] = jpl_df["uid"].astype(int).map(link_to_jps)
        jpl_out = jpl_df.rename(columns={
            "uid": "JPL_UID",
            "route_link_uid":                "RLIN_UID",
            "from_point_point_id":           "JPL_from_point_atco_code",
            "to_point_point_id":             "JPL_to_point_atco_code",
            "from_point_sequence_num":       "from_sequence_num",
            "from_point_activity":           "from_activity",
            "from_point_destination_display": "from_dest_dispaly",
            "from_point_timing_status":      "from_timing_status",
            "from_point_fare_stage_num":     "from_fare_stage_num",
            "to_point_sequence_num":         "to_sequence_num",
            "to_point_activity":             "to_activity",
            "to_point_destination_display":  "to_dest_dispaly",
            "to_point_timing_status":        "to_timing_status",
            "to_point_fare_stage_num":       "to_fare_stage_num",
            "run_time":                      "JPL_run_time",
            "global_seq":                    "JPL_global_seq",
        })[["JPL_UID", "RLIN_UID", "JPS_UID",
            "JPL_from_point_atco_code", "JPL_to_point_atco_code",
            "from_sequence_num", "from_activity", "from_dest_dispaly",
            "from_timing_status", "from_fare_stage_num",
            "to_sequence_num", "to_activity", "to_dest_dispaly",
            "to_timing_status", "to_fare_stage_num",
            "JPL_run_time", "JPL_global_seq"]]
        _to_sql(conn, jpl_out, "Journey_Pattern_Link")

    # ── 15. Vehicle_Journey ──────────────────────────────────────────────
    #   CSV uid columns are already the real UIDs (not indices).
    #   Nullable: garage_uid (-1 → NULL), serviced_organisation_uid (-1 → NULL)
    if vj_df is not None:
        vj_df = vj_df.copy()
        _sentinel_to_null(vj_df, "garage_uid")
        _sentinel_to_null(vj_df, "serviced_organisation_uid")
        vj_out = vj_df.rename(columns={
            "uid":             "VJ_UID",
            "operator_uid":    "OPE_UID",
            "serviced_organisation_uid": "SO_UID",
            "garage_uid":      "GAR_UID",
            "service_uid":     "SER_UID",
            "line_uid":        "LIN_UID",
            "JP_uid":          "JP_UID",
            "private_code":    "VJ_private_code",
            "sequence_number": "VJ_sequence_number",
            "VJ_code":         "VJ_code",
            "departure_time":  "departure_time",
        })[["VJ_UID", "OPE_UID", "SO_UID", "GAR_UID", "SER_UID",
            "LIN_UID", "JP_UID",
            "VJ_private_code", "VJ_sequence_number", "VJ_code",
            "departure_time"]]
        _to_sql(conn, vj_out, "Vehicle_Journey")

    # ── 16. Days_Of_Week ─────────────────────────────────────────────────
    if dow_df is not None:
        dow_df = dow_df.copy()
        day_cols = ["monday", "tuesday", "wednesday", "thursday",
                    "friday", "saturday", "sunday"]
        def _days_bits(row):
            # bit 0 = Monday, bit 6 = Sunday  (matches query.py: 1 << day_of_week)
            return sum(
                1 << i
                for i, c in enumerate(day_cols)
                if str(row.get(c, "")).lower() == "true"
            )
        dow_df["DOW_days"] = dow_df.apply(_days_bits, axis=1).astype(str)

        _to_sql(conn, dow_df.rename(columns={
            "uid": "DOW_UID",
            "parent_VJ_uid": "VJ_UID",
            "monday":    "DOW_monday",
            "tuesday":   "DOW_tuesday",
            "wednesday": "DOW_wednesday",
            "thursday":  "DOW_thursday",
            "friday":    "DOW_friday",
            "saturday":  "DOW_saturday",
            "sunday":    "DOW_sunday",
        })[["DOW_UID", "VJ_UID", "DOW_monday", "DOW_tuesday", "DOW_wednesday",
            "DOW_thursday", "DOW_friday", "DOW_saturday", "DOW_sunday",
            "DOW_days"]], "Days_Of_Week")

    # ── 17. Special_Days_Operation ───────────────────────────────────────
    if sdo_df is not None:
        _to_sql(conn, sdo_df.rename(columns={
            "uid": "SDO_UID",
            "parent_VJ_uid": "VJ_UID",
            "do_operate":    "SDO_do_operate",
            "start_date":    "SDO_start_date",
            "end_date":      "SDO_end_date",
        })[["SDO_UID", "VJ_UID", "SDO_do_operate", "SDO_start_date",
            "SDO_end_date"]], "Special_Days_Operation")

    # ── 18. Bank_Holiday_Operation ───────────────────────────────────────
    if bho_df is not None:
        _to_sql(conn, bho_df.rename(columns={
            "uid": "BHO_UID",
            "parent_VJ_uid": "VJ_UID",
            "days_of_operation":     "BHO_days_of_operation",
            "days_of_non_operation": "BHO_days_of_non_operation",
        })[["BHO_UID", "VJ_UID", "BHO_days_of_operation",
            "BHO_days_of_non_operation"]], "Bank_Holiday_Operation")

    # ── 19. Vehicle_Journey_Link ─────────────────────────────────────────
    if vjl_df is not None:
        _to_sql(conn, vjl_df.rename(columns={
            "uid": "VJL_UID",
            "parent_VJ_uid": "VJ_UID",
            "JP_link_uid":   "JPL_UID",
            "link_id":       "VJL_id",
            "runtime":       "VJL_run_time",
            "from_activity": "VJL_from_activity",
            "to_activity":   "VJL_to_activity",
            "global_seq":    "VJL_global_seq",
        })[["VJL_UID", "VJ_UID", "JPL_UID", "VJL_id", "VJL_run_time",
            "VJL_from_activity", "VJL_to_activity", "VJL_global_seq"]],
            "Vehicle_Journey_Link")

    print()


# ═══════════════════════════════════════════════════════════════════════════════

def load_data():
    if not os.path.exists(DB_PATH):
        print("Error: Database not found. Run db.py first!")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = OFF;")   # trust pre-filtered data
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")

    _load_nptg(conn)
    _load_bus(conn)

    conn.commit()
    conn.close()
    print("✅ CSV loading complete.")


# if __name__ == "__main__":
#     load_data()
