"""
detector.py — FK integrity checks for CSV_NPTG and CSV_BUS data.

Reads the CSV files produced by the NPTG/NaPTAN and Bus preprocessors and
validates every foreign-key reference.  Reports violations but does not modify
any files.

Usage
-----
    python detector.py                  # check both NPTG and BUS
    python detector.py --nptg           # check NPTG only
    python detector.py --bus            # check BUS only
"""

import os
import sys
import pandas as pd


# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
CSV_NPTG_DIR = os.path.join(SCRIPT_DIR, "CSV_NPTG")
CSV_BUS_DIR  = os.path.join(SCRIPT_DIR, "CSV_BUS")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _read(name: str, csv_dir: str) -> pd.DataFrame:
    path = os.path.join(csv_dir, name)
    if not os.path.exists(path):
        print(f"  [MISSING] {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, keep_default_na=False, dtype=str)
    return df


def _report(
    table_name: str,
    fk_col: str,
    ref_table: str,
    ref_col: str,
    bad_mask: pd.Series,
    df: pd.DataFrame,
    max_examples: int = 20,
) -> int:
    """Print a human-readable report for one FK check.  Returns count of bad rows."""
    n_bad = int(bad_mask.sum())
    if n_bad == 0:
        print(f"  ✅  {table_name}.{fk_col}  →  {ref_table}.{ref_col}   ALL OK  ({len(df)} rows)")
        return 0

    pct = n_bad / len(df) * 100 if len(df) else 0
    print(f"  ❌  {table_name}.{fk_col}  →  {ref_table}.{ref_col}   "
          f"{n_bad} invalid / {len(df)} rows  ({pct:.2f}%)")

    examples = df.loc[bad_mask, fk_col].unique()[:max_examples]
    print(f"      sample invalid values: {list(examples)}")
    return n_bad


def _report_range(
    table_name: str,
    start_col: str,
    end_col: str,
    child_table: str,
    child_max_uid: int,
    df: pd.DataFrame,
    nullable: bool = False,
    max_examples: int = 10,
) -> int:
    """Check that every (start_index, end_index) pair falls within [0, child_max_uid].
    Nullable rows (empty or -1) are skipped.  Returns count of bad rows."""
    starts = df[start_col]
    ends   = df[end_col]
    label  = f"{table_name}.{start_col}..{end_col} → {child_table}"

    if nullable:
        # skip rows where either column is empty or "-1"
        keep = ((starts != "") & (ends != "") &
                (starts != "-1") & (ends != "-1"))
        starts = starts[keep]
        ends   = ends[keep]

    s = starts.astype(int)
    e = ends.astype(int)
    total = len(s)

    bad = (s < 0) | (e < 0) | (s > child_max_uid) | (e > child_max_uid) | (s > e)
    n_bad = int(bad.sum())

    if n_bad == 0:
        print(f"  ✅  {label}   ALL OK  ({total} rows)")
    else:
        pct = n_bad / total * 100 if total else 0
        print(f"  ❌  {label}   {n_bad} invalid / {total} rows  ({pct:.2f}%)")
        bad_idx = bad[bad].index[:max_examples]
        samples = [f"[{starts[i]},{ends[i]}]" for i in bad_idx]
        print(f"      sample ranges: {samples}")
    return n_bad


def _report_reverse_combined(
    parent_table: str,
    parent_pk: str,
    parent_uids: set,
    children: list,
    max_examples: int = 20,
) -> int:
    """Combined reverse FK check — are all parent UIDs referenced by >= 1 child?

    Parameters
    ----------
    children : list of (child_name, child_df, fk_col, nullable) tuples
    """
    referenced = set()
    child_labels = []
    for child_name, child_df, fk_col, nullable in children:
        if child_df.empty:
            continue
        fk_vals = set(child_df[fk_col])
        if nullable:
            fk_vals.discard("-1")
            fk_vals.discard("")
        referenced |= fk_vals
        child_labels.append(f"{child_name}.{fk_col}")

    children_str = ", ".join(child_labels) if child_labels else "(none)"
    label = f"{parent_table}.{parent_pk} ← {{{children_str}}}"

    orphans = parent_uids - referenced
    n_orphan = len(orphans)

    if n_orphan == 0:
        print(f"  ✅  {label}   ALL {len(parent_uids)} referenced")
    else:
        pct = n_orphan / len(parent_uids) * 100 if parent_uids else 0
        print(f"  ⚠️  {label}   "
              f"{n_orphan} / {len(parent_uids)} orphans ({pct:.1f}%)")
        samples = sorted(
            orphans,
            key=lambda x: int(x) if x.lstrip("-").isdigit() else 0,
        )[:max_examples]
        print(f"      sample orphan UIDs: {samples}")

    return n_orphan


def _report_range_coverage(
    parent_table: str,
    start_col: str,
    end_col: str,
    child_table: str,
    parent_df: "pd.DataFrame",
    child_max_uid: int,
    nullable: bool = False,
    max_examples: int = 10,
) -> int:
    """Reverse index-range check — every child uid in [0, child_max_uid]
    must be covered by at least one parent's [start, end] range.
    Returns count of orphan (uncovered) child UIDs."""
    label = f"{child_table}.uid ← {parent_table}.{start_col}..{end_col}"
    total_child = child_max_uid + 1 if child_max_uid >= 0 else 0

    starts = parent_df[start_col].copy()
    ends = parent_df[end_col].copy()

    if nullable:
        keep = (starts != "") & (ends != "") & (starts != "-1") & (ends != "-1")
        starts = starts[keep]
        ends = ends[keep]

    if starts.empty:
        if total_child > 0:
            print(f"  ❌  {label}   {total_child} orphan UIDs (no valid ranges)")
            return total_child
        print(f"  ✅  {label}   no children, no ranges")
        return 0

    s_arr = starts.astype(int).to_numpy()
    e_arr = ends.astype(int).to_numpy()
    order = s_arr.argsort()
    s_arr = s_arr[order]
    e_arr = e_arr[order]

    # Merge intervals
    merged_s = [int(s_arr[0])]
    merged_e = [int(e_arr[0])]
    for i in range(1, len(s_arr)):
        si, ei = int(s_arr[i]), int(e_arr[i])
        if si <= merged_e[-1] + 1:
            if ei > merged_e[-1]:
                merged_e[-1] = ei
        else:
            merged_s.append(si)
            merged_e.append(ei)

    covered = sum(me - ms + 1 for ms, me in zip(merged_s, merged_e))
    n_orphan = total_child - covered

    if n_orphan == 0:
        print(f"  ✅  {label}   ALL {total_child} child UIDs covered")
    else:
        gaps = []
        prev_end = -1
        for ms, me in zip(merged_s, merged_e):
            if ms > prev_end + 1:
                gaps.append((prev_end + 1, ms - 1))
            prev_end = me
        if prev_end < child_max_uid:
            gaps.append((prev_end + 1, child_max_uid))

        print(f"  ❌  {label}   {n_orphan} orphan UIDs in {len(gaps)} gap(s)")
        for gs, ge in gaps[:max_examples]:
            print(f"      gap: [{gs}, {ge}]  ({ge - gs + 1} UIDs)")

    return n_orphan


# ── Main validation ─────────────────────────────────────────────────────────

def check_nptg_fk(csv_dir: str = CSV_NPTG_DIR):
    """
    Validate all FK relationships in CSV_NPTG.

    Parameters
    ----------
    csv_dir : str
        Directory containing the six NPTG CSV files.
    """
    total_errors = 0

    # ── 1. Load all CSV files ────────────────────────────────────────────
    print("\n══════════════════════════════════════════════")
    print("  Loading CSV_NPTG files …")
    print("══════════════════════════════════════════════\n")

    region_df       = _read("region.csv",       csv_dir)
    authority_df    = _read("authority.csv",     csv_dir)
    district_df     = _read("district.csv",     csv_dir)
    locality_df     = _read("locality.csv",     csv_dir)
    stop_area_df    = _read("stop_area.csv",    csv_dir)
    stop_point_df   = _read("stop_point.csv",   csv_dir)

    print(f"  region      : {len(region_df)} rows")
    print(f"  authority   : {len(authority_df)} rows")
    print(f"  district    : {len(district_df)} rows")
    print(f"  locality    : {len(locality_df)} rows")
    print(f"  stop_area   : {len(stop_area_df)} rows")
    print(f"  stop_point  : {len(stop_point_df)} rows")

    # ── 2. Build PK sets ─────────────────────────────────────────────────
    region_codes    = set(region_df["region_code"])         if not region_df.empty    else set()
    auth_codes      = set(authority_df["admin_area_code"])  if not authority_df.empty  else set()
    district_codes  = set(district_df["nptg_district_code"])if not district_df.empty  else set()
    locality_codes  = set(locality_df["nptg_locality_code"])if not locality_df.empty  else set()
    stop_area_codes = set(stop_area_df["stop_area_code"])   if not stop_area_df.empty else set()

    # Also build index-based maps (parent_index is a positional index)
    region_index_set    = set(str(i) for i in range(len(region_df)))
    authority_index_set = set(str(i) for i in range(len(authority_df)))

    # ── 3. Authority  →  Region (via parent_index) ──────────────────────
    print("\n── Authority FK checks ────────────────────────")
    if not authority_df.empty and not region_df.empty:
        bad = ~authority_df["parent_index"].isin(region_index_set)
        total_errors += _report("Authority", "parent_index", "Region", "(row index)",
                                bad, authority_df)

    # ── 4. District  →  Authority (via parent_index) ────────────────────
    print("\n── District FK checks ─────────────────────────")
    if not district_df.empty and not authority_df.empty:
        bad = ~district_df["parent_index"].isin(authority_index_set)
        total_errors += _report("District", "parent_index", "Authority", "(row index)",
                                bad, district_df)

    # ── 5. Locality  →  Authority, District, self (parent_locality) ─────
    print("\n── Locality FK checks ─────────────────────────")
    if not locality_df.empty:
        # 5a. authority_ref → Authority.admin_area_code
        bad_auth = ~locality_df["authority_ref"].isin(auth_codes)
        total_errors += _report("Locality", "authority_ref", "Authority", "admin_area_code",
                                bad_auth, locality_df)

        # 5b. nptg_district_ref → District.nptg_district_code
        bad_dist = ~locality_df["nptg_district_ref"].isin(district_codes)
        total_errors += _report("Locality", "nptg_district_ref", "District", "nptg_district_code",
                                bad_dist, locality_df)

        # 5c. parent_nptg_locality_ref → Locality.nptg_locality_code  (nullable)
        has_parent = locality_df["parent_nptg_locality_ref"] != ""
        if has_parent.any():
            bad_parent = has_parent & ~locality_df["parent_nptg_locality_ref"].isin(locality_codes)
            total_errors += _report("Locality", "parent_nptg_locality_ref", "Locality",
                                    "nptg_locality_code", bad_parent, locality_df)
        else:
            print(f"  ✅  Locality.parent_nptg_locality_ref  →  Locality.nptg_locality_code   "
                  f"ALL NULL / empty  (nothing to check)")

    # ── 6. Stop_Area  →  Authority ──────────────────────────────────────
    print("\n── Stop_Area FK checks ────────────────────────")
    if not stop_area_df.empty:
        bad = ~stop_area_df["nptg_authority_ref"].isin(auth_codes)
        total_errors += _report("Stop_Area", "nptg_authority_ref", "Authority",
                                "admin_area_code", bad, stop_area_df)

    # ── 7. Stop_Point  →  Authority, Stop_Area, Locality ───────────────
    print("\n── Stop_Point FK checks ───────────────────────")
    if not stop_point_df.empty:
        # 7a. nptg_authority_ref → Authority.admin_area_code
        bad_auth = ~stop_point_df["nptg_authority_ref"].isin(auth_codes)
        total_errors += _report("Stop_Point", "nptg_authority_ref", "Authority",
                                "admin_area_code", bad_auth, stop_point_df)

        # 7b. stop_areas_ref → Stop_Area.stop_area_code  (nullable)
        has_sa = stop_point_df["stop_areas_ref"] != ""
        if has_sa.any():
            bad_sa = has_sa & ~stop_point_df["stop_areas_ref"].isin(stop_area_codes)
            total_errors += _report("Stop_Point", "stop_areas_ref", "Stop_Area",
                                    "stop_area_code", bad_sa, stop_point_df)
        else:
            print(f"  ✅  Stop_Point.stop_areas_ref  →  Stop_Area.stop_area_code   "
                  f"ALL empty  (nothing to check)")

        # 7c. place_nptg_locality_ref → Locality.nptg_locality_code
        bad_loc = ~stop_point_df["place_nptg_locality_ref"].isin(locality_codes)
        total_errors += _report("Stop_Point", "place_nptg_locality_ref", "Locality",
                                "nptg_locality_code", bad_loc, stop_point_df)

    # ── 8. Summary ───────────────────────────────────────────────────────
    print("\n══════════════════════════════════════════════")
    if total_errors == 0:
        print("  🎉  All FK references are valid!")
    else:
        print(f"  ⚠️   Total FK violations: {total_errors}")
    print("══════════════════════════════════════════════\n")

    return total_errors


# ── Bus FK validation ────────────────────────────────────────────────────────

def check_bus_fk(csv_dir: str = CSV_BUS_DIR, nptg_dir: str = CSV_NPTG_DIR):
    """
    Validate all FK relationships in CSV_BUS.
    Forward (33) + cross-domain (1) + reverse-UID (15) + reverse-range (6).
    """
    total_errors = 0

    # ── 1. Load all CSV files ────────────────────────────────────────────
    print("\n══════════════════════════════════════════════")
    print("  Loading CSV_BUS files …")
    print("══════════════════════════════════════════════\n")

    operator_df   = _read("operator.csv",   csv_dir)
    garage_df     = _read("garage.csv",     csv_dir)
    service_df    = _read("service.csv",    csv_dir)
    line_df       = _read("line.csv",       csv_dir)
    route_df      = _read("route.csv",      csv_dir)
    route_sec_df  = _read("route_section.csv",  csv_dir)
    route_link_df = _read("route_link.csv",     csv_dir)
    route_loc_df  = _read("route_location.csv", csv_dir)
    bus_sp_df     = _read("bus_stop_point.csv",  csv_dir)
    jp_df         = _read("journey_pattern.csv",         csv_dir)
    jps_df        = _read("journey_pattern_section.csv", csv_dir)
    jpl_df        = _read("journey_pattern_link.csv",    csv_dir)
    vj_df         = _read("vehicle_journey.csv",       csv_dir)
    vjl_df        = _read("vehicle_journey_link.csv",  csv_dir)
    dow_df        = _read("days_of_week.csv",          csv_dir)
    sdo_df        = _read("special_days_operation.csv", csv_dir)
    bho_df        = _read("bank_holiday_operation.csv", csv_dir)
    so_df         = _read("serviced_organisation.csv",  csv_dir)
    sodr_df       = _read("serviced_organisation_date_range.csv", csv_dir)

    tables = [
        ("operator",              operator_df),
        ("garage",                garage_df),
        ("service",               service_df),
        ("line",                  line_df),
        ("route",                 route_df),
        ("route_section",         route_sec_df),
        ("route_link",            route_link_df),
        ("route_location",        route_loc_df),
        ("bus_stop_point",        bus_sp_df),
        ("journey_pattern",       jp_df),
        ("journey_pattern_section", jps_df),
        ("journey_pattern_link",  jpl_df),
        ("vehicle_journey",       vj_df),
        ("vehicle_journey_link",  vjl_df),
        ("days_of_week",          dow_df),
        ("special_days_operation", sdo_df),
        ("bank_holiday_operation", bho_df),
        ("serviced_organisation", so_df),
        ("serviced_org_date_range", sodr_df),
    ]
    for name, df in tables:
        print(f"  {name:30s}: {len(df):>10,} rows")

    # ── 2. Build PK uid sets ─────────────────────────────────────────────
    def _uids(df: pd.DataFrame) -> set:
        return set(df["uid"]) if not df.empty and "uid" in df.columns else set()

    operator_uids = _uids(operator_df)
    garage_uids   = _uids(garage_df)
    service_uids  = _uids(service_df)
    line_uids     = _uids(line_df)
    route_uids    = _uids(route_df)
    route_sec_uids = _uids(route_sec_df)
    route_link_uids = _uids(route_link_df)
    route_loc_uids  = _uids(route_loc_df)
    bus_sp_uids   = _uids(bus_sp_df)
    jp_uids       = _uids(jp_df)
    jps_uids      = _uids(jps_df)
    jpl_uids      = _uids(jpl_df)
    vj_uids       = _uids(vj_df)
    vjl_uids      = _uids(vjl_df)
    dow_uids      = _uids(dow_df)
    sdo_uids      = _uids(sdo_df)
    bho_uids      = _uids(bho_df)
    so_uids       = _uids(so_df)

    bus_sp_refs = set(bus_sp_df["stop_point_ref"]) if not bus_sp_df.empty else set()

    # max uid for index-range checks
    def _max_uid(df: pd.DataFrame) -> int:
        if df.empty or "uid" not in df.columns:
            return -1
        return df["uid"].astype(int).max()

    route_sec_max = _max_uid(route_sec_df)
    route_link_max = _max_uid(route_link_df)
    route_loc_max  = _max_uid(route_loc_df)
    jpl_max        = _max_uid(jpl_df)
    sdo_max        = _max_uid(sdo_df)
    vjl_max        = _max_uid(vjl_df)

    # ════════════════════════════════════════════════════════════════════
    #  Section 1: UID → UID  (25 checks)
    # ════════════════════════════════════════════════════════════════════

    # ── #1  garage.parent_operator_uid → operator.uid ────────────────────
    print("\n── Garage FK checks ───────────────────────────")
    if not garage_df.empty:
        bad = ~garage_df["parent_operator_uid"].isin(operator_uids)
        total_errors += _report("Garage", "parent_operator_uid", "Operator", "uid",
                                bad, garage_df)

    # ── #2  service.operator_uid → operator.uid ──────────────────────────
    print("\n── Service FK checks ──────────────────────────")
    if not service_df.empty:
        bad = ~service_df["operator_uid"].isin(operator_uids)
        total_errors += _report("Service", "operator_uid", "Operator", "uid",
                                bad, service_df)

    # ── #3  line.parent_service_uid → service.uid ────────────────────────
    print("\n── Line FK checks ─────────────────────────────")
    if not line_df.empty:
        bad = ~line_df["parent_service_uid"].isin(service_uids)
        total_errors += _report("Line", "parent_service_uid", "Service", "uid",
                                bad, line_df)

    # ── #4-8  journey_pattern FKs ────────────────────────────────────────
    print("\n── Journey_Pattern FK checks ──────────────────")
    if not jp_df.empty:
        # #4 operator_uid → operator
        bad = ~jp_df["operator_uid"].isin(operator_uids)
        total_errors += _report("JP", "operator_uid", "Operator", "uid", bad, jp_df)

        # #5 route_uid → route
        bad = ~jp_df["route_uid"].isin(route_uids)
        total_errors += _report("JP", "route_uid", "Route", "uid", bad, jp_df)

        # #6 JP_section_start_uid → journey_pattern_section
        bad = ~jp_df["JP_section_start_uid"].isin(jps_uids)
        total_errors += _report("JP", "JP_section_start_uid", "JPS", "uid", bad, jp_df)

        # #7 JP_section_end_uid → journey_pattern_section
        bad = ~jp_df["JP_section_end_uid"].isin(jps_uids)
        total_errors += _report("JP", "JP_section_end_uid", "JPS", "uid", bad, jp_df)

        # #8 parent_service_uid → service
        bad = ~jp_df["parent_service_uid"].isin(service_uids)
        total_errors += _report("JP", "parent_service_uid", "Service", "uid", bad, jp_df)

    # ── #9  journey_pattern_link.route_link_uid → route_link.uid ─────────
    print("\n── Journey_Pattern_Link FK checks ─────────────")
    if not jpl_df.empty:
        bad = ~jpl_df["route_link_uid"].isin(route_link_uids)
        total_errors += _report("JPL", "route_link_uid", "Route_Link", "uid", bad, jpl_df)

    # ── #10-17  vehicle_journey FKs ──────────────────────────────────────
    print("\n── Vehicle_Journey FK checks ──────────────────")
    if not vj_df.empty:
        # #10 operator_uid → operator
        bad = ~vj_df["operator_uid"].isin(operator_uids)
        total_errors += _report("VJ", "operator_uid", "Operator", "uid", bad, vj_df)

        # #11 days_of_week_uid → days_of_week (nullable, -1 = no ref)
        has_dow = (vj_df["days_of_week_uid"] != "-1") & (vj_df["days_of_week_uid"] != "")
        if has_dow.any():
            bad = has_dow & ~vj_df["days_of_week_uid"].isin(dow_uids)
            total_errors += _report("VJ", "days_of_week_uid", "Days_Of_Week", "uid", bad, vj_df)
        else:
            print("  ✅  VJ.days_of_week_uid  →  Days_Of_Week.uid   ALL -1/empty  (nothing to check)")

        # #12 bank_holiday_operation_uid → bank_holiday_operation (nullable, empty = no ref)
        has_bho = vj_df["bank_holiday_operation_uid"] != ""
        if has_bho.any():
            bad = has_bho & ~vj_df["bank_holiday_operation_uid"].isin(bho_uids)
            total_errors += _report("VJ", "bank_holiday_operation_uid", "BHO", "uid", bad, vj_df)
        else:
            print("  ✅  VJ.bank_holiday_operation_uid  →  BHO.uid   ALL empty  (nothing to check)")

        # #13 serviced_organisation_uid → serviced_organisation (nullable, -1 = no ref)
        has_so = (vj_df["serviced_organisation_uid"] != "-1") & (vj_df["serviced_organisation_uid"] != "")
        if has_so.any():
            bad = has_so & ~vj_df["serviced_organisation_uid"].isin(so_uids)
            total_errors += _report("VJ", "serviced_organisation_uid", "Serviced_Org", "uid", bad, vj_df)
        else:
            print("  ✅  VJ.serviced_organisation_uid  →  Serviced_Org.uid   ALL -1/empty  (nothing to check)")

        # #14 garage_uid → garage (nullable, -1 = no ref)
        has_gar = (vj_df["garage_uid"] != "-1") & (vj_df["garage_uid"] != "")
        if has_gar.any():
            bad = has_gar & ~vj_df["garage_uid"].isin(garage_uids)
            total_errors += _report("VJ", "garage_uid", "Garage", "uid", bad, vj_df)
        else:
            print("  ✅  VJ.garage_uid  →  Garage.uid   ALL -1/empty  (nothing to check)")

        # #15 service_uid → service
        bad = ~vj_df["service_uid"].isin(service_uids)
        total_errors += _report("VJ", "service_uid", "Service", "uid", bad, vj_df)

        # #16 line_uid → line
        bad = ~vj_df["line_uid"].isin(line_uids)
        total_errors += _report("VJ", "line_uid", "Line", "uid", bad, vj_df)

        # #17 JP_uid → journey_pattern
        bad = ~vj_df["JP_uid"].isin(jp_uids)
        total_errors += _report("VJ", "JP_uid", "Journey_Pattern", "uid", bad, vj_df)

    # ── #18-19  vehicle_journey_link FKs ─────────────────────────────────
    print("\n── Vehicle_Journey_Link FK checks ─────────────")
    if not vjl_df.empty:
        # #18 parent_VJ_uid → vehicle_journey
        bad = ~vjl_df["parent_VJ_uid"].isin(vj_uids)
        total_errors += _report("VJL", "parent_VJ_uid", "Vehicle_Journey", "uid", bad, vjl_df)

        # #19 JP_link_uid → journey_pattern_link
        bad = ~vjl_df["JP_link_uid"].isin(jpl_uids)
        total_errors += _report("VJL", "JP_link_uid", "JP_Link", "uid", bad, vjl_df)

    # ── #20  days_of_week.parent_VJ_uid → vehicle_journey.uid ────────────
    print("\n── Days_Of_Week FK checks ─────────────────────")
    if not dow_df.empty:
        bad = ~dow_df["parent_VJ_uid"].isin(vj_uids)
        total_errors += _report("DOW", "parent_VJ_uid", "Vehicle_Journey", "uid", bad, dow_df)

    # ── #21  special_days_operation.parent_VJ_uid → vehicle_journey.uid ──
    print("\n── Special_Days_Operation FK checks ───────────")
    if not sdo_df.empty:
        bad = ~sdo_df["parent_VJ_uid"].isin(vj_uids)
        total_errors += _report("SDO", "parent_VJ_uid", "Vehicle_Journey", "uid", bad, sdo_df)

    # ── #22  bank_holiday_operation.parent_VJ_uid → vehicle_journey.uid ──
    print("\n── Bank_Holiday_Operation FK checks ───────────")
    if not bho_df.empty:
        bad = ~bho_df["parent_VJ_uid"].isin(vj_uids)
        total_errors += _report("BHO", "parent_VJ_uid", "Vehicle_Journey", "uid", bad, bho_df)

    # ── #23  serviced_org_date_range.organisation_uid → serviced_org.uid ─
    print("\n── Serviced_Org_Date_Range FK checks ──────────")
    if not sodr_df.empty:
        bad = ~sodr_df["organisation_uid"].isin(so_uids)
        total_errors += _report("SODR", "organisation_uid", "Serviced_Org", "uid", bad, sodr_df)

    # ── #24-25  route_link.from/to_bus_stop_point_uid → bus_stop_point ───
    print("\n── Route_Link FK checks ───────────────────────")
    if not route_link_df.empty:
        bad = ~route_link_df["from_bus_stop_point_uid"].isin(bus_sp_uids)
        total_errors += _report("Route_Link", "from_bus_stop_point_uid", "Bus_Stop_Point", "uid",
                                bad, route_link_df)
        bad = ~route_link_df["to_bus_stop_point_uid"].isin(bus_sp_uids)
        total_errors += _report("Route_Link", "to_bus_stop_point_uid", "Bus_Stop_Point", "uid",
                                bad, route_link_df)

    # ════════════════════════════════════════════════════════════════════
    #  Section 2: Index-Range FKs  (6 checks)
    # ════════════════════════════════════════════════════════════════════

    print("\n── Index-Range FK checks ──────────────────────")

    # #26  route → route_section
    if not route_df.empty:
        total_errors += _report_range("Route", "route_section_start_index",
                                      "route_section_end_index", "Route_Section",
                                      route_sec_max, route_df)

    # #27  route_section → route_link
    if not route_sec_df.empty:
        total_errors += _report_range("Route_Section", "start_route_link_index",
                                      "end_route_link_index", "Route_Link",
                                      route_link_max, route_sec_df)

    # #28  route_link → route_location (nullable, -1 = no ref)
    if not route_link_df.empty:
        total_errors += _report_range("Route_Link", "start_route_location_index",
                                      "end_route_location_index", "Route_Location",
                                      route_loc_max, route_link_df, nullable=True)

    # #29  journey_pattern_section → journey_pattern_link
    if not jps_df.empty:
        total_errors += _report_range("JPS", "start_JP_link_index",
                                      "end_JP_link_index", "JP_Link",
                                      jpl_max, jps_df)

    # #30  vehicle_journey → special_days_operation (nullable, empty = none)
    if not vj_df.empty:
        total_errors += _report_range("VJ", "special_days_operation_start_index",
                                      "special_days_operation_end_index", "SDO",
                                      sdo_max, vj_df, nullable=True)

    # #31  vehicle_journey → vehicle_journey_link (nullable, -1 = no ref)
    if not vj_df.empty:
        total_errors += _report_range("VJ", "VJ_link_start_index",
                                      "VJ_link_end_index", "VJ_Link",
                                      vjl_max, vj_df, nullable=True)

    # ════════════════════════════════════════════════════════════════════
    #  Section 3: Stop-Point Ref FK  (2 checks)
    # ════════════════════════════════════════════════════════════════════

    print("\n── Stop-Point Ref FK checks ───────────────────")
    if not jpl_df.empty:
        # #32  from_point_point_id → bus_stop_point.stop_point_ref
        bad = ~jpl_df["from_point_point_id"].isin(bus_sp_refs)
        total_errors += _report("JPL", "from_point_point_id", "Bus_Stop_Point", "stop_point_ref",
                                bad, jpl_df)

        # #33  to_point_point_id → bus_stop_point.stop_point_ref
        bad = ~jpl_df["to_point_point_id"].isin(bus_sp_refs)
        total_errors += _report("JPL", "to_point_point_id", "Bus_Stop_Point", "stop_point_ref",
                                bad, jpl_df)

    fwd_errors = total_errors   # snapshot after forward sections 1–3

    # ════════════════════════════════════════════════════════════════════
    #  Section 4: Cross-domain FK  (1 check)
    # ════════════════════════════════════════════════════════════════════

    cross_errors = 0
    print("\n── Cross-domain FK checks ─────────────────────")
    nptg_sp_df = _read("stop_point.csv", nptg_dir) if nptg_dir else pd.DataFrame()
    if not nptg_sp_df.empty and not bus_sp_df.empty:
        nptg_atco = set(nptg_sp_df["atco_code"])
        bad = ~bus_sp_df["stop_point_ref"].isin(nptg_atco)
        cross_errors += _report("Bus_Stop_Point", "stop_point_ref",
                                "NaPTAN_Stop_Point", "atco_code", bad, bus_sp_df)
    elif not nptg_dir:
        print("  ⚠️  No NPTG directory supplied — skipped")
    else:
        print("  ⚠️  NaPTAN stop_point.csv or bus_stop_point.csv empty — skipped")

    # ════════════════════════════════════════════════════════════════════
    #  Section 5: Reverse UID→UID  (orphan detection, 15 combined checks)
    # ════════════════════════════════════════════════════════════════════

    rev_orphans = 0
    print("\n── Reverse UID→UID (orphan detection) ─────────")

    # R1  operator ← garage, service, JP, VJ
    rev_orphans += _report_reverse_combined(
        "Operator", "uid", operator_uids, [
            ("Garage", garage_df, "parent_operator_uid", False),
            ("Service", service_df, "operator_uid", False),
            ("JP", jp_df, "operator_uid", False),
            ("VJ", vj_df, "operator_uid", False),
        ])

    # R2  garage ← VJ (nullable)
    rev_orphans += _report_reverse_combined(
        "Garage", "uid", garage_uids, [
            ("VJ", vj_df, "garage_uid", True),
        ])

    # R3  service ← line, JP, VJ
    rev_orphans += _report_reverse_combined(
        "Service", "uid", service_uids, [
            ("Line", line_df, "parent_service_uid", False),
            ("JP", jp_df, "parent_service_uid", False),
            ("VJ", vj_df, "service_uid", False),
        ])

    # R4  line ← VJ
    rev_orphans += _report_reverse_combined(
        "Line", "uid", line_uids, [
            ("VJ", vj_df, "line_uid", False),
        ])

    # R5  route ← JP
    rev_orphans += _report_reverse_combined(
        "Route", "uid", route_uids, [
            ("JP", jp_df, "route_uid", False),
        ])

    # R6  route_link ← JPL
    rev_orphans += _report_reverse_combined(
        "Route_Link", "uid", route_link_uids, [
            ("JPL", jpl_df, "route_link_uid", False),
        ])

    # R7  bus_stop_point.uid ← route_link (from + to)
    rev_orphans += _report_reverse_combined(
        "Bus_Stop_Point", "uid", bus_sp_uids, [
            ("Route_Link", route_link_df, "from_bus_stop_point_uid", False),
            ("Route_Link", route_link_df, "to_bus_stop_point_uid", False),
        ])

    # R8  bus_stop_point.stop_point_ref ← JPL (from + to)
    rev_orphans += _report_reverse_combined(
        "Bus_Stop_Point", "stop_point_ref", bus_sp_refs, [
            ("JPL", jpl_df, "from_point_point_id", False),
            ("JPL", jpl_df, "to_point_point_id", False),
        ])

    # R9  JP ← VJ
    rev_orphans += _report_reverse_combined(
        "Journey_Pattern", "uid", jp_uids, [
            ("VJ", vj_df, "JP_uid", False),
        ])

    # R10  JPS ← JP (via start/end uid)
    rev_orphans += _report_reverse_combined(
        "JP_Section", "uid", jps_uids, [
            ("JP", jp_df, "JP_section_start_uid", False),
            ("JP", jp_df, "JP_section_end_uid", False),
        ])

    # R11  JPL ← VJL
    rev_orphans += _report_reverse_combined(
        "JP_Link", "uid", jpl_uids, [
            ("VJL", vjl_df, "JP_link_uid", False),
        ])

    # R12  VJ ← VJL, DOW, SDO, BHO
    rev_orphans += _report_reverse_combined(
        "Vehicle_Journey", "uid", vj_uids, [
            ("VJL", vjl_df, "parent_VJ_uid", False),
            ("DOW", dow_df, "parent_VJ_uid", False),
            ("SDO", sdo_df, "parent_VJ_uid", False),
            ("BHO", bho_df, "parent_VJ_uid", False),
        ])

    # R13  DOW ← VJ (nullable)
    rev_orphans += _report_reverse_combined(
        "Days_Of_Week", "uid", dow_uids, [
            ("VJ", vj_df, "days_of_week_uid", True),
        ])

    # R14  BHO ← VJ (nullable)
    rev_orphans += _report_reverse_combined(
        "Bank_Holiday_Op", "uid", bho_uids, [
            ("VJ", vj_df, "bank_holiday_operation_uid", True),
        ])

    # R15  Serviced_Org ← VJ (nullable) + SODR
    rev_orphans += _report_reverse_combined(
        "Serviced_Org", "uid", so_uids, [
            ("VJ", vj_df, "serviced_organisation_uid", True),
            ("SODR", sodr_df, "organisation_uid", False),
        ])

    # ════════════════════════════════════════════════════════════════════
    #  Section 6: Reverse index-range coverage  (6 checks)
    # ════════════════════════════════════════════════════════════════════

    range_orphans = 0
    print("\n── Reverse index-range coverage ───────────────")

    # RC1  route_section ← route
    if not route_df.empty:
        range_orphans += _report_range_coverage(
            "Route", "route_section_start_index", "route_section_end_index",
            "Route_Section", route_df, route_sec_max)

    # RC2  route_link ← route_section
    if not route_sec_df.empty:
        range_orphans += _report_range_coverage(
            "Route_Section", "start_route_link_index", "end_route_link_index",
            "Route_Link", route_sec_df, route_link_max)

    # RC3  route_location ← route_link (nullable)
    if not route_link_df.empty:
        range_orphans += _report_range_coverage(
            "Route_Link", "start_route_location_index", "end_route_location_index",
            "Route_Location", route_link_df, route_loc_max, nullable=True)

    # RC4  JP_Link ← JPS
    if not jps_df.empty:
        range_orphans += _report_range_coverage(
            "JPS", "start_JP_link_index", "end_JP_link_index",
            "JP_Link", jps_df, jpl_max)

    # RC5  SDO ← VJ (nullable)
    if not vj_df.empty:
        range_orphans += _report_range_coverage(
            "VJ", "special_days_operation_start_index",
            "special_days_operation_end_index",
            "SDO", vj_df, sdo_max, nullable=True)

    # RC6  VJL ← VJ (nullable)
    if not vj_df.empty:
        range_orphans += _report_range_coverage(
            "VJ", "VJ_link_start_index", "VJ_link_end_index",
            "VJ_Link", vj_df, vjl_max, nullable=True)

    # ── Summary ──────────────────────────────────────────────────────────
    grand_total = fwd_errors + cross_errors + rev_orphans + range_orphans
    print("\n══════════════════════════════════════════════")
    print(f"  Forward FK violations   : {fwd_errors}")
    print(f"  Cross-domain violations : {cross_errors}")
    print(f"  Reverse UID orphans     : {rev_orphans}")
    print(f"  Reverse range orphans   : {range_orphans}")
    print(f"  ─────────────────────────")
    print(f"  Grand total             : {grand_total}")
    if grand_total == 0:
        print("  🎉  All CSV_BUS FK checks passed!")
    print("══════════════════════════════════════════════\n")

    return grand_total


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    total = 0
    total += check_nptg_fk()
    total += check_bus_fk()
    return total  # return violation count; caller decides what to do


if __name__ == "__main__":
    import sys
    sys.exit(1 if main() > 0 else 0)

# if __name__ == "__main__":
#     main()
