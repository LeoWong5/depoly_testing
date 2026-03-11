"""
filter.py — Remove rows with invalid FK references from CSV_NPTG data.

Reads the six CSV files, drops rows that have broken foreign keys,
remaps all positional-index columns so they remain consistent,
and writes the cleaned CSVs back.

Usage
-----
    python3 filter.py                           # filter in-place (overwrites CSV_NPTG/)
    python3 filter.py --out cleaned_CSV_NPTG    # write to a different dir
"""

import ast
import os
import sys

import numpy as np
import pandas as pd
from typing import Optional


# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
CSV_DIR     = os.path.join(SCRIPT_DIR, "CSV_NPTG")
CSV_BUS_DIR = os.path.join(SCRIPT_DIR, "CSV_BUS")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _read(name: str, csv_dir: str) -> pd.DataFrame:
    path = os.path.join(csv_dir, name)
    if not os.path.exists(path):
        print(f"  [MISSING] {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, keep_default_na=False, dtype=str)
    return df


def _write(df: pd.DataFrame, name: str, out_dir: str) -> None:
    path = os.path.join(out_dir, name)
    df.to_csv(path, index=False)
    print(f"  wrote {len(df):>7} rows → {path}")


def _build_remap(old_indices: pd.Index) -> dict[int, int]:
    """Build a mapping from old positional index → new positional index.

    *old_indices* is the pandas Index of the filtered DataFrame (which still
    carries the original row numbers before reset_index).
    """
    return {old: new for new, old in enumerate(old_indices)}


def _remap_index_col(df: pd.DataFrame, col: str, remap: dict[int, int]) -> pd.DataFrame:
    """Re-map a single integer index column.  Rows whose value can't be
    remapped are marked invalid (returned separately)."""
    df = df.copy()
    df[col] = df[col].apply(lambda v: str(remap.get(int(v), -1)) if v != "" else "")
    bad = df[col] == "-1"
    return df, bad


def _remap_list_col(df: pd.DataFrame, col: str, remap: dict[int, int]) -> pd.DataFrame:
    """Re-map a list-of-int column like '[0, 1, 2]'.
    Entries not found in remap are silently dropped from the list."""
    df = df.copy()

    def _remap_list(val_str: str) -> str:
        if val_str == "" or val_str == "[]":
            return "[]"
        try:
            old_list = ast.literal_eval(val_str)
        except (ValueError, SyntaxError):
            return "[]"
        new_list = [remap[old] for old in old_list if old in remap]
        return str(new_list)

    df[col] = df[col].apply(_remap_list)
    return df


def _report_drop(table: str, before: int, after: int) -> None:
    dropped = before - after
    if dropped:
        print(f"  🔸 {table}: dropped {dropped} rows ({before} → {after})")
    else:
        print(f"  ✅ {table}: no rows dropped ({after} rows)")


def _make_remap_array(total: int, drop_uids: set) -> np.ndarray:
    """Create remap array: arr[old_uid] = new_uid, -1 if dropped."""
    mask = np.zeros(total, dtype=bool)
    for uid in drop_uids:
        if 0 <= uid < total:
            mask[uid] = True
    new_uids = np.arange(total, dtype=np.int64) - np.cumsum(mask).astype(np.int64)
    new_uids[mask] = -1
    return new_uids


def _remap_range_columns(
    df: pd.DataFrame,
    start_col: str,
    end_col: str,
    remap: np.ndarray,
    nullable: bool = False,
) -> pd.DataFrame:
    """Remap [start, end] index-range FK columns using a child UID remap array."""
    df = df.copy()
    starts_str = df[start_col].values
    ends_str = df[end_col].values
    new_s, new_e = [], []
    for sv, ev in zip(starts_str, ends_str):
        if nullable and (sv in ("", "-1") or ev in ("", "-1")):
            new_s.append(sv)
            new_e.append(ev)
            continue
        s, e = int(sv), int(ev)
        chunk = remap[s : e + 1]
        valid = chunk[chunk >= 0]
        if len(valid) > 0:
            new_s.append(str(valid[0]))
            new_e.append(str(valid[-1]))
        else:
            new_s.append("-1")
            new_e.append("-1")
    df[start_col] = new_s
    df[end_col] = new_e
    return df


# ── Main filter logic ───────────────────────────────────────────────────────

def filter_nptg(csv_dir: str = CSV_DIR, out_dir: Optional[str] = None) -> int:
    """
    Remove rows with invalid FKs from CSV_NPTG and remap all positional indices.

    Returns the total number of rows dropped.
    """
    if out_dir is None:
        out_dir = csv_dir
    os.makedirs(out_dir, exist_ok=True)

    total_dropped = 0

    # ── 1. Load ──────────────────────────────────────────────────────────
    print("\n══════════════════════════════════════════════")
    print("  Loading CSV_NPTG files …")
    print("══════════════════════════════════════════════\n")

    region_df     = _read("region.csv",     csv_dir)
    authority_df  = _read("authority.csv",  csv_dir)
    district_df   = _read("district.csv",  csv_dir)
    locality_df   = _read("locality.csv",  csv_dir)
    stop_area_df  = _read("stop_area.csv", csv_dir)
    stop_point_df = _read("stop_point.csv", csv_dir)

    print(f"  region      : {len(region_df):>7}")
    print(f"  authority   : {len(authority_df):>7}")
    print(f"  district    : {len(district_df):>7}")
    print(f"  locality    : {len(locality_df):>7}")
    print(f"  stop_area   : {len(stop_area_df):>7}")
    print(f"  stop_point  : {len(stop_point_df):>7}")

    # ── 2. Region (root, no parent FK — nothing to filter) ──────────────
    #    Just keep original indices for remap.
    n_before = len(region_df)
    region_remap = {i: i for i in range(len(region_df))}  # identity — no rows dropped
    region_codes = set(region_df["region_code"]) if not region_df.empty else set()

    # ── 3. Authority → Region (via parent_index) ────────────────────────
    print("\n── Filtering Authority ────────────────────────")
    n_before = len(authority_df)
    if not authority_df.empty:
        region_index_set = set(str(i) for i in range(len(region_df)))
        bad = ~authority_df["parent_index"].isin(region_index_set)
        authority_df = authority_df[~bad]  # keep original index for remap
        authority_remap = _build_remap(authority_df.index)
        authority_df = authority_df.reset_index(drop=True)
    else:
        authority_remap = {}
    _report_drop("Authority", n_before, len(authority_df))
    total_dropped += n_before - len(authority_df)

    # Remap Region.admin_areas_list (list of authority indices)
    if not region_df.empty:
        region_df = _remap_list_col(region_df, "admin_areas_list", authority_remap)

    auth_codes = set(authority_df["admin_area_code"]) if not authority_df.empty else set()

    # ── 4. District → Authority (via parent_index) ──────────────────────
    print("\n── Filtering District ─────────────────────────")
    n_before = len(district_df)
    if not district_df.empty:
        authority_index_set = set(str(i) for i in range(len(authority_df)))
        bad = ~district_df["parent_index"].isin(authority_index_set)
        district_df = district_df[~bad]
        district_remap = _build_remap(district_df.index)
        district_df = district_df.reset_index(drop=True)
    else:
        district_remap = {}
    _report_drop("District", n_before, len(district_df))
    total_dropped += n_before - len(district_df)

    # Remap Authority.nptg_districts_list (list of district indices)
    if not authority_df.empty:
        authority_df = _remap_list_col(authority_df, "nptg_districts_list", district_remap)

    # Now remap District.parent_index to match (potentially filtered) authority positions
    # Since we already filtered authorities and reset their index, 
    # district parent_index values that survived still point at OLD authority indices.
    # We need to remap them.
    if not district_df.empty and authority_remap:
        district_df, bad_after_remap = _remap_index_col(district_df, "parent_index", authority_remap)
        if bad_after_remap.any():
            district_df = district_df[~bad_after_remap].reset_index(drop=True)
            # Rebuild district remap after second pass removal
            district_remap_2 = {i: i for i in range(len(district_df))}
            if not authority_df.empty:
                authority_df = _remap_list_col(authority_df, "nptg_districts_list", district_remap_2)

    # Similarly remap Authority.parent_index for filtered regions (identity if no regions dropped)
    if not authority_df.empty and region_remap:
        authority_df, _ = _remap_index_col(authority_df, "parent_index", region_remap)

    district_codes = set(district_df["nptg_district_code"]) if not district_df.empty else set()

    # ── 4b. Inject dummy district for code "310" ────────────────────────
    #   District 310 is a legacy catch-all code (Northumberland reorganisation).
    #   20,000+ localities reference it. Instead of dropping them all, we add
    #   a dummy district row so the FK is satisfied.
    DUMMY_DISTRICT_CODE = "310"
    if DUMMY_DISTRICT_CODE not in district_codes and not locality_df.empty:
        # Check that at least some localities actually reference it
        n_refs = (locality_df["nptg_district_ref"] == DUMMY_DISTRICT_CODE).sum()
        if n_refs > 0:
            # Assign to first authority (index 0) — it spans 100+ authorities,
            # there is no single correct parent.
            dummy_row = pd.DataFrame([{
                "nptg_district_code": DUMMY_DISTRICT_CODE,
                "name": "UNKNOWN / LEGACY (310)",
                "parent_index": "0",
            }])
            district_df = pd.concat([district_df, dummy_row], ignore_index=True)
            district_codes.add(DUMMY_DISTRICT_CODE)
            print(f"  🔹 Injected dummy district '{DUMMY_DISTRICT_CODE}' for {n_refs} localities")

    # ── 5. Locality → Authority, District, self ─────────────────────────
    print("\n── Filtering Locality ─────────────────────────")
    n_before = len(locality_df)
    if not locality_df.empty:
        # authority_ref → Authority.admin_area_code
        bad_auth = ~locality_df["authority_ref"].isin(auth_codes)

        # nptg_district_ref → District.nptg_district_code
        bad_dist = ~locality_df["nptg_district_ref"].isin(district_codes)

        # parent_nptg_locality_ref → Locality.nptg_locality_code (nullable)
        locality_codes_set = set(locality_df["nptg_locality_code"])
        has_parent = locality_df["parent_nptg_locality_ref"] != ""
        bad_parent = has_parent & ~locality_df["parent_nptg_locality_ref"].isin(locality_codes_set)

        bad_any = bad_auth | bad_dist | bad_parent
        locality_df = locality_df[~bad_any].reset_index(drop=True)

        # Second pass: dropping rows may orphan other parent_nptg_locality_refs
        locality_codes_set = set(locality_df["nptg_locality_code"])
        has_parent = locality_df["parent_nptg_locality_ref"] != ""
        bad_parent2 = has_parent & ~locality_df["parent_nptg_locality_ref"].isin(locality_codes_set)
        if bad_parent2.any():
            # NULL out dangling self-refs rather than cascade-dropping
            locality_df.loc[bad_parent2, "parent_nptg_locality_ref"] = ""

    _report_drop("Locality", n_before, len(locality_df))
    total_dropped += n_before - len(locality_df)

    locality_codes = set(locality_df["nptg_locality_code"]) if not locality_df.empty else set()

    # ── 6. Stop_Area → Authority ────────────────────────────────────────
    print("\n── Filtering Stop_Area ────────────────────────")
    n_before = len(stop_area_df)
    if not stop_area_df.empty:
        bad = ~stop_area_df["nptg_authority_ref"].isin(auth_codes)
        stop_area_df = stop_area_df[~bad].reset_index(drop=True)
    _report_drop("Stop_Area", n_before, len(stop_area_df))
    total_dropped += n_before - len(stop_area_df)

    stop_area_codes = set(stop_area_df["stop_area_code"]) if not stop_area_df.empty else set()

    # ── 7. Stop_Point → Authority, Stop_Area, Locality ─────────────────
    print("\n── Filtering Stop_Point ───────────────────────")
    n_before = len(stop_point_df)
    if not stop_point_df.empty:
        # nptg_authority_ref → Authority.admin_area_code
        bad_auth = ~stop_point_df["nptg_authority_ref"].isin(auth_codes)

        # stop_areas_ref → Stop_Area.stop_area_code (nullable → NULL out invalid)
        has_sa = stop_point_df["stop_areas_ref"] != ""
        bad_sa = has_sa & ~stop_point_df["stop_areas_ref"].isin(stop_area_codes)
        # NULL out invalid stop_areas_ref instead of dropping (it's a nullable FK)
        stop_point_df.loc[bad_sa, "stop_areas_ref"] = ""
        if bad_sa.any():
            print(f"  🔸 Stop_Point: nulled {int(bad_sa.sum())} invalid stop_areas_ref values")

        # place_nptg_locality_ref → Locality.nptg_locality_code (must exist)
        bad_loc = ~stop_point_df["place_nptg_locality_ref"].isin(locality_codes)

        # Only drop rows for non-nullable FK violations
        bad_drop = bad_auth | bad_loc
        stop_point_df = stop_point_df[~bad_drop].reset_index(drop=True)

    _report_drop("Stop_Point", n_before, len(stop_point_df))
    total_dropped += n_before - len(stop_point_df)

    # ── 8. Write ─────────────────────────────────────────────────────────
    print("\n══════════════════════════════════════════════")
    print(f"  Total rows dropped: {total_dropped}")
    print("══════════════════════════════════════════════\n")

    print("Writing cleaned CSVs …")
    _write(region_df,     "region.csv",     out_dir)
    _write(authority_df,  "authority.csv",  out_dir)
    _write(district_df,   "district.csv",  out_dir)
    _write(locality_df,   "locality.csv",  out_dir)
    _write(stop_area_df,  "stop_area.csv", out_dir)
    _write(stop_point_df, "stop_point.csv", out_dir)
    print("Done.\n")

    return total_dropped


# ── Bus cross-domain filter ──────────────────────────────────────────────────

def filter_bus(
    bus_dir: str = CSV_BUS_DIR,
    nptg_dir: str = CSV_DIR,
    out_dir: Optional[str] = None,
) -> int:
    """Drop bus_stop_point rows whose stop_point_ref is not in NaPTAN and
    cascade through route_link, route_location, route_section,
    journey_pattern_link, journey_pattern_section, vehicle_journey_link,
    and vehicle_journey.

    Returns total rows dropped across all tables.
    """
    if out_dir is None:
        out_dir = bus_dir
    os.makedirs(out_dir, exist_ok=True)

    total_dropped = 0

    print("\n══════════════════════════════════════════════")
    print("  Filter CSV_BUS: cross-domain FK cleanup")
    print("══════════════════════════════════════════════\n")

    # ── Load affected tables ─────────────────────────────────────────────
    nptg_sp = _read("stop_point.csv", nptg_dir)
    bus_sp  = _read("bus_stop_point.csv", bus_dir)
    rl      = _read("route_link.csv", bus_dir)
    rloc    = _read("route_location.csv", bus_dir)
    rs      = _read("route_section.csv", bus_dir)
    jpl     = _read("journey_pattern_link.csv", bus_dir)
    jps     = _read("journey_pattern_section.csv", bus_dir)
    vjl     = _read("vehicle_journey_link.csv", bus_dir)
    vj      = _read("vehicle_journey.csv", bus_dir)

    nptg_atco = set(nptg_sp["atco_code"]) if not nptg_sp.empty else set()
    if not nptg_atco:
        print("  ⚠️  No NaPTAN stop_point data — nothing to filter")
        return 0

    # ═════════════════════════════════════════════════════════════════════
    #  Phase 1: Identify all rows to drop
    # ═════════════════════════════════════════════════════════════════════

    # 1a) bus_stop_point: invalid cross-domain FK
    bad_sp_mask = ~bus_sp["stop_point_ref"].isin(nptg_atco)
    bad_sp_ints = set(bus_sp.loc[bad_sp_mask, "uid"].astype(int))
    bad_sp_refs = set(bus_sp.loc[bad_sp_mask, "stop_point_ref"])

    if not bad_sp_ints:
        print("  ✅ No invalid bus_stop_point.stop_point_ref — nothing to filter")
        return 0

    print(f"  bus_stop_point rows to drop  : {len(bad_sp_ints)}")
    print(f"  invalid stop_point_refs      : {sorted(bad_sp_refs)}")

    # 1b) route_link: references deleted bus_stop_point UIDs
    bad_sp_str = {str(u) for u in bad_sp_ints}
    rl_bad_mask = (
        rl["from_bus_stop_point_uid"].isin(bad_sp_str)
        | rl["to_bus_stop_point_uid"].isin(bad_sp_str)
    )
    bad_rl_ints = set(rl.loc[rl_bad_mask, "uid"].astype(int))
    print(f"  route_link rows to drop      : {len(bad_rl_ints)}")

    # 1c) route_location: owned by deleted route_links
    bad_rloc_ints: set[int] = set()
    for _, row in rl[rl_bad_mask].iterrows():
        s, e = row["start_route_location_index"], row["end_route_location_index"]
        if s not in ("", "-1") and e not in ("", "-1"):
            bad_rloc_ints.update(range(int(s), int(e) + 1))
    print(f"  route_location rows to drop  : {len(bad_rloc_ints)}")

    # 1d) journey_pattern_link: references deleted route_links or stop_point_refs
    bad_rl_str = {str(u) for u in bad_rl_ints}
    jpl_bad_mask = (
        jpl["route_link_uid"].isin(bad_rl_str)
        | jpl["from_point_point_id"].isin(bad_sp_refs)
        | jpl["to_point_point_id"].isin(bad_sp_refs)
    )
    bad_jpl_ints = set(jpl.loc[jpl_bad_mask, "uid"].astype(int))
    print(f"  journey_pattern_link to drop  : {len(bad_jpl_ints)}")

    # 1e) vehicle_journey_link: references deleted JPLs
    bad_jpl_str = {str(u) for u in bad_jpl_ints}
    vjl_bad_mask = vjl["JP_link_uid"].isin(bad_jpl_str)
    bad_vjl_ints = set(vjl.loc[vjl_bad_mask, "uid"].astype(int))
    print(f"  vehicle_journey_link to drop  : {len(bad_vjl_ints)}")

    # ═════════════════════════════════════════════════════════════════════
    #  Phase 2: Drop rows, rebuild UIDs, build remap arrays
    # ═════════════════════════════════════════════════════════════════════
    print()

    # bus_stop_point
    n0 = len(bus_sp)
    sp_remap = _make_remap_array(n0, bad_sp_ints)
    bus_sp = bus_sp[~bad_sp_mask].reset_index(drop=True)
    bus_sp["uid"] = [str(i) for i in range(len(bus_sp))]
    _report_drop("bus_stop_point", n0, len(bus_sp))
    total_dropped += n0 - len(bus_sp)

    # route_link
    n0 = len(rl)
    rl_remap = _make_remap_array(n0, bad_rl_ints)
    rl = rl[~rl_bad_mask].reset_index(drop=True)
    rl["uid"] = [str(i) for i in range(len(rl))]
    _report_drop("route_link", n0, len(rl))
    total_dropped += n0 - len(rl)

    # route_location
    n0 = len(rloc)
    rloc_remap = _make_remap_array(n0, bad_rloc_ints)
    rloc_drop = rloc["uid"].astype(int).isin(bad_rloc_ints)
    rloc = rloc[~rloc_drop].reset_index(drop=True)
    rloc["uid"] = [str(i) for i in range(len(rloc))]
    _report_drop("route_location", n0, len(rloc))
    total_dropped += n0 - len(rloc)

    # journey_pattern_link
    n0 = len(jpl)
    jpl_remap = _make_remap_array(n0, bad_jpl_ints)
    jpl = jpl[~jpl_bad_mask].reset_index(drop=True)
    jpl["uid"] = [str(i) for i in range(len(jpl))]
    _report_drop("journey_pattern_link", n0, len(jpl))
    total_dropped += n0 - len(jpl)

    # vehicle_journey_link
    n0 = len(vjl)
    vjl_remap = _make_remap_array(n0, bad_vjl_ints)
    vjl = vjl[~vjl_bad_mask].reset_index(drop=True)
    vjl["uid"] = [str(i) for i in range(len(vjl))]
    _report_drop("vehicle_journey_link", n0, len(vjl))
    total_dropped += n0 - len(vjl)

    # ═════════════════════════════════════════════════════════════════════
    #  Phase 3: Apply remaps to FK columns in surviving rows
    # ═════════════════════════════════════════════════════════════════════
    print("\n── Remapping FK columns ───────────────────────")

    # route_link.from/to_bus_stop_point_uid  (non-nullable)
    rl["from_bus_stop_point_uid"] = sp_remap[
        rl["from_bus_stop_point_uid"].astype(int).values
    ].astype(str)
    rl["to_bus_stop_point_uid"] = sp_remap[
        rl["to_bus_stop_point_uid"].astype(int).values
    ].astype(str)
    print("  ✅ route_link.from/to_bus_stop_point_uid")

    # route_link.start/end_route_location_index  (nullable, sentinel = '-1'/'')
    rl = _remap_range_columns(
        rl, "start_route_location_index", "end_route_location_index",
        rloc_remap, nullable=True,
    )
    print("  ✅ route_link.start/end_route_location_index")

    # route_section.start/end_route_link_index  (non-nullable)
    rs = _remap_range_columns(
        rs, "start_route_link_index", "end_route_link_index", rl_remap,
    )
    n_empty_rs = ((rs["start_route_link_index"] == "-1")
                  & (rs["end_route_link_index"] == "-1")).sum()
    if n_empty_rs:
        print(f"  ⚠️  {n_empty_rs} route_sections fully emptied")
    print("  ✅ route_section.start/end_route_link_index")

    # journey_pattern_link.route_link_uid  (non-nullable)
    jpl["route_link_uid"] = rl_remap[
        jpl["route_link_uid"].astype(int).values
    ].astype(str)
    print("  ✅ journey_pattern_link.route_link_uid")

    # journey_pattern_section.start/end_JP_link_index  (non-nullable)
    jps = _remap_range_columns(
        jps, "start_JP_link_index", "end_JP_link_index", jpl_remap,
    )
    n_empty_jps = ((jps["start_JP_link_index"] == "-1")
                   & (jps["end_JP_link_index"] == "-1")).sum()
    if n_empty_jps:
        print(f"  ⚠️  {n_empty_jps} JP_sections fully emptied")
    print("  ✅ journey_pattern_section.start/end_JP_link_index")

    # vehicle_journey_link.JP_link_uid  (non-nullable)
    vjl["JP_link_uid"] = jpl_remap[
        vjl["JP_link_uid"].astype(int).values
    ].astype(str)
    print("  ✅ vehicle_journey_link.JP_link_uid")

    # vehicle_journey.VJ_link_start/end_index  (nullable)
    vj = _remap_range_columns(
        vj, "VJ_link_start_index", "VJ_link_end_index",
        vjl_remap, nullable=True,
    )
    print("  ✅ vehicle_journey.VJ_link ranges")

    # ═════════════════════════════════════════════════════════════════════
    #  Phase 4: Write modified CSVs
    # ═════════════════════════════════════════════════════════════════════
    print(f"\n  Total rows dropped across all bus tables: {total_dropped}")
    print("\nWriting filtered CSVs …")
    _write(bus_sp, "bus_stop_point.csv", out_dir)
    _write(rl,     "route_link.csv",    out_dir)
    _write(rloc,   "route_location.csv", out_dir)
    _write(rs,     "route_section.csv", out_dir)
    _write(jpl,    "journey_pattern_link.csv", out_dir)
    _write(jps,    "journey_pattern_section.csv", out_dir)
    _write(vjl,    "vehicle_journey_link.csv", out_dir)
    _write(vj,     "vehicle_journey.csv", out_dir)
    print("Done.\n")

    return total_dropped


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    filter_nptg()
    filter_bus()


# if __name__ == "__main__":
#     main()
