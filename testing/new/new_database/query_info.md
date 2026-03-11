# query.py — Database Query API Reference

This document describes how to use the `query.py` module to replace the fake data
in `webserver_task.py` with real database queries. All methods follow the interface
spec defined in `API_specification.html`.

---

## Table of Contents

1. [Connection & Setup](#connection--setup)
2. [Data Transformation: DB → API Spec](#data-transformation-db--api-spec)
3. [Endpoint Integration Guide](#endpoint-integration-guide)
   - [GET /map — MapService](#get-map--mapservice)
   - [GET /timetable — TimetableLookup](#get-timetable--timetablelookup)
   - [GET /services/\<id\> — ServiceLookup](#get-servicesid--servicelookup)
   - [GET /tracking/\<id\> — LiveTracking](#get-trackingid--livetracking)
   - [GET /routes — RouteService](#get-routes--routeservice)
4. [Full Method Reference](#full-method-reference)
5. [Day-of-Week Convention](#day-of-week-convention)
6. [Error / Invalid Input Behaviour](#error--invalid-input-behaviour)

---

## Connection & Setup

`TransportDatabase` is a context-manager class that manages a SQLite connection
to `nptg_naptan.db`.

```python
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'new_database'))
from query import TransportDatabase

# Recommended: use as a context manager (auto-opens and closes)
with TransportDatabase() as db:
    stops = db.get_stops_in_area(54.05, -2.80)

# Or manage the lifecycle manually
db = TransportDatabase()
db.connect()
stops = db.get_stops_in_area(54.05, -2.80)
db.close()
```

For the webserver, **open one connection per request** (context manager recommended)
or keep a long-lived instance at module level — the underlying SQLite connection is
lightweight.

### Database Location

By default the constructor finds `nptg_naptan.db` relative to `query.py` itself.
You can override with an explicit path:

```python
db = TransportDatabase(db_path="/absolute/path/to/nptg_naptan.db")
```

---

## Data Transformation: DB → API Spec

The API specification defines a **Node** as:

```json
{
  "id":       "250012188",
  "type":     "bus",
  "name":     "Sulby Drive",
  "location": { "lat": 54.039601822, "long": -2.79614868 }
}
```

`query.py` returns raw DB rows as dictionaries with column names like
`SP_atco_code`, `SP_name`, `SP_latitude`, `SP_longitude`. You must transform them.

### Stop → Node

```python
def stop_to_node(stop: dict) -> dict:
    """Convert a query.py stop dict to an API Node object."""
    return {
        "id":       stop["SP_atco_code"],
        "type":     "bus",                       # all DB stops are bus stops
        "name":     stop["SP_name"],
        "location": {
            "lat":  stop["SP_latitude"],
            "long": stop["SP_longitude"]
        }
    }
```

### Vehicle Journey → Timetable Arrival

```python
from datetime import datetime, timezone, timedelta

def vj_to_arrival(vj: dict, route_info: dict, query_date: datetime) -> dict:
    """Convert a vehicle-journey row + route info to a Timetable arrival."""
    # departure_time is stored as "HH:MM:SS"
    h, m, s = map(int, vj["departure_time"].split(":"))
    dep_dt = query_date.replace(hour=h, minute=m, second=s, tzinfo=timezone.utc)

    return {
        "time":     dep_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "name":     route_info.get("line_name", route_info.get("JP_id", "")),
        "operator": vj.get("operator_name", "Unknown"),
        "status":   "scheduled"        # no live data yet
    }
```

### Route Stops → Path

```python
def route_stops_to_path(stops: list[dict], start_atco: str, end_atco: str) -> dict:
    """Build a Path object from ordered route stops."""
    return {
        "from": start_atco,
        "to":   end_atco,
        "path": [
            {"lat": s["SP_latitude"], "long": s["SP_longitude"]}
            for s in stops
        ]
    }
```

---

## Endpoint Integration Guide

Below is the concrete code to replace each `# TODO` in `webserver_task.py`.

---

### GET /map — MapService

**Spec**: Return all bus stops as Node objects + Transport Metadata.

**query.py method**: `db.get_stops_in_area(lat, lon, radius_km)` for nearby stops,
or fetch all stops if a full map is needed.

```python
def map_service(lat: float, long: float) -> tuple[dict, int]:
    with TransportDatabase() as db:
        # Get all stops within ~50 km to cover the visible map area
        # Adjust radius as needed, or fetch all Stop_Point rows for full coverage
        stops = db.get_stops_in_area(lat, long, radius_km=50.0)

    nodes = [stop_to_node(s) for s in stops]

    information = {
        "weather": {
            "weather": "unknown",
            "location": {"lat": lat, "long": long}
        },
        "messages": [],
        "warnings": []
    }

    body = {
        "api-version":   API_VERSION,
        "response-type": "map",
        "nodes":         nodes,
        "information":   information
    }
    return body, 200
```

> **Cached variant** (`/map?cached=true`): skip the DB call, return `"nodes": []`
> and only the `"information"` block (already implemented in `map_service_cached`).

---

### GET /timetable — TimetableLookup

**Spec**: For each requested Node ID, return a Timetable: list of upcoming arrivals
with time, service name, operator, and status.

**query.py methods**:
1. `db.get_routes_at_stop(atco_code)` — find which journey patterns serve the stop
2. `db.get_vehicle_journeys(jp_uid, day_of_week)` — get departure times per JP

```python
from datetime import datetime, timezone

def timetable_lookup(query_time: datetime, node_ids: list[str]) -> tuple[dict, int]:
    timetables = {}
    excluded   = []

    # Derive day-of-week (0=Monday, 6=Sunday) from the query time
    dow = query_time.weekday()

    with TransportDatabase() as db:
        for node_id in node_ids:
            stop = db.get_stop_by_code(node_id)
            if stop is None:
                excluded.append(node_id)
                continue

            routes = db.get_routes_at_stop(node_id)
            arrivals = []

            for route in routes:
                jp_uid = route["JP_UID"]
                journeys = db.get_vehicle_journeys(jp_uid, dow)

                for vj in journeys:
                    h, m, s = map(int, vj["departure_time"].split(":"))
                    dep_dt = query_time.replace(
                        hour=h, minute=m, second=s, tzinfo=timezone.utc
                    )
                    # Only include future arrivals relative to query_time
                    if dep_dt >= query_time:
                        arrivals.append({
                            "time":     dep_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "name":     route.get("line_name", ""),
                            "operator": route.get("operator_name", "Unknown"),
                            "status":   "scheduled"
                        })

            # Sort by time, limit to reasonable number
            arrivals.sort(key=lambda a: a["time"])
            arrivals = arrivals[:50]

            timetables[node_id] = {
                "arrivals":     arrivals,
                "last-updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "from-time":    query_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "node":         node_id
            }

    body = {
        "api-version":   API_VERSION,
        "response-type": "timetable",
        "excluded":      excluded,
        "timetables":    timetables
    }
    return body, 200
```

**Key details**:
- `departure_time` is stored as `"HH:MM:SS"` (e.g. `"07:30:00"`).
- Day-of-week bitmask: Monday = bit 0, Sunday = bit 6 (use `query_time.weekday()`
  directly — Python's weekday() already returns 0 = Monday).
- Invalid node IDs go into `"excluded"` instead of raising an error (per spec).

---

### GET /services/\<id\> — ServiceLookup

**Spec**: Return service paths through a Node — the graphical route each service
takes on the map.

**query.py methods**:
1. `db.get_routes_at_stop(atco_code)` — get journey patterns through this stop
2. `db.get_route_stops(jp_uid)` — get ordered stops for each JP (to build Path)

```python
def service_lookup(node_id: str) -> tuple[dict, int]:
    with TransportDatabase() as db:
        stop = db.get_stop_by_code(node_id)
        if stop is None:
            return {
                "api-version":   API_VERSION,
                "response-type": "service",
                "node":          node_id,
                "services":      []
            }, 404

        routes = db.get_routes_at_stop(node_id)
        services = []
        seen_lines = set()

        for route in routes:
            line_name = route.get("line_name", "")
            if line_name in seen_lines:
                continue          # avoid duplicate service entries
            seen_lines.add(line_name)

            jp_uid = route["JP_UID"]
            stops = db.get_route_stops(jp_uid)

            if len(stops) < 2:
                continue

            first_atco = stops[0]["atco_code"]
            last_atco  = stops[-1]["atco_code"]

            path = {
                "from": first_atco,
                "to":   last_atco,
                "path": [
                    {"lat": s["SP_latitude"], "long": s["SP_longitude"]}
                    for s in stops
                ]
            }

            services.append({
                "service-name": line_name,
                "path":         path
            })

    body = {
        "api-version":   API_VERSION,
        "response-type": "service",
        "node":          node_id,
        "services":      services
    }
    return body, 200
```

---

### GET /tracking/\<id\> — LiveTracking

**Spec**: Return live vehicle positions near a Node.

**No database support** — the DB contains only static timetable data. Return an
empty vehicles list until a live-tracking data source is integrated.

```python
def live_tracking(node_id: str) -> tuple[dict, int]:
    body = {
        "api-version":   API_VERSION,
        "response-type": "live-tracking",
        "node":          node_id,
        "vehicles":      []              # no live data available yet
    }
    return body, 200
```

---

### GET /routes — RouteService

**Spec**: Return suggested routes between a source and destination stop.

**query.py methods**:
1. `db.get_routes_between_stops(source_atco, dest_atco)` — direct journey patterns
2. `db.get_route_stops(jp_uid)` — ordered stop sequence to build Paths
3. `db.get_vehicle_journeys(jp_uid, day)` — departure times to pick the best option

```python
def route_service(source_id: str, dest_id: str,
                  query_time: datetime) -> tuple[dict, int]:
    dow = query_time.weekday()

    with TransportDatabase() as db:
        # Verify both stops exist
        src = db.get_stop_by_code(source_id)
        dst = db.get_stop_by_code(dest_id)
        if src is None or dst is None:
            return {
                "api-version":   API_VERSION,
                "response-type": "route",
                "source":        source_id,
                "destination":   dest_id,
                "routes":        []
            }, 404

        # Find direct journey patterns connecting the two stops
        jps = db.get_routes_between_stops(source_id, dest_id)

        routes = []
        for jp in jps:
            jp_uid = jp["JP_UID"]
            vjs = db.get_vehicle_journeys(jp_uid, dow)

            # Find the first departure at or after query_time
            for vj in vjs:
                h, m, s = map(int, vj["departure_time"].split(":"))
                dep_dt = query_time.replace(
                    hour=h, minute=m, second=s, tzinfo=timezone.utc
                )
                if dep_dt < query_time:
                    continue

                # Build the route stops for a Path
                stops = db.get_route_stops(jp_uid)
                first_atco = stops[0]["atco_code"]  if stops else source_id
                last_atco  = stops[-1]["atco_code"] if stops else dest_id

                # Compute approximate duration from run_time sum
                total_minutes = sum(
                    (st.get("travel_time_from_prev") or 0) for st in stops
                ) // 60  # run_time is in seconds

                route_obj = {
                    "duration": total_minutes,
                    "type":     [],                 # classification TBD
                    "travel": [
                        {
                            "departure": dep_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "duration":  total_minutes,
                            "node":      source_id,
                            "type":      "bus"
                        }
                    ],
                    "destination": dest_id,
                    "path": [
                        {
                            "from": first_atco,
                            "to":   last_atco,
                            "path": [
                                {"lat": s["SP_latitude"], "long": s["SP_longitude"]}
                                for s in stops
                            ]
                        }
                    ]
                }
                routes.append(route_obj)
                break                # one departure per JP is enough

    if not routes:
        return {
            "api-version":   API_VERSION,
            "response-type": "route",
            "source":        source_id,
            "destination":   dest_id,
            "routes":        []
        }, 500                           # spec: 500 if no route found

    body = {
        "api-version":   API_VERSION,
        "response-type": "route",
        "source":        source_id,
        "destination":   dest_id,
        "routes":        routes
    }
    return body, 200
```

---

## Full Method Reference

### Stop Queries

| Method | Params | Returns | Notes |
|--------|--------|---------|-------|
| `get_stop_by_code(atco_code)` | `str` | `Dict \| None` | Full Stop_Point row |
| `search_stops_by_name(pattern, limit=20)` | `str, int` | `List[Dict]` | Case-insensitive LIKE `%pattern%` |
| `get_stops_in_locality(locality_code)` | `str` | `List[Dict]` | All stops in NPTG locality |
| `get_stops_in_area(lat, lon, radius_km=1.0)` | `float, float, float` | `List[Dict]` | Bounding-box approximation |

### Route & Journey Queries

| Method | Params | Returns | Notes |
|--------|--------|---------|-------|
| `get_routes_at_stop(atco_code)` | `str` | `List[Dict]` | Journey patterns through stop; includes `JP_UID`, `line_name`, `operator_name` |
| `get_routes_between_stops(start, end)` | `str, str` | `List[Dict]` | Direct JPs connecting two stops |
| `get_route_stops(jp_uid)` | `int` | `List[Dict]` | Ordered stops with `atco_code`, `SP_name`, `SP_latitude`, `SP_longitude`, `sequence`, `travel_time_from_prev` |

### Service & Operator Queries

| Method | Params | Returns | Notes |
|--------|--------|---------|-------|
| `get_services_by_operator(code)` | `str` | `List[Dict]` | Services by operator code |
| `get_lines_for_service(code)` | `str` | `List[Dict]` | Lines for a service code |
| `get_operators()` | — | `List[Dict]` | All operators |

### Timetable Queries

| Method | Params | Returns | Notes |
|--------|--------|---------|-------|
| `get_vehicle_journeys(jp_uid, day_of_week=None)` | `int, int\|None` | `List[Dict]` | Each dict has `VJ_UID`, `departure_time` (`"HH:MM:SS"`), `operator_name` |
| `get_timetable_for_route(jp_uid, day_of_week=None)` | `int, int\|None` | `Dict` | Combined `{route_info, stops, journeys}` |

### Location / Geographic Queries

| Method | Params | Returns | Notes |
|--------|--------|---------|-------|
| `get_localities(auth_code=None)` | `str\|None` | `List[Dict]` | All localities, or filtered by authority |
| `get_authorities(region_code=None)` | `str\|None` | `List[Dict]` | All authorities, or filtered by region |
| `get_regions()` | — | `List[Dict]` | All NPTG regions |

### Convenience (standalone functions)

| Function | Params | Notes |
|----------|--------|-------|
| `find_routes(start, end, max_results=10)` | `str, str, int` | Wrapper for `get_routes_between_stops` |
| `get_stops_by_name(pattern, limit=20)` | `str, int` | Wrapper for `search_stops_by_name` |
| `get_nearby_stops(lat, lon, radius_km=1.0)` | `float, float, float` | Wrapper for `get_stops_in_area` |

---

## Day-of-Week Convention

The database stores days of operation as a **bitmask** in `DOW_days`:

| Day       | Bit Position | Bitmask Value | Python `weekday()` |
|-----------|:------------:|:-------------:|:-------------------:|
| Monday    | 0            | 1             | 0                   |
| Tuesday   | 1            | 2             | 1                   |
| Wednesday | 2            | 4             | 2                   |
| Thursday  | 3            | 8             | 3                   |
| Friday    | 4            | 16            | 4                   |
| Saturday  | 5            | 32            | 5                   |
| Sunday    | 6            | 64            | 6                   |

`query.py` handles this internally — just pass `day_of_week` as an integer
`0–6` (matching Python's `datetime.weekday()`).

```python
from datetime import datetime
dow = datetime.now().weekday()  # 0 = Monday, 6 = Sunday
journeys = db.get_vehicle_journeys(jp_uid=2795, day_of_week=dow)
```

---

## Error / Invalid Input Behaviour

All methods are safe to call with invalid inputs:

| Scenario | Return value |
|----------|-------------|
| Non-existent ATCO code | `None` (single) or `[]` (list) |
| Empty string pattern | `[]` |
| Bogus locality / auth / region code | `[]` |
| Coordinates in the ocean | `[]` |
| SQL-injection strings (`'; DROP TABLE`) | `[]` — parameterised queries prevent injection |
| Non-existent JP_UID | `[]` or raises `TypeError` on `get_timetable_for_route` (route_info row is None) |

The webserver should map these to appropriate HTTP status codes per the API spec
(e.g. 404 for invalid Node ID).
