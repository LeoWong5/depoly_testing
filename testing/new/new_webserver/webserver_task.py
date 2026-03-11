from webserver_component import Server_Logger, stop_to_node
from webserver_weather import Weather_Service
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
import json
import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Database access ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(SCRIPT_DIR, '..', 'new_database'))
from query import TransportDatabase

global API_VERSION, logger, counter, weather_service, service_lookup_cache

API_VERSION : str = ""
logger = None
counter : int = 0
weather_service: Weather_Service | None = None
SERVICE_CACHE_MAX = 512
service_lookup_cache: OrderedDict = OrderedDict()



def set_api_version(v : str):
	global API_VERSION
	API_VERSION = v

def set_server_logger(log : Server_Logger):
	global logger
	logger = log

def counter_fetch_add() -> int:
	global counter
	data = counter
	counter += 1
	return data

def set_weather_service(ws: Weather_Service):
    global weather_service
    weather_service = ws

def load_json(relative_path : str) -> dict:
	# resolve relative to the webserver_task.py directory, not cwd
	absolute_path = os.path.join(SCRIPT_DIR, relative_path)
	with open(absolute_path, "r", encoding="utf-8") as f:
		return json.load(f)


def to_iso_utc(value: datetime) -> str:
	if value.tzinfo is None:
		value = value.replace(tzinfo=timezone.utc)
	return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_departure_as_datetime(query_time: datetime, departure_time_raw: str) -> datetime | None:
	if not isinstance(departure_time_raw, str):
		return None

	cleaned = departure_time_raw.strip()
	if cleaned == "":
		return None

	# Common DB format: HH:MM:SS
	for fmt in ("%H:%M:%S", "%H:%M"):
		try:
			parsed = datetime.strptime(cleaned, fmt)
			candidate = query_time.replace(
				hour=parsed.hour,
				minute=parsed.minute,
				second=parsed.second,
				microsecond=0,
			)
			if candidate < query_time:
				candidate += timedelta(days=1)
			return candidate
		except ValueError:
			continue

	# Fallback for already-ISO values if they appear.
	try:
		parsed_iso = datetime.strptime(cleaned, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
		return parsed_iso
	except ValueError:
		return None












def map_service(EVENT_ID : int, lat : float, long : float) -> tuple[dict, int]:
    # ── real DB implementation ──
    # logger.log_continue("start db query (get_stops_in_area)", EVENT_ID)
    with TransportDatabase() as db:
        stops = db.get_stops_in_area(lat, long, radius_km=3.5)
    # logger.log_continue(f"end db query — {len(stops)} stops", EVENT_ID)

    nodes = [stop_to_node(s) for s in stops]

    if weather_service is not None:
        weather_obj = weather_service.get_weather_obj(lon=long, lat=lat)
    else:
        weather_obj = {"weather": "unknown", "location": {"lat": lat, "long": long}}

    information = {
        "weather": weather_obj,
        "messages": [],
        "warnings": []
    }
    body = {
        "api-version":   API_VERSION,
        "response-type": "map",
        "nodes":         nodes,
        "information":   information
    }
    # logger.log_continue(f"end computation — {len(nodes)} nodes", EVENT_ID)
    return body, 200

def map_service_cached(EVENT_ID : int, lat : float, long : float) -> tuple[dict, int]:
    # logger.log_continue("cached map — no db query", EVENT_ID)
    if weather_service is not None:
        weather_obj = weather_service.get_weather_obj(lon=long, lat=lat)
    else:
        weather_obj = {"weather": "unknown", "location": {"lat": lat, "long": long}}

    information = {
        "weather": weather_obj,
        "messages": [],
        "warnings": []
    }
    body = {
        "api-version": API_VERSION,
        "response-type": "map",
        "nodes": [],          # should be empty for cached response
        "information": information
    }

    # logger.log_continue("end computation", EVENT_ID)
    return body, 200

def timetable_lookup(EVENT_ID: int, query_time: datetime, nodes_ids: list[str]) -> tuple[dict, int]:
    if query_time.tzinfo is None:
        query_time = query_time.replace(tzinfo=timezone.utc)

    timetables = {}
    excluded   = []
    dow = query_time.weekday()          # 0=Monday … 6=Sunday
    query_time_hms = query_time.strftime("%H:%M:%S")

    with TransportDatabase() as db:
        # 1. Bulk validate all requested stops
        valid_stops = db.get_stops_batch(nodes_ids)
        
        valid_node_ids = []
        for node_id in nodes_ids:
            if node_id not in valid_stops:
                excluded.append(node_id)
            else:
                valid_node_ids.append(node_id)

        # 2. Bulk fetch upcoming arrivals for all valid stops
        batched_arrivals = db.get_upcoming_arrivals_batch(valid_node_ids, dow, query_time_hms)

 		# 3. Process the batched results purely in memory
        for node_id in valid_node_ids:
            arrivals = []
            upcoming = batched_arrivals.get(node_id, [])
            
            # THE FIX: Keep track of buses we've already seen to prevent duplicates
            seen_arrivals = set() 

            for row in upcoming:
                dep_dt = parse_departure_as_datetime(query_time, row.get("departure_time", ""))
                if dep_dt is None:
                    continue

                iso_time = to_iso_utc(dep_dt)
                line_name = row.get("line_name", "")
                
                # Create a unique fingerprint (e.g., "06:28" and "Line 100")
                fingerprint = (iso_time, line_name)
                
                # If we already added this exact bus at this exact time, skip it
                if fingerprint in seen_arrivals:
                    continue
                
                # Otherwise, remember it and add it to our list
                seen_arrivals.add(fingerprint)

                arrivals.append({
                    "time":     iso_time,
                    "name":     line_name,
                    "operator": row.get("operator_name", "Unknown"),
                    "status":   "scheduled"
                })

            # Sort and strictly cap at 50 per stop
            arrivals.sort(key=lambda a: a["time"])
            arrivals = arrivals[:50]

            timetables[node_id] = {
                "arrivals":     arrivals,
                "last-updated": to_iso_utc(datetime.now(timezone.utc)),
                "from-time":    to_iso_utc(query_time),
                "node":         node_id
            }

    body = {
        "api-version":   API_VERSION,
        "response-type": "timetable",
        "excluded":      excluded,
        "timetables":    timetables
    }
    
    return body, 200


def service_lookup(EVENT_ID: int, node_id: str) -> tuple[dict, int]:
    dow = datetime.now(timezone.utc).weekday()  # 0=Mon … 6=Sun
    cache_key = (node_id, dow)

    # Cache hit — move to end (most-recently used) and return
    if cache_key in service_lookup_cache:
        service_lookup_cache.move_to_end(cache_key)
        return service_lookup_cache[cache_key]
    
    with TransportDatabase() as db:
        # 1. Validate the stop
        stop = db.get_stop_by_code(node_id)
        if stop is None:
            return {
                "api-version":   API_VERSION,
                "response-type": "service",
                "node":          node_id,
                "services":      []
            }, 404
    
        # 2. Get active routes for the stop
        active_jps = db.get_active_jp_for_stop(node_id, dow)
        
        # If no active routes, return early to avoid unnecessary processing
        if not active_jps:
            result = ({
                "api-version":   API_VERSION,
                "response-type": "service",
                "node":          node_id,
                "services":      []
            }, 200)
            service_lookup_cache[cache_key] = result
            if len(service_lookup_cache) > SERVICE_CACHE_MAX:
                service_lookup_cache.popitem(last=False)
            return result

        # 3. Extract all Journey Pattern UIDs
        jp_uids = [row["JP_UID"] for row in active_jps]
        
        # 4. THE FIX: Fetch all stops and polylines in bulk (Batching)
        stops_batch = db.get_route_stops_batch(jp_uids)
        polylines_batch = db.get_route_polylines_batch(jp_uids)
        
        services = []
        
        # 5. Process the results entirely in memory
        for row in active_jps:
            line_name = row["line_name"]
            jp_uid    = row["JP_UID"]
    
            # Pull from our pre-fetched dictionaries instead of hitting the DB
            stops = stops_batch.get(jp_uid, [])
            if len(stops) < 2:
                continue
    
            poly = polylines_batch.get(jp_uid, [])
    
            services.append({
                "service-name": line_name,
                "path": {
                    "from": stops[0]["atco_code"],
                    "to":   stops[-1]["atco_code"],
                    "path": poly
                }
            })
    
    # 6. Construct and return the final payload
    result = ({
        "api-version":   API_VERSION,
        "response-type": "service",
        "node":          node_id,
        "services":      services
    }, 200)

    service_lookup_cache[cache_key] = result
    if len(service_lookup_cache) > SERVICE_CACHE_MAX:
        service_lookup_cache.popitem(last=False)
    return result


def live_tracking(EVENT_ID : int, node_id : str) -> tuple[dict, int]:
	# logger.log_continue("live_tracking — no db support yet", EVENT_ID)

	body = {
		"api-version":   API_VERSION,
		"response-type": "tracking", 
		"node":          node_id,
		"vehicles":      []    # TODO: populate with live data
	}

	# logger.log_continue("end computation", EVENT_ID)
	return body, 200

def route_service(EVENT_ID : int, source_id : str, dest_id : str, query_time : datetime) -> tuple[dict, int]:
	# logger.log_continue("route_service — no db support yet", EVENT_ID)

	body = {
		"api-version":   API_VERSION,
		"response-type": "route",
		"source":        source_id,
		"destination":   dest_id,
		"routes":        []    # TODO: populate with live data
	}

	# logger.log_continue("end computation", EVENT_ID)
	return body, 200

def weather_lookup(EVENT_ID: int, lat: float, long: float) -> tuple[dict, int]:
    if weather_service is None:
        return {
            "api-version":   API_VERSION,
            "response-type": "weather",
            "error":         "Weather service unavailable"
        }, 503

    weather_obj = weather_service.get_weather_obj(lon=long, lat=lat)

    if weather_obj.get("weather") == "unknown":
        return {
            "api-version":   API_VERSION,
            "response-type": "weather",
            "error":         "Weather data unavailable for this location"
        }, 503

    return {
        "api-version":   API_VERSION,
        "response-type": "weather",
        "weather":       weather_obj
    }, 200