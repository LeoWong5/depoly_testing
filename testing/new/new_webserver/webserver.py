"""
webserver.py
============
Flask back-end for the Transport Application API.

Usage:
	python webserver.py <port>

Example:
	python webserver.py 8080

Visit http://localhost:<port>/ for the fake front end UI.

Install dependencies:
	pip install flask flask-cors
"""

import sys
import os
from datetime import datetime, timezone

from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS

from webserver_component import Server_Logger
from webserver_weather import Weather_Service
import webserver_task as task
from concurrent.futures import ProcessPoolExecutor

# ── App setup ──────────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app)   # allow cross-origin requests (needed if HTML opened as a local file)

API_VERSION = "1.2.0"
HERE = os.path.dirname(os.path.abspath(__file__))
logger = Server_Logger()
ws = Weather_Service()

def _worker_init(api_version: str, log) -> None:
	"""Initialise webserver_task globals in each pool worker process."""
	task.set_api_version(api_version)
	task.set_server_logger(log)
	task.set_weather_service(ws)


process_pool = ProcessPoolExecutor(
	max_workers=2,
	initializer=_worker_init,
	initargs=(API_VERSION, logger),
)

# ── Helper ──────────────────────────────────────────────────────────────
def error(code, message):
	return jsonify({
		"api-version": API_VERSION,
		"response-type": "error",
		"error": message
	}), code


def typed_error(response_type, code, message, **extra_fields):
	body = {
		"api-version": API_VERSION,
		"response-type": response_type,
		"error": message,
	}
	body.update(extra_fields)
	return jsonify(body), code


# ── Serve the fake front end ───────────────────────────────────────────

@app.route("/")
def index():
	"""Serve built front end if available, otherwise fallback fake front end HTML page."""

	EVENT_ID = logger.log_request("User request index")

	built_html_path = os.path.join(HERE, "index.html")
	html_path = built_html_path if os.path.isfile(built_html_path) else os.path.join(HERE, "fake_front_end.html")

	logger.log_response(f"Server return index", EVENT_ID)

	return send_file(html_path)


@app.route("/assets/<path:filename>")
def built_assets(filename):
	"""Serve built Vite asset files copied into new_webserver/assets."""
	assets_dir = os.path.join(HERE, "assets")
	return send_from_directory(assets_dir, filename)


@app.route("/vite.svg")
def vite_favicon():
	"""Serve Vite favicon from built frontend output."""
	return send_from_directory(HERE, "vite.svg")


# ── API endpoints ───────────────────────────────────────────────────

@app.route("/map")
def map_service():
	"""
	MapService
	----------
	GET /map?lat=<number>&long=<number>
		Returns all nodes + transport metadata.

	GET /map?cached=true?lat=<number>?long=<number>
		Returns only transport metadata (no nodes array).
	"""

	# The spec's cached variant uses '?' as separator throughout, so Flask won't
	# automatically parse lat/long from that form. We handle both styles here.
	raw_url = request.query_string.decode()

	EVENT_ID = logger.log_request(f"User request map service = {raw_url}")

	cached_arg = (request.args.get("cached") or "").strip().lower()
	cached = cached_arg in ("1", "true", "yes")
	lat_str = request.args.get("lat")
	long_str = request.args.get("long")

	# Fallback parser for non-standard '?'-separated query strings.
	if (lat_str is None or long_str is None or (not cached and "cached=true" in raw_url)) and raw_url:
		parts = {}
		normalized_query = raw_url.replace("?", "&")
		for segment in normalized_query.split("&"):
			if "=" in segment:
				k, _, v = segment.partition("=")
				parts[k] = v

		if lat_str is None:
			lat_str = parts.get("lat")
		if long_str is None:
			long_str = parts.get("long")
		if not cached:
			cached_fallback = (parts.get("cached") or "").strip().lower()
			cached = cached_fallback in ("1", "true", "yes")

	# Validate lat/long
	if lat_str is None or long_str is None:
		logger.log_response(f"Server return error 400 -> Invalid lat/log", EVENT_ID)
		return error(400, "Missing required parameters: lat, long")
	try:
		lat   = float(lat_str)
		long = float(long_str)

	except ValueError:
		logger.log_response(f"Server return error 400 -> Invalid lat/log", EVENT_ID)
		return error(400, f"Invalid lat/long values: lat={lat_str!r}, long={long_str!r}")

	logger.log_continue(f"Server parse valid map arg, lat = {lat}, log = {long}, cached = {cached}", EVENT_ID)

	
	if (cached):
		result = process_pool.submit(task.map_service_cached, EVENT_ID, lat, long).result()
		# result = task.map_service_cached(EVENT_ID, lat, long)
		logger.log_response("Server return cached map service", EVENT_ID)
	else:
		result = process_pool.submit(task.map_service, EVENT_ID, lat, long).result()
		# result = task.map_service(EVENT_ID, lat, long)
		logger.log_response("Server return map service", EVENT_ID)

	return jsonify(result[0])


@app.route("/timetable")
def timetable_lookup():
	"""
	TimetableService – TimetableLookup
	-----------------------------------
	GET /timetable?time=<DATETIME>&nodes=<ID1>,<ID2>,...
	"""

	EVENT_ID = logger.log_request("User request timetable")

	time_str  = request.args.get("time")
	nodes_str = request.args.get("nodes")

	if (time_str == None):
		logger.log_response("Server return error 400 -> Missing time", EVENT_ID)
		return error(400, "Missing required parameter: time")
	
	try:
		dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
	except (ValueError, TypeError):
		logger.log_response("Server return error 400 -> Invalid time", EVENT_ID)
		return error(400, "Invalid required parameter: time")

	if dt.year < 2020 or dt.year > 2028:
		logger.log_response("Server return error 400 -> Unreasonable time", EVENT_ID)
		return error(400, "Invalid required parameter: time")
	
	
	if not nodes_str:
		logger.log_response("Server return error 400 -> Missing nodes", EVENT_ID)
		return error(400, "Missing required parameter: nodes")

	node_ids = [n.strip() for n in nodes_str.split(",") if n.strip()]

	if ( len(node_ids) >= 50 ):
		logger.log_response("Server return error 414 -> Too many nodes", EVENT_ID)
		return error(414, "Number of nodes exceed limit")

	logger.log_continue(f"Server get valid arg, time = {time_str}, nodes = {node_ids}", EVENT_ID)



	# TODO: validate datetime format and query database
	
	result = process_pool.submit(task.timetable_lookup, EVENT_ID, dt, node_ids).result()
	# result = task.timetable_lookup(EVENT_ID, dt, node_ids)

	if (result[1] == 200):
		logger.log_response("Server return timetable lookup", EVENT_ID)
		return jsonify(result[0])
	else:
		logger.log_response(f"Server return error {result[1]} -> Invalid time", EVENT_ID)
		return error(result[1], "Invalid required parameter: time")


@app.route("/services/<node_id>")
def service_lookup(node_id):
	"""
	TimetableService – ServiceLookup
	---------------------------------
	GET /services/<NODE ID>
	"""

	EVENT_ID = logger.log_request(f"User request service lookup for node_id = {node_id}")

	result = process_pool.submit(task.service_lookup, EVENT_ID, node_id).result()
	# result = task.service_lookup(EVENT_ID, node_id)

	if (result[1] != 200):
		logger.log_response(f"Server return error {result[1]} -> Invalid node id = {node_id}", EVENT_ID)
		return error(result[1], "Invalid node id")
	
	body = result[0]
	logger.log_response(f"Server return service lookup for node_id = {node_id}", EVENT_ID)
	return jsonify(body)


@app.route("/tracking/<node_id>")
def live_tracking(node_id):
	"""
	TimetableService – LiveTracking
	--------------------------------
	GET /tracking/<NODE ID>
	"""
	EVENT_ID = logger.log_request(f"User request live tracking for node_id = {node_id}")

	# TODO: fetch live vehicle positions for this node
	
	result = process_pool.submit(task.live_tracking, EVENT_ID, node_id).result()
	# result = task.live_tracking(EVENT_ID, node_id)

	if (result[1] != 200):
		logger.log_response(f"Server return error {result[1]} -> Invalid node id = {node_id}", EVENT_ID)
		return error(result[1], "Invalid node id") # error code is 404
	body = result[0]

	logger.log_response(f"Server return live tracking for node_id = {node_id}", EVENT_ID)
	return jsonify(body)


@app.route("/routes")
def route_service():
	"""
	RouteService
	------------
	GET /routes?source=<NODEID>&dest=<NODEID>&time=<DATETIME>
	"""

	EVENT_ID = logger.log_request("User request route service")

	source_id = request.args.get("source")
	dest_id   = request.args.get("dest")
	time_str  = request.args.get("time")

	if not source_id:
		logger.log_response("Server return error 404 -> Missing source", EVENT_ID)
		return error(404, "Missing required parameter: source")
	if not dest_id:
		logger.log_response("Server return error 404 -> Missing dest", EVENT_ID)
		return error(404, "Missing required parameter: dest")
	if not time_str:
		logger.log_response("Server return error 404 -> Missing time", EVENT_ID)
		return error(404, "Missing required parameter: time")

	try:
		dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ")
	except (ValueError, TypeError):
		logger.log_response("Server return error 404 -> Invalid time", EVENT_ID)
		return error(404, "Invalid required parameter: time")

	# TODO: run routing algorithm and query database
	result = process_pool.submit(task.route_service, EVENT_ID, source_id, dest_id, dt).result()
	# result = task.route_service(EVENT_ID, source_id, dest_id, dt)

	if (result[1] != 200):
		logger.log_response(f"Server return error {result[1]} -> Invalid source/dest", EVENT_ID)
		return error(result[1], "Invalid source or destination node id")
	body = result[0]

	logger.log_response(f"Server return route service for source = {source_id}, dest = {dest_id}", EVENT_ID)

	return jsonify(body)


@app.route("/weather")
def weather_service():
	"""
	WeatherService
	--------------
	GET /weather?lat=<number>&long=<number>
		Returns weather information for the given location.

	Errors (per API spec):
		400  lat or long parameter is missing
		400  lat or long value is not a valid number
		400  lat or long is out of valid geographic range
	"""

	EVENT_ID = logger.log_request(f"User request weather service = {request.query_string.decode()}")

	lat_str  = request.args.get("lat")
	long_str = request.args.get("long")

	if lat_str is None or long_str is None:
		logger.log_response("Server return error 400 -> Missing lat/long", EVENT_ID)
		return typed_error("weather", 400, "Missing required parameters: lat, long", lat=lat_str, long=long_str)

	try:
		lat  = float(lat_str)
		long = float(long_str)
	except ValueError:
		logger.log_response("Server return error 400 -> Invalid lat/long", EVENT_ID)
		return typed_error("weather", 400, f"Invalid lat/long values: lat={lat_str!r}, long={long_str!r}", lat=lat_str, long=long_str)

	if not (-90.0 <= lat <= 90.0):
		logger.log_response("Server return error 400 -> lat out of range", EVENT_ID)
		return typed_error("weather", 400, f"lat out of valid range [-90, 90]: {lat}", lat=lat, long=long)

	if not (-180.0 <= long <= 180.0):
		logger.log_response("Server return error 400 -> long out of range", EVENT_ID)
		return typed_error("weather", 400, f"long out of valid range [-180, 180]: {long}", lat=lat, long=long)

	logger.log_continue(f"Server parsed valid weather arg, lat={lat}, long={long}", EVENT_ID)

	result = process_pool.submit(task.weather_lookup, EVENT_ID, lat, long).result()

	if result[1] != 200:
		logger.log_response(f"Server return error {result[1]} -> weather unavailable", EVENT_ID)
		return jsonify(result[0]), result[1]

	logger.log_response("Server return weather service", EVENT_ID)
	return jsonify(result[0])

def main():
	port = 8080

	print(f"Fake front end UI at\t\thttp://localhost:{port}/\n")

	logger.log_init(port)

	# use_reloader=False prevents Flask from re-executing the parent script
	# (the reloader forks a subprocess which would re-run all_run.py from scratch)
	# host="0.0.0.0" makes Flask listen on all interfaces, not just loopback
	try:
		app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
		# app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False, threaded=True)
	finally:
		process_pool.shutdown(wait=True)
		# pass


if __name__ == "__main__":
	main()
