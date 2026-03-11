"""
Database Query Module for Transport Application

Provides utilities for querying bus routes, stops, services, and timetables.
"""

import sqlite3
from typing import Optional, List, Dict, Tuple
from os.path import dirname, abspath, join
import math


class TransportDatabase:
	"""Main database interface for transport queries."""
	
	def __init__(self, db_path: Optional[str] = None):
		"""
		Initialize database connection.
		
		Args:
			db_path: Path to database file. If None, uses relative path.
		"""
		if db_path is None:
			folder = dirname(abspath(__file__))
			db_path = join(folder, 'nptg_naptan.db')
		
		self.db_path = db_path
		self.conn = None
	
	def connect(self) -> None:
		"""Establish database connection."""
		self.conn = sqlite3.connect(self.db_path)
		self.conn.row_factory = sqlite3.Row  # Enable column access by name
		self.conn.execute("PRAGMA foreign_keys = ON;")
	
	def close(self) -> None:
		"""Close database connection."""
		if self.conn:
			self.conn.close()
			self.conn = None
	
	def __enter__(self):
		"""Context manager entry."""
		self.connect()
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		"""Context manager exit."""
		self.close()
	
	# ==================== STOP QUERIES ====================
	
	def get_stop_by_code(self, atco_code: str) -> Optional[Dict]:
		"""
		Get stop details by ATCO code.
		
		Args:
			atco_code: Stop's ATCO code
			
		Returns:
			Dictionary with stop details or None if not found
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		cursor.execute('''
			SELECT * FROM Stop_Point WHERE SP_atco_code = ?
		''', (atco_code,))
		
		row = cursor.fetchone()
		return dict(row) if row else None
	
	def get_stops_in_area(self, latitude: float, longitude: float, 
						 radius_km: float = 1.0) -> List[Dict]:
		"""Get stops within a bounding box of a coordinate."""
		if not self.conn:
			self.connect()
		
		# 1 degree of latitude is ~111.32 km everywhere
		delta_lat = radius_km / 111.32
		
		# 1 degree of longitude shrinks based on the cosine of the latitude
		# Prevent division by zero at the exact poles
		lat_radians = math.radians(latitude)
		delta_lon = radius_km / (111.32 * math.cos(lat_radians)) if math.cos(lat_radians) != 0 else 0
		
		cursor = self.conn.cursor()
		cursor.execute('''
			SELECT SP_atco_code, SP_name, SP_street, SP_latitude, SP_longitude
			FROM Stop_Point 
			WHERE SP_latitude BETWEEN ? AND ?
			  AND SP_longitude BETWEEN ? AND ?
			ORDER BY SP_name
		''', (latitude - delta_lat, latitude + delta_lat, 
			  longitude - delta_lon, longitude + delta_lon))
		
		return [dict(row) for row in cursor.fetchall()]
	
	# ==================== ROUTE & JOURNEY QUERIES ====================
	
	def get_route_stops_batch(self, atco_code: str) -> List[Dict]:
		"""
		Get all routes passing through a specific stop.
		
		Args:
			atco_code: Stop's ATCO code
			
		Returns:
			List of journey patterns passing through the stop
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		cursor.execute('''
			SELECT DISTINCT
				jp.JP_UID,
				jp.JP_id,
				jp.JP_dest_display,
				jp.JP_direction,
				s.SER_origin,
				s.SER_destination,
				o.OPE_short_name as operator_name,
				l.LIN_name as line_name
			FROM Journey_Pattern_Link jpl
			INNER JOIN Journey_Pattern_Section jps ON jpl.JPS_UID = jps.JPS_UID
			INNER JOIN Journey_Pattern jp ON jps.JP_UID = jp.JP_UID
			INNER JOIN Service s ON jp.SER_UID = s.SER_UID
			INNER JOIN Operator o ON jp.OPE_UID = o.OPE_UID
			INNER JOIN Line l ON s.SER_UID = l.SER_UID
			WHERE jpl.JPL_from_point_atco_code = ? OR jpl.JPL_to_point_atco_code = ?
			ORDER BY jp.JP_id
		''', (atco_code, atco_code))
		
		return [dict(row) for row in cursor.fetchall()]
	
	def get_routes_between_stops(self, start_atco: str, end_atco: str) -> List[Dict]:
		"""
		Get direct routes (journey patterns) between two stops.
		
		Args:
			start_atco: Starting stop ATCO code
			end_atco: Ending stop ATCO code
			
		Returns:
			List of journey patterns connecting the two stops
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		cursor.execute('''
			SELECT DISTINCT
				jp.JP_UID,
				jp.JP_id,
				jp.JP_dest_display,
				jp.JP_direction,
				s.SER_origin,
				s.SER_destination,
				o.OPE_short_name as operator_name,
				l.LIN_name as line_name
			FROM Journey_Pattern jp
			INNER JOIN Service s ON jp.SER_UID = s.SER_UID
			INNER JOIN Operator o ON jp.OPE_UID = o.OPE_UID
			INNER JOIN Line l ON s.SER_UID = l.SER_UID
			WHERE jp.JP_UID IN (
				SELECT jps2.JP_UID
				FROM Journey_Pattern_Link jpl2
				INNER JOIN Journey_Pattern_Section jps2 ON jpl2.JPS_UID = jps2.JPS_UID
				WHERE jpl2.JPL_from_point_atco_code = ?
			)
			AND jp.JP_UID IN (
				SELECT jps3.JP_UID
				FROM Journey_Pattern_Link jpl3
				INNER JOIN Journey_Pattern_Section jps3 ON jpl3.JPS_UID = jps3.JPS_UID
				WHERE jpl3.JPL_to_point_atco_code = ?
			)
			ORDER BY jp.JP_id
		''', (start_atco, end_atco))
		
		return [dict(row) for row in cursor.fetchall()]
	
	def get_route_stops(self, jp_uid: int) -> List[Dict]:
		"""
		Get all stops on a specific journey pattern route, in order.
		
		Args:
			jp_uid: Journey Pattern UID
			
		Returns:
			List of stops in order of traversal
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		cursor.execute('''
			SELECT 
				jpl.JPL_from_point_atco_code as atco_code,
				sp.SP_name,
				sp.SP_street,
				jpl.from_sequence_num as sequence,
				jpl.JPL_run_time as travel_time_from_prev,
				sp.SP_latitude,
				sp.SP_longitude
			FROM Journey_Pattern_Link jpl
			INNER JOIN Journey_Pattern_Section jps ON jpl.JPS_UID = jps.JPS_UID
			INNER JOIN Stop_Point sp ON jpl.JPL_from_point_atco_code = sp.SP_atco_code
			WHERE jps.JP_UID = ?
			ORDER BY jpl.from_sequence_num
		''', (jp_uid,))
		
		return [dict(row) for row in cursor.fetchall()]
	
	def get_route_polyline(self, jp_uid: int) -> List[Dict]:
		"""
		Get the full polyline for a journey pattern, using Route_Location
		waypoints between stops.  Falls back to stop coordinates when no
		waypoints exist for a link.

		Args:
			jp_uid: Journey Pattern UID

		Returns:
			Ordered list of {"lat": float, "long": float}.
			Empty list if the JP does not exist or has no links.
		"""
		if not self.conn:
			self.connect()

		cursor = self.conn.cursor()

		# 1. Ordered links with from/to stop coordinates
		cursor.execute('''
			SELECT
				jpl.from_sequence_num,
				jpl.RLIN_UID,
				sp_from.SP_latitude  AS from_lat,
				sp_from.SP_longitude AS from_long,
				sp_to.SP_latitude    AS to_lat,
				sp_to.SP_longitude   AS to_long
			FROM Journey_Pattern_Section jps
			JOIN Journey_Pattern_Link jpl ON jpl.JPS_UID = jps.JPS_UID
			JOIN Stop_Point sp_from ON jpl.JPL_from_point_atco_code = sp_from.SP_atco_code
			JOIN Stop_Point sp_to   ON jpl.JPL_to_point_atco_code   = sp_to.SP_atco_code
			WHERE jps.JP_UID = ?
			ORDER BY jpl.from_sequence_num
		''', (jp_uid,))

		links = cursor.fetchall()
		if not links:
			return []

		# 2. All waypoints for the collected RLIN_UIDs
		rlin_uids = list({link['RLIN_UID'] for link in links})
		placeholders = ','.join('?' * len(rlin_uids))
		cursor.execute(f'''
			SELECT RLIN_UID, RLOC_latitude, RLOC_longitude, RLOC_global_seq
			FROM Route_Location
			WHERE RLIN_UID IN ({placeholders})
			ORDER BY RLIN_UID, RLOC_global_seq
		''', rlin_uids)

		waypoints_by_rlin: Dict[int, List[Dict]] = {}
		for row in cursor.fetchall():
			rlin = row['RLIN_UID']
			if rlin not in waypoints_by_rlin:
				waypoints_by_rlin[rlin] = []
			waypoints_by_rlin[rlin].append({
				"lat":  row['RLOC_latitude'],
				"long": row['RLOC_longitude'],
			})

		# 3. Build ordered polyline: from_stop → waypoints → (last to_stop)
		polyline: List[Dict] = []
		for i, link in enumerate(links):
			polyline.append({"lat": link['from_lat'], "long": link['from_long']})

			rlin_uid = link['RLIN_UID']
			if rlin_uid in waypoints_by_rlin:
				polyline.extend(waypoints_by_rlin[rlin_uid])

			# Append the to-stop of the last link to close the path
			if i == len(links) - 1:
				polyline.append({"lat": link['to_lat'], "long": link['to_long']})

		return polyline

	def get_route_stops_batch(self, jp_uids: List[int]) -> Dict[int, List[Dict]]:
		"""Fetch stops for multiple journey patterns in a single query."""
		if not jp_uids:
			return {}
		if not self.conn:
			self.connect()
		placeholders = ','.join('?' * len(jp_uids))
		cursor = self.conn.cursor()
		cursor.execute(f'''
			SELECT
				jps.JP_UID,
				jpl.JPL_from_point_atco_code AS atco_code,
				sp.SP_name,
				sp.SP_street,
				jpl.from_sequence_num AS sequence,
				jpl.JPL_run_time AS travel_time_from_prev,
				sp.SP_latitude,
				sp.SP_longitude
			FROM Journey_Pattern_Section jps
			INNER JOIN Journey_Pattern_Link jpl ON jpl.JPS_UID = jps.JPS_UID
			INNER JOIN Stop_Point sp ON jpl.JPL_from_point_atco_code = sp.SP_atco_code
			WHERE jps.JP_UID IN ({placeholders})
			ORDER BY jps.JP_UID, jpl.from_sequence_num
		''', jp_uids)
		result: Dict[int, List[Dict]] = {jp: [] for jp in jp_uids}
		for row in cursor.fetchall():
			jp = row['JP_UID']
			d = dict(row)
			del d['JP_UID']
			result[jp].append(d)
		return result

	def get_route_polylines_batch(self, jp_uids: List[int]) -> Dict[int, List[Dict]]:
		"""Fetch full polylines for multiple journey patterns in two queries."""
		if not jp_uids:
			return {}
		if not self.conn:
			self.connect()
		placeholders = ','.join('?' * len(jp_uids))
		cursor = self.conn.cursor()
		# Query 1: all links for all JPs
		cursor.execute(f'''
			SELECT
				jps.JP_UID,
				jpl.from_sequence_num,
				jpl.RLIN_UID,
				sp_from.SP_latitude  AS from_lat,
				sp_from.SP_longitude AS from_long,
				sp_to.SP_latitude    AS to_lat,
				sp_to.SP_longitude   AS to_long
			FROM Journey_Pattern_Section jps
			JOIN Journey_Pattern_Link jpl ON jpl.JPS_UID = jps.JPS_UID
			JOIN Stop_Point sp_from ON jpl.JPL_from_point_atco_code = sp_from.SP_atco_code
			JOIN Stop_Point sp_to   ON jpl.JPL_to_point_atco_code   = sp_to.SP_atco_code
			WHERE jps.JP_UID IN ({placeholders})
			ORDER BY jps.JP_UID, jpl.from_sequence_num
		''', jp_uids)
		all_links = cursor.fetchall()
		# Query 2: all waypoints for the collected RLIN_UIDs
		rlin_uids = list({lnk['RLIN_UID'] for lnk in all_links})
		waypoints_by_rlin: Dict[int, List[Dict]] = {}
		if rlin_uids:
			rlin_ph = ','.join('?' * len(rlin_uids))
			cursor.execute(f'''
				SELECT RLIN_UID, RLOC_latitude, RLOC_longitude, RLOC_global_seq
				FROM Route_Location
				WHERE RLIN_UID IN ({rlin_ph})
				ORDER BY RLIN_UID, RLOC_global_seq
			''', rlin_uids)
			for row in cursor.fetchall():
				rlin = row['RLIN_UID']
				if rlin not in waypoints_by_rlin:
					waypoints_by_rlin[rlin] = []
				waypoints_by_rlin[rlin].append({"lat": row['RLOC_latitude'], "long": row['RLOC_longitude']})
		# Group links by JP_UID
		links_by_jp: Dict[int, list] = {}
		for lnk in all_links:
			jp = lnk['JP_UID']
			if jp not in links_by_jp:
				links_by_jp[jp] = []
			links_by_jp[jp].append(lnk)
		# Build polylines
		result: Dict[int, List[Dict]] = {}
		for jp_uid in jp_uids:
			links = links_by_jp.get(jp_uid)
			if not links:
				result[jp_uid] = []
				continue
			polyline: List[Dict] = []
			for i, lnk in enumerate(links):
				polyline.append({"lat": lnk['from_lat'], "long": lnk['from_long']})
				rlin_uid = lnk['RLIN_UID']
				if rlin_uid in waypoints_by_rlin:
					polyline.extend(waypoints_by_rlin[rlin_uid])
				if i == len(links) - 1:
					polyline.append({"lat": lnk['to_lat'], "long": lnk['to_long']})
			result[jp_uid] = polyline
		return result

	# ==================== SERVICE & LINE QUERIES ==
	
	def get_services_by_operator(self, operator_code: str) -> List[Dict]:
		"""
		Get all services operated by a specific operator.
		
		Args:
			operator_code: Operator code
			
		Returns:
			List of services
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		cursor.execute('''
			SELECT 
				s.SER_UID,
				s.SER_service_code,
				s.SER_origin,
				s.SER_destination,
				s.SER_start_date,
				s.SER_end_date,
				o.OPE_short_name as operator_name
			FROM Service s
			INNER JOIN Operator o ON s.OPE_UID = o.OPE_UID
			WHERE o.OPE_code = ?
			ORDER BY s.SER_service_code
		''', (operator_code,))
		
		return [dict(row) for row in cursor.fetchall()]
	
	def get_lines_for_service(self, service_code: str) -> List[Dict]:
		"""
		Get all lines for a specific service.
		
		Args:
			service_code: Service code
			
		Returns:
			List of lines with details
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		cursor.execute('''
			SELECT 
				l.LIN_UID,
				l.LIN_id,
				l.LIN_name,
				l.LIN_out_bound_orig,
				l.LIN_out_bound_dest,
				l.LIN_in_bound_orig,
				l.LIN_in_bound_dest
			FROM Line l
			INNER JOIN Service s ON l.SER_UID = s.SER_UID
			WHERE s.SER_service_code = ?
			ORDER BY l.LIN_name
		''', (service_code,))
		
		return [dict(row) for row in cursor.fetchall()]
	
	def get_operators(self) -> List[Dict]:
		"""
		Get all operators in the database.
		
		Returns:
			List of operators
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		cursor.execute('''
			SELECT 
				OPE_UID,
				OPE_code,
				OPE_short_name,
				OPE_trading_name,
				OPE_licence_name
			FROM Operator
			ORDER BY OPE_short_name
		''')
		
		return [dict(row) for row in cursor.fetchall()]
	
	# ==================== TIMETABLE QUERIES ====================
	
	def get_vehicle_journeys(self, jp_uid: int, day_of_week: Optional[int] = None) -> List[Dict]:
		"""
		Get vehicle journeys for a journey pattern.
		
		Args:
			jp_uid: Journey Pattern UID
			day_of_week: Day of week (0=Monday, 6=Sunday). If None, return all.
			
		Returns:
			List of vehicle journeys with departure times
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		
		if day_of_week is not None:
			# Filter by day of week using bitwise operation
			# DOW_days is a bitmask where bit positions represent days
			cursor.execute('''
				SELECT 
					vj.VJ_UID,
					vj.VJ_code,
					vj.departure_time,
					o.OPE_short_name as operator_name
				FROM Vehicle_Journey vj
				INNER JOIN Journey_Pattern jp ON vj.JP_UID = jp.JP_UID
				INNER JOIN Operator o ON vj.OPE_UID = o.OPE_UID
				INNER JOIN Days_Of_Week dow ON vj.VJ_UID = dow.VJ_UID
				WHERE jp.JP_UID = ?
				  AND (dow.DOW_days & ?) != 0
				ORDER BY vj.departure_time
			''', (jp_uid, 1 << day_of_week))
		else:
			cursor.execute('''
				SELECT 
					vj.VJ_UID,
					vj.VJ_code,
					vj.departure_time,
					o.OPE_short_name as operator_name
				FROM Vehicle_Journey vj
				INNER JOIN Journey_Pattern jp ON vj.JP_UID = jp.JP_UID
				INNER JOIN Operator o ON vj.OPE_UID = o.OPE_UID
				WHERE jp.JP_UID = ?
				ORDER BY vj.departure_time
			''', (jp_uid,))
		
		return [dict(row) for row in cursor.fetchall()]

	def get_upcoming_arrivals_at_stop(
		self,
		atco_code: str,
		day_of_week: int,
		query_time_hms: str,
		limit: int = 50,
	) -> List[Dict]:
		"""
		Get upcoming arrivals at a stop using a single query.

		This avoids the expensive N+1 pattern of:
		stop -> routes -> vehicle journeys per route.

		Args:
			atco_code: Stop ATCO code
			day_of_week: 0=Monday, 6=Sunday
			query_time_hms: Time string in HH:MM:SS (UTC)
			limit: Maximum number of arrivals to return

		Returns:
			List[Dict] with keys: departure_time, line_name, operator_name
		"""
		if not self.conn:
			self.connect()

		# Guardrails for unexpected values
		if not (0 <= day_of_week <= 6):
			return []
		if not isinstance(query_time_hms, str) or len(query_time_hms.strip()) == 0:
			return []
		if limit <= 0:
			return []

		current_day_bit = 1 << day_of_week
		next_day_bit = 1 << ((day_of_week + 1) % 7)

		cursor = self.conn.cursor()
		cursor.execute(
			'''
			WITH stop_jps AS (
				SELECT DISTINCT jps.JP_UID
				FROM Journey_Pattern_Link jpl
				INNER JOIN Journey_Pattern_Section jps ON jpl.JPS_UID = jps.JPS_UID
				WHERE jpl.JPL_from_point_atco_code = ? OR jpl.JPL_to_point_atco_code = ?
			)
			SELECT
				vj.departure_time AS departure_time,
				MIN(l.LIN_name) AS line_name,
				MIN(o.OPE_short_name) AS operator_name
			FROM stop_jps sj
			INNER JOIN Journey_Pattern jp ON jp.JP_UID = sj.JP_UID
			INNER JOIN Service s ON jp.SER_UID = s.SER_UID
			INNER JOIN Line l ON s.SER_UID = l.SER_UID
			INNER JOIN Vehicle_Journey vj ON vj.JP_UID = jp.JP_UID
			INNER JOIN Days_Of_Week dow ON dow.VJ_UID = vj.VJ_UID
			INNER JOIN Operator o ON vj.OPE_UID = o.OPE_UID
			WHERE
				(
					(vj.departure_time >= ? AND (dow.DOW_days & ?) != 0)
					OR
					(vj.departure_time < ? AND (dow.DOW_days & ?) != 0)
				)
			GROUP BY vj.VJ_UID, vj.departure_time
			ORDER BY
				CASE WHEN vj.departure_time >= ? THEN 0 ELSE 1 END,
				vj.departure_time
			LIMIT ?
			''',
			(
				atco_code,
				atco_code,
				query_time_hms,
				current_day_bit,
				query_time_hms,
				next_day_bit,
				query_time_hms,
				limit,
			)
		)

		return [dict(row) for row in cursor.fetchall()]

	def get_active_jp_for_stop(
		self,
		atco_code: str,
		day_of_week: int,
	) -> List[Dict]:
		"""
		Get distinct (line_name, JP_UID) pairs for a stop, filtered by
		day-of-week via Vehicle_Journey → Days_Of_Week.

		Only journey patterns that have at least one vehicle journey
		operating on the given day are returned.

		Args:
			atco_code: Stop ATCO code
			day_of_week: 0=Monday … 6=Sunday

		Returns:
			List[Dict] with keys: line_name, JP_UID
		"""
		if not self.conn:
			self.connect()

		if not (0 <= day_of_week <= 6):
			return []
		if not isinstance(atco_code, str) or not atco_code.strip():
			return []

		day_bit = 1 << day_of_week

		cursor = self.conn.cursor()
		cursor.execute(
			'''
			WITH stop_jps AS (
				-- Step 1: distinct JP_UIDs whose links touch this stop
				SELECT DISTINCT jps.JP_UID
				FROM Journey_Pattern_Link jpl
				INNER JOIN Journey_Pattern_Section jps ON jpl.JPS_UID = jps.JPS_UID
				WHERE jpl.JPL_from_point_atco_code = ? OR jpl.JPL_to_point_atco_code = ?
			),
			active_jps AS (
				-- Step 2: keep only JPs that have a VJ operating on this DOW
				SELECT DISTINCT jp.JP_UID, l.LIN_name AS line_name
				FROM stop_jps sj
				INNER JOIN Journey_Pattern jp ON jp.JP_UID = sj.JP_UID
				INNER JOIN Service s ON jp.SER_UID = s.SER_UID
				INNER JOIN Line l ON l.SER_UID = s.SER_UID
				WHERE EXISTS (
					SELECT 1
					FROM Vehicle_Journey vj
					INNER JOIN Days_Of_Week dow ON dow.VJ_UID = vj.VJ_UID
					WHERE vj.JP_UID = jp.JP_UID
					  AND (dow.DOW_days & ?) != 0
				)
			)
			-- Step 3: one row per line — cheapest distinct JP (avoids Route_Location join)
			SELECT line_name, MIN(JP_UID) AS JP_UID
			FROM active_jps
			GROUP BY line_name
			ORDER BY line_name
			''',
			(atco_code, atco_code, day_bit),
		)

		return [dict(row) for row in cursor.fetchall()]

	def get_timetable_for_route(self, jp_uid: int, day_of_week: Optional[int] = None) -> Dict:
		"""
		Get complete timetable for a route (stops + departure times).
		
		Args:
			jp_uid: Journey Pattern UID
			day_of_week: Day of week filter (optional)
			
		Returns:
			Dictionary with route info and stops with timings
		"""
		if not self.conn:
			self.connect()
		
		# Get route stops
		stops = self.get_route_stops(jp_uid)
		
		# Get vehicle journeys
		journeys = self.get_vehicle_journeys(jp_uid, day_of_week)
		
		# Get service info
		cursor = self.conn.cursor()
		cursor.execute('''
			SELECT 
				jp.JP_id,
				jp.JP_dest_display,
				s.SER_origin,
				s.SER_destination,
				o.OPE_short_name as operator_name
			FROM Journey_Pattern jp
			INNER JOIN Service s ON jp.SER_UID = s.SER_UID
			INNER JOIN Operator o ON jp.OPE_UID = o.OPE_UID
			WHERE jp.JP_UID = ?
		''', (jp_uid,))
		
		route_info = dict(cursor.fetchone())
		
		return {
			'route_info': route_info,
			'stops': stops,
			'journeys': journeys
		}
	
	# ==================== LOCATION & GEOGRAPHIC QUERIES ====================
	
	def get_localities(self, authority_code: Optional[str] = None) -> List[Dict]:
		"""
		Get localities, optionally filtered by authority.
		
		Args:
			authority_code: Authority code to filter by (optional)
			
		Returns:
			List of localities
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		
		if authority_code:
			cursor.execute('''
				SELECT 
					LOC_nptg_code,
					LOC_name,
					LOC_type,
					LOC_latitude,
					LOC_longitude
				FROM Locality
				WHERE Aut_admin_area_code = ?
				ORDER BY LOC_name
			''', (authority_code,))
		else:
			cursor.execute('''
				SELECT 
					LOC_nptg_code,
					LOC_name,
					LOC_type,
					LOC_latitude,
					LOC_longitude
				FROM Locality
				ORDER BY LOC_name
			''')
		
		return [dict(row) for row in cursor.fetchall()]
	
	def get_authorities(self, region_code: Optional[str] = None) -> List[Dict]:
		"""
		Get authorities, optionally filtered by region.
		
		Args:
			region_code: Region code to filter by (optional)
			
		Returns:
			List of authorities
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		
		if region_code:
			cursor.execute('''
				SELECT 
					Aut_admin_area_code,
					Aut_name,
					Aut_short_name,
					Aut_atco_area_code
				FROM Authority
				WHERE Reg_code = ?
				ORDER BY Aut_name
			''', (region_code,))
		else:
			cursor.execute('''
				SELECT 
					Aut_admin_area_code,
					Aut_name,
					Aut_short_name,
					Aut_atco_area_code
				FROM Authority
				ORDER BY Aut_name
			''')
		
		return [dict(row) for row in cursor.fetchall()]
	
	def get_regions(self) -> List[Dict]:
		"""
		Get all regions.
		
		Returns:
			List of regions
		"""
		if not self.conn:
			self.connect()
		
		cursor = self.conn.cursor()
		cursor.execute('SELECT * FROM Region ORDER BY Reg_name')
		
		return [dict(row) for row in cursor.fetchall()]


	def get_stops_batch(self, atco_codes: List[str]) -> Dict[str, Dict]:
		"""Fetch multiple stops by ATCO code in a single query."""
		if not atco_codes:
			return {}
		if not self.conn:
			self.connect()
			
		placeholders = ','.join('?' * len(atco_codes))
		cursor = self.conn.cursor()
		cursor.execute(f'''
			SELECT * FROM Stop_Point 
			WHERE SP_atco_code IN ({placeholders})
		''', atco_codes)
		
		return {row['SP_atco_code']: dict(row) for row in cursor.fetchall()}

	def get_upcoming_arrivals_batch(
		self, 
		atco_codes: List[str], 
		day_of_week: int, 
		query_time_hms: str
	) -> Dict[str, List[Dict]]:
		"""
		Get upcoming arrivals for multiple stops using a single query.
		Returns a dictionary mapping stop codes to a list of their arrivals.
		"""
		if not atco_codes or not self.conn:
			if not self.conn:
				self.connect()
			return {}

		current_day_bit = 1 << day_of_week
		next_day_bit = 1 << ((day_of_week + 1) % 7)
		placeholders = ','.join('?' * len(atco_codes))

		cursor = self.conn.cursor()
		
		# We use a UNION here to safely handle cases where a single route 
		# link connects two stops that are BOTH in the requested list.
		query = f'''
			WITH stop_jps AS (
				SELECT jpl.JPL_from_point_atco_code AS stop_code, jps.JP_UID
				FROM Journey_Pattern_Link jpl
				INNER JOIN Journey_Pattern_Section jps ON jpl.JPS_UID = jps.JPS_UID
				WHERE jpl.JPL_from_point_atco_code IN ({placeholders})
				
				UNION
				
				SELECT jpl.JPL_to_point_atco_code AS stop_code, jps.JP_UID
				FROM Journey_Pattern_Link jpl
				INNER JOIN Journey_Pattern_Section jps ON jpl.JPS_UID = jps.JPS_UID
				WHERE jpl.JPL_to_point_atco_code IN ({placeholders})
			)
			SELECT
				sj.stop_code,
				vj.departure_time AS departure_time,
				MIN(l.LIN_name) AS line_name,
				MIN(o.OPE_short_name) AS operator_name,
				CASE WHEN vj.departure_time >= ? THEN 0 ELSE 1 END AS day_offset
			FROM stop_jps sj
			INNER JOIN Journey_Pattern jp ON jp.JP_UID = sj.JP_UID
			INNER JOIN Service s ON jp.SER_UID = s.SER_UID
			INNER JOIN Line l ON s.SER_UID = l.SER_UID
			INNER JOIN Vehicle_Journey vj ON vj.JP_UID = jp.JP_UID
			INNER JOIN Days_Of_Week dow ON dow.VJ_UID = vj.VJ_UID
			INNER JOIN Operator o ON vj.OPE_UID = o.OPE_UID
			WHERE
				(
					(vj.departure_time >= ? AND (dow.DOW_days & ?) != 0)
					OR
					(vj.departure_time < ? AND (dow.DOW_days & ?) != 0)
				)
			GROUP BY sj.stop_code, vj.VJ_UID, vj.departure_time
			ORDER BY day_offset, vj.departure_time
		'''
		
		# Parameters: 2 sets of ATCO codes for the UNION, then the time parameters
		params = atco_codes + atco_codes + [
			query_time_hms, 
			query_time_hms, current_day_bit, 
			query_time_hms, next_day_bit
		]
		
		cursor.execute(query, params)
		
		# Initialize dictionary with empty lists for all requested codes
		results: Dict[str, List[Dict]] = {code: [] for code in atco_codes}
		for row in cursor.fetchall():
			results[row['stop_code']].append(dict(row))
			
		return results

# ==================== CONVENIENCE FUNCTIONS ====================

def find_routes(start_stop: str, end_stop: str, max_results: int = 10) -> List[Dict]:
	"""
	Find routes between two stops.
	
	Args:
		start_stop: Starting stop ATCO code or name pattern
		end_stop: Ending stop ATCO code or name pattern
		max_results: Maximum number of results
		
	Returns:
		List of available routes
	"""
	with TransportDatabase() as db:
		routes = db.get_routes_between_stops(start_stop, end_stop)
		return routes[:max_results]


def get_stops_by_name(name_pattern: str, limit: int = 20) -> List[Dict]:
	"""Search for stops by name."""
	with TransportDatabase() as db:
		return db.search_stops_by_name(name_pattern, limit)


def get_nearby_stops(latitude: float, longitude: float, 
					radius_km: float = 1.0) -> List[Dict]:
	"""Get stops near a coordinate."""
	with TransportDatabase() as db:
		return db.get_stops_in_area(latitude, longitude, radius_km)


if __name__ == "__main__":
	# Example usage
	with TransportDatabase() as db:
		print("=== Available Operators ===")
		operators = db.get_operators()
		for op in operators[:5]:
			print(f"  {op['OPE_short_name']} ({op['OPE_code']})")
		
		print("\n=== Regions ===")
		regions = db.get_regions()
		for region in regions[:5]:
			print(f"  {region['Reg_name']} ({region['Reg_code']})")
