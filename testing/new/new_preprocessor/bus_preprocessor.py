from dataclasses import dataclass, asdict, fields

@dataclass
class Service:

	uid				: int

	service_code	: str
	start_date		: str
	end_date		: str | None

	operator_ref	: str
	operator_uid	: int

	origin			: str
	destination		: str

	def to_flat_dict(self) -> dict:
		flat = {
			"uid": self.uid,
			"service_code": self.service_code,
			"start_date": self.start_date,
			"end_date": self.end_date or "",
			"operator_ref": self.operator_ref,
			"operator_uid": self.operator_uid,
			"origin": self.origin,
			"destination": self.destination,
		}
		return flat


@dataclass
class Line:
	@dataclass
	class S_Bound:
		origin		: str | None
		destination	: str | None
		description	: str | None
		# could be none, not all have
	uid			: int
	line_id			: str
	line_name		: str
	out_bound		: S_Bound
	in_bound		: S_Bound # sometime, in_bound == reversed out_bound

	parent_service_code	: str
	parent_service_uid	: int

	def to_flat_dict(self) -> dict:
		flat = {
			"uid": self.uid,
			"line_id": self.line_id,
			"line_name": self.line_name,
			"parent_service_code": self.parent_service_code,
			"parent_service_uid": self.parent_service_uid,
		}
		flat.update({f"out_bound_{k}": (v if v is not None else "") for k, v in asdict(self.out_bound).items()})
		flat.update({f"in_bound_{k}": (v if v is not None else "") for k, v in asdict(self.in_bound).items()})
		return flat

@dataclass
class Route:
	uid				: int # new added, index or id of Route
	route_id		: str # primary identifier (from XML @id)

	private_code	: str
	description		: str
	
	route_section_start_index	: int
	route_section_end_index		: int

@dataclass
class Route_Location: # only live in Route_Link to form a ployline
	uid				: int	# new added, index or id of Route_Location
	location_id		: str	# name of RS_Location in Route_Link, duplicated
	longitude		: float
	latitude		: float

	global_seq		: int



@dataclass
class Route_Link:
	# Storage/indexing
	uid					: int = 0  # new added, index or id of Route_Link
	link_id				: str = ""  # like local name, duplicated (from XML @id)
	
	# Route geometry
	from_bus_stop_point_uid		: int = -1
	from_stop_point_ref		: str = ""

	to_bus_stop_point_uid		: int = -1
	to_stop_point_ref		: str = ""

	distance				: int = 0
	start_route_location_index	: int = 0
	end_route_location_index	: int = 0

	global_seq				: int = 0
	
	# Note: all Route_Location will be stored into csv, even duplicated
	# start_RL_index = the start index of Route_Location for this Route_Link
	# end_RL_index = the end index of Route_Location for this Route_Link


@dataclass
class Route_Section:
	# Storage/indexing
	uid					: int = 0  # new added, index or id of Route_Section
	section_id			: str = ""  # primary identifier (from XML @id)
	start_route_link_index	: int = 0
	end_route_link_index	: int = 0
	
	# Note: all Route_Link will be stored into csv, even duplicated
	# start_RL_index = the start index of Route_Link for this Route_Section
	# end_RL_index = the end index of Route_Link for this Route_Section


@dataclass
class Bus_Stop_Point: # it do called StopPoint, but same with naptan StopPoint
	# Storage/indexing
	uid				: int # for convenient access
	stop_point_ref	: str
	common_name		: str


@dataclass
class Journey_Pattern_Link:
	@dataclass
	class JP_Point:
		point_id		: str # like name
		sequence_num	: int
		activity		: str 
		destination_display	: str | None = None  # Fixed: was None, should be optional str
		timing_status	: str = ""
		fare_stage_num	: str | None = None
		
		# activity:
		# 	pickUp: The bus starts here; you can get on, but nobody is getting off yet.
		# 	pickUpAndSetDown: A standard stop where people can both board and alight.
		# 	setDown: The final stop; everyone must get off.
		# timing_status:
		# 	principalTimingPoint: These are the "Checkpoints." The bus must not leave these stops early.
		# 	otherPoint: These are regular stops. The bus stops if someone is there.


	# XML attributes
	id						: str  # XML @id attribute

	# Storage/indexing
	uid						: int = 0
	from_JP_point			: JP_Point | None = None
	to_JP_point				: JP_Point | None = None
	route_link_ref			: str = ""
	route_link_uid			: int = 0
	run_time				: str = "PT0M0S"  # ISO 8601 duration format

	global_seq				: int = 0

	def to_flat_dict(self) -> dict:
		flat = {
			"id": self.id,
			"uid": self.uid,
			"global_seq": self.global_seq,
			"route_link_ref": self.route_link_ref,
			"route_link_uid": self.route_link_uid,
			"run_time": self.run_time,
		}
		
		empty_point = {
			"point_id": "",
			"sequence_num": "",
			"activity": "",
			"destination_display": "",
			"timing_status": "",
			"fare_stage_num": "",
		}
		
		if self.from_JP_point:
			flat.update({f"from_point_{k}": (v if v is not None else "") for k, v in asdict(self.from_JP_point).items()})
		else:
			flat.update({f"from_point_{k}": v for k, v in empty_point.items()})
		
		if self.to_JP_point:
			flat.update({f"to_point_{k}": (v if v is not None else "") for k, v in asdict(self.to_JP_point).items()})
		else:
			flat.update({f"to_point_{k}": v for k, v in empty_point.items()})
		
		return flat
	
@dataclass
class Journey_Pattern:
	journey_pattern_id	: str

	uid				: int
	
	destination_display	: str

	operatior_ref	: str
	operator_uid		: int

	direction		: str
	description		: str | None

	route_ref		: str
	route_uid		: int

	JP_section_start_ref	: str
	JP_section_start_uid	: int

	JP_section_end_ref	: str
	JP_section_end_uid	: int

	parent_service_code	: str
	parent_service_uid	: int


@dataclass
class Journey_Pattern_Section:
	uid				: int
	section_id		: str
	start_JP_link_index	: int
	end_JP_link_index	: int

@dataclass
class Operator:
	"""Bus operator/company information"""
	# XML attributes
	id		: str
	uid		: int

	# Elements
	national_operator_code	: str
	operator_code			: str
	operator_short_name	: str
	licence_number				: str  # Essential - operator licence number
	
	operator_name_on_licence	: str | None = None  # Optional, 98.94% frequency
	trading_name				: str | None = None

@dataclass
class Garage:
	uid			: int
	garage_code	: str
	garage_name	: str
	longitude	: float
	latitude	: float

	parent_operator_uid	: int

@dataclass
class Serviced_Organisation:
	uid				: int
	organisation_code	: str
	name				: str


@dataclass
class Serviced_Organisation_Date_Range:
	uid				: int

	organisation_code	: str
	organisation_uid	: int

	start_date		: str
	end_date		: str
	description		: str


@dataclass
class Days_Of_Week:
	"""Represents which days of the week a service operates"""
	uid				: int
	parent_VJ_uid	: int

	monday		: bool = False
	tuesday		: bool = False
	wednesday	: bool = False
	thursday	: bool = False
	friday		: bool = False
	saturday	: bool = False
	sunday		: bool = False



@dataclass
class Special_Days_Operation:
	"""Operating on specific date ranges"""
	uid				: int
	parent_VJ_uid	: int

	do_operate		: bool  # True if service runs on these dates, False if service does NOT run on these dates
	start_date	: str = ""  # YYYY-MM-DD format
	end_date	: str = ""  # YYYY-MM-DD format



@dataclass
class Bank_Holiday_Operation:
	"""Holiday operating rules - which holidays service DOES or DOES NOT run"""
	uid					: int
	parent_VJ_uid		: int

	days_of_operation	: list[str] | None = None  # List of holiday names when service RUNS
	days_of_non_operation: list[str] | None = None  # List of holiday names when service DOESN'T RUN

	# Possible holiday values:
	# SpringBank, BoxingDay, GoodFriday, NewYearsDay, LateSummerBankHolidayNotScotland,
	# MayDay, EasterMonday, ChristmasDay, ChristmasEve, NewYearsEve, 
	# ChristmasDayHoliday, BoxingDayHoliday, NewYearsDayHoliday

	def to_flat_dict(self) -> dict:
		return {
			"uid": self.uid,
			"parent_VJ_uid": self.parent_VJ_uid,
			"days_of_operation": ",".join(self.days_of_operation) if self.days_of_operation else "",
			"days_of_non_operation": ",".join(self.days_of_non_operation) if self.days_of_non_operation else "",
		}


@dataclass
class Vehicle_Journey:
	uid						: int

	private_code			: str

	sequence_number			: int

	operator_ref			: str
	operator_uid			: int

	days_of_week_uid			: int

	special_days_operation_start_index	: int | None
	special_days_operation_end_index	: int | None

	bank_holiday_operation_uid	: int | None

	serviced_organisation_ref	: str
	serviced_organisation_uid	: int

	garage_ref				: str
	garage_uid				: int

	VJ_code				: str

	service_ref			: str
	service_uid			: int

	line_ref			: str
	line_uid			: int

	JP_ref				: str
	JP_uid				: int

	departure_time		: str  # ISO 8601 time format (HH:MM:SS)

	VJ_link_start_index	: int
	VJ_link_end_index	: int



@dataclass
class Vehicle_Journey_Link:
	uid				: int

	parent_VJ_code	: str
	parent_VJ_uid	: int

	link_id			: str # XML @id attribute, like name, duplicated

	JP_link_ref		: str
	JP_link_uid		: int

	runtime			: str # ISO 8601 duration format
	from_activity	: str
	to_activity		: str

	global_seq		: int

# ==================== PARSER IMPLEMENTATION ====================

import xml.etree.ElementTree as ET
import csv
import os
from pathlib import Path

def csv_save(file_name: str, item_list: list, verbose: bool = True) -> None:
	if not item_list:
		return

	header = [field.name for field in fields(item_list[0])]

	file_exists = os.path.exists(file_name)
	file_empty = not file_exists or os.path.getsize(file_name) == 0

	with open(file_name, "a", newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=header)
		if file_empty:
			writer.writeheader()
		for item in item_list:
			writer.writerow(asdict(item))

	if verbose:
		print(f"Saved {len(item_list)} records to {file_name}")

def csv_save_nested(file_name: str, item_list: list, verbose: bool = True) -> None:
	if not item_list:
		return

	sample_dict = item_list[0].to_flat_dict()
	header = list(sample_dict.keys())

	file_exists = os.path.exists(file_name)
	file_empty = not file_exists or os.path.getsize(file_name) == 0

	with open(file_name, "a", newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=header)
		if file_empty:
			writer.writeheader()
		for item in item_list:
			writer.writerow(item.to_flat_dict())

	if verbose:
		print(f"Saved {len(item_list)} records to {file_name}")

def csv_init(file_name: str) -> None:
	with open(file_name, "w", newline="", encoding="utf-8") as f:
		f.write("")

class XMLParser:
	def __init__(self, xml_file_path: str, output_dir: str = "EXPERIMENT_CSV", verbose: bool = True):
		self.xml_file = xml_file_path
		self.output_dir = output_dir
		self.verbose = verbose
		self.tree = ET.parse(xml_file_path)
		self.root = self.tree.getroot()
		self.ns = {'tc': 'http://www.transxchange.org.uk/'}
		
		# Create output directory if it doesn't exist
		os.makedirs(output_dir, exist_ok=True)
		
		# Collections for each object type
		self.operators: list[Operator] = []
		self.garages: list[Garage] = []
		self.serviced_organisations: list[Serviced_Organisation] = []
		self.serviced_organisation_date_ranges: list[Serviced_Organisation_Date_Range] = []
		self.services: list[Service] = []
		self.lines: list[Line] = []
		self.stop_points: list[Bus_Stop_Point] = []
		self.routes: list[Route] = []
		self.route_sections: list[Route_Section] = []
		self.route_links: list[Route_Link] = []
		self.route_locations: list[Route_Location] = []
		self.journey_pattern_links: list[Journey_Pattern_Link] = []
		self.journey_pattern_sections: list[Journey_Pattern_Section] = []
		self.journey_patterns: list[Journey_Pattern] = []
		self.days_of_weeks: list[Days_Of_Week] = []
		self.special_days_operations: list[Special_Days_Operation] = []
		self.bank_holiday_operations: list[Bank_Holiday_Operation] = []
		self.vehicle_journeys: list[Vehicle_Journey] = []
		self.vehicle_journey_links: list[Vehicle_Journey_Link] = []
		
		# Counters for UIDs
		self.operator_uid_counter = 0
		self.garage_uid_counter = 0
		self.serviced_organisation_uid_counter = 0
		self.serviced_organisation_date_range_uid_counter = 0
		self.service_uid_counter = 0
		self.line_uid_counter = 0
		self.route_uid_counter = 0
		self.stop_point_uid_counter = 0
		self.route_section_uid_counter = 0
		self.route_link_uid_counter = 0
		self.route_location_uid_counter = 0
		self.journey_pattern_link_uid_counter = 0
		self.journey_pattern_section_uid_counter = 0
		self.journey_pattern_uid_counter = 0
		self.days_of_week_uid_counter = 0
		self.special_days_operation_uid_counter = 0
		self.bank_holiday_operation_uid_counter = 0
		self.vehicle_journey_uid_counter = 0
		self.vehicle_journey_link_uid_counter = 0
		self.current_service_uid_map: dict[str, int] = {}
		self.current_journey_pattern_uid_map: dict[str, int] = {}
		self.current_operator_uid_map: dict[str, int] = {}  # per-file: local @id -> global uid
		self.global_garage_uid_map: dict[str, int] = {}    # global (never cleared): garage_code -> uid
		self.current_line_uid_map: dict[str, int] = {}
		self.route_section_def_map: dict[str, Route_Section] = {}
		self.current_route_link_uid_map: dict[str, int] = {}
		self.current_route_uid_map: dict[str, int] = {}
		self.current_journey_pattern_link_uid_map: dict[str, int] = {}
		self.current_journey_pattern_section_uid_map: dict[str, int] = {}
		self.global_serviced_org_uid_map: dict[str, int] = {}             # global (never cleared): org_code -> uid
		# Global content-based cache for operators: (national_code, op_code, short_name) -> uid
		# Allows correct dedup when different files reuse the same local @id for different operators
		self.global_operator_cache: dict[tuple, int] = {}
		# Global map: stop_point_ref -> uid (persists across files; stop points are global)
		self.stop_point_ref_to_uid: dict[str, int] = {}
		
	def _find_element(self, parent, tc_path: str, no_ns_path: str):
		"""Helper to find element with or without namespace"""
		elem = parent.find(tc_path, self.ns)
		if elem is None:
			elem = parent.find(no_ns_path)
		return elem
	
	def _findtext(self, parent, tc_path: str, no_ns_path: str, default: str = ''):
		"""Helper to find text with or without namespace"""
		text = parent.findtext(tc_path, None, self.ns)
		if text is None:
			text = parent.findtext(no_ns_path, default)
		return text if text else default
	
	def parse(self):
		"""Parse the XML file and populate collections"""
		# Clear per-file local-id maps so operator @ids from one file
		# don't pollute lookups in the next file.
		self.current_operator_uid_map.clear()
		if self.verbose:
			print("Parsing operators...")
		self.parse_operators()
		if self.verbose:
			print(f"  Found {len(self.operators)} operators")
		
		if self.verbose:
			print("Parsing stop points...")
		self.parse_stop_points()
		if self.verbose:
			print(f"  Found {len(self.stop_points)} stop points")

		if self.verbose:
			print("Parsing serviced organisations...")
		self.parse_serviced_organisations()
		if self.verbose:
			print(f"  Found {len(self.serviced_organisations)} serviced organisations")
		
		if self.verbose:
			print("Parsing route sections...")
		self.route_section_def_map.clear()
		self.current_route_link_uid_map.clear()
		self.current_journey_pattern_link_uid_map.clear()
		self.parse_route_sections()
		if self.verbose:
			print(f"  Found {len(self.route_section_def_map)} route sections")
		
		if self.verbose:
			print("Parsing routes...")
		self.current_route_uid_map.clear()
		self.parse_routes()
		if self.verbose:
			print(f"  Found {len(self.routes)} routes")
		
		if self.verbose:
			print("Parsing journey pattern sections...")
		self.current_journey_pattern_section_uid_map.clear()
		self.parse_journey_pattern_sections()
		if self.verbose:
			print(f"  Found {len(self.journey_pattern_sections)} journey pattern sections")
		
		if self.verbose:
			print("Parsing services...")
		self.current_service_uid_map.clear()
		self.current_line_uid_map.clear()
		self.current_journey_pattern_uid_map.clear()
		self.parse_services()
		if self.verbose:
			print(f"  Found {len(self.services)} services")
		
		if self.verbose:
			print("Parsing vehicle journeys...")
		self.parse_vehicle_journeys()
		if self.verbose:
			print(f"  Found {len(self.vehicle_journeys)} vehicle journeys")
		
	def parse_operators(self):
		"""Parse Operators section"""
		operators_elem = self._find_element(self.root, 'tc:Operators', 'Operators')
		if operators_elem is None:
			return
			
		for op_elem in (operators_elem.findall('tc:Operator', self.ns) or operators_elem.findall('Operator')):
			op_id = op_elem.get('id', '')
			national_code = self._findtext(op_elem, 'tc:NationalOperatorCode', 'NationalOperatorCode')
			op_code = self._findtext(op_elem, 'tc:OperatorCode', 'OperatorCode')
			short_name = self._findtext(op_elem, 'tc:OperatorShortName', 'OperatorShortName')
			licence_name = self._findtext(op_elem, 'tc:OperatorNameOnLicence', 'OperatorNameOnLicence')
			trading_name = self._findtext(op_elem, 'tc:TradingName', 'TradingName')
			licence_number = self._findtext(op_elem, 'tc:LicenceNumber', 'LicenceNumber')

			# Dedup by content across all files: same operator may appear in many files
			# under different local @id values
			cache_key = (national_code, op_code, short_name)
			if cache_key in self.global_operator_cache:
				operator_uid = self.global_operator_cache[cache_key]
			else:
				operator_uid = self.operator_uid_counter
				self.operator_uid_counter += 1
				self.global_operator_cache[cache_key] = operator_uid
				self.operators.append(Operator(
					id=op_id,
					uid=operator_uid,
					national_operator_code=national_code,
					operator_code=op_code,
					operator_short_name=short_name,
					licence_number=licence_number,
					operator_name_on_licence=licence_name if licence_name else None,
					trading_name=trading_name if trading_name else None,
				))

			# Always map this file's local @id to the resolved global uid
			if op_id:
				self.current_operator_uid_map[op_id] = operator_uid
			
			garages_elem = self._find_element(op_elem, 'tc:Garages', 'Garages')
			if garages_elem is not None:
				for garage_elem in (garages_elem.findall('tc:Garage', self.ns) or garages_elem.findall('Garage')):
					garage_code = self._findtext(garage_elem, 'tc:GarageCode', 'GarageCode')
					if not garage_code:
						garage_code = garage_elem.get('id', '')
					if not garage_code:
						continue
					if garage_code in self.global_garage_uid_map:
						continue
					garage_name = self._findtext(garage_elem, 'tc:GarageName', 'GarageName')
					loc_elem = self._find_element(garage_elem, 'tc:Location', 'Location')
					garage_lat = 0.0
					garage_lon = 0.0
					if loc_elem is not None:
						garage_lat_str = self._findtext(loc_elem, 'tc:Latitude', 'Latitude')
						garage_lon_str = self._findtext(loc_elem, 'tc:Longitude', 'Longitude')
						try:
							garage_lat = float(garage_lat_str) if garage_lat_str else 0.0
						except ValueError:
							garage_lat = 0.0
						try:
							garage_lon = float(garage_lon_str) if garage_lon_str else 0.0
						except ValueError:
							garage_lon = 0.0

					garage_uid = self.garage_uid_counter
					self.garage_uid_counter += 1
					self.global_garage_uid_map[garage_code] = garage_uid
					self.garages.append(
						Garage(
							uid=garage_uid,
							garage_code=garage_code,
							garage_name=garage_name,
							longitude=garage_lon,
							latitude=garage_lat,
							parent_operator_uid=operator_uid,
						)
					)
	
	def parse_stop_points(self):
		"""Parse StopPoints section"""
		stop_points_elem = self._find_element(self.root, 'tc:StopPoints', 'StopPoints')
		if stop_points_elem is None:
			return
		
		for sp_elem in (stop_points_elem.findall('tc:AnnotatedStopPointRef', self.ns) or stop_points_elem.findall('AnnotatedStopPointRef')):
			stop_ref = self._findtext(sp_elem, 'tc:StopPointRef', 'StopPointRef')
			common_name = self._findtext(sp_elem, 'tc:CommonName', 'CommonName')
			
			stop_point = Bus_Stop_Point(
				uid=self.stop_point_uid_counter,
				stop_point_ref=stop_ref,
				common_name=common_name,
			)
			self.stop_points.append(stop_point)
			if stop_ref and stop_ref not in self.stop_point_ref_to_uid:
				self.stop_point_ref_to_uid[stop_ref] = self.stop_point_uid_counter
			self.stop_point_uid_counter += 1

	def parse_serviced_organisations(self):
		"""Parse ServicedOrganisations and date ranges"""
		orgs_elem = self._find_element(self.root, 'tc:ServicedOrganisations', 'ServicedOrganisations')
		if orgs_elem is None:
			return

		for org_elem in (orgs_elem.findall('tc:ServicedOrganisation', self.ns) or orgs_elem.findall('ServicedOrganisation')):
			org_code = self._findtext(org_elem, 'tc:OrganisationCode', 'OrganisationCode')
			name = self._findtext(org_elem, 'tc:Name', 'Name')
			if not org_code:
				continue

			org_uid = self.global_serviced_org_uid_map.get(org_code)
			if org_uid is None:
				org_uid = self.serviced_organisation_uid_counter
				self.serviced_organisation_uid_counter += 1
				self.global_serviced_org_uid_map[org_code] = org_uid
				self.serviced_organisations.append(
					Serviced_Organisation(uid=org_uid, organisation_code=org_code, name=name)
				)

			working_days_elem = self._find_element(org_elem, 'tc:WorkingDays', 'WorkingDays')
			if working_days_elem is None:
				continue
			for dr_elem in (working_days_elem.findall('tc:DateRange', self.ns) or working_days_elem.findall('DateRange')):
				start_date = self._findtext(dr_elem, 'tc:StartDate', 'StartDate')
				end_date = self._findtext(dr_elem, 'tc:EndDate', 'EndDate')
				description = self._findtext(dr_elem, 'tc:Description', 'Description')

				date_range = Serviced_Organisation_Date_Range(
					uid=self.serviced_organisation_date_range_uid_counter,
					organisation_code=org_code,
					organisation_uid=org_uid,
					start_date=start_date,
					end_date=end_date,
					description=description,
				)
				self.serviced_organisation_date_ranges.append(date_range)
				self.serviced_organisation_date_range_uid_counter += 1
	
	def parse_route_sections(self):
		"""Parse RouteSections with RouteLinks and RouteLocations"""
		route_sections_elem = self._find_element(self.root, 'tc:RouteSections', 'RouteSections')
		if route_sections_elem is None:
			return
		
		for rs_elem in (route_sections_elem.findall('tc:RouteSection', self.ns) or route_sections_elem.findall('RouteSection')):
			rs_id = rs_elem.get('id', '')
			
			# Parse RouteLinks within this RouteSection
			start_rl_index = self.route_link_uid_counter
			
			for rl_elem in (rs_elem.findall('tc:RouteLink', self.ns) or rs_elem.findall('RouteLink')):
				rl_id = rl_elem.get('id', '')
				
				# Get From StopPointRef
				from_elem = self._find_element(rl_elem, 'tc:From', 'From')
				from_stop = ''
				if from_elem is not None:
					from_stop = self._findtext(from_elem, 'tc:StopPointRef', 'StopPointRef')
				
				# Get To StopPointRef
				to_elem = self._find_element(rl_elem, 'tc:To', 'To')
				to_stop = ''
				if to_elem is not None:
					to_stop = self._findtext(to_elem, 'tc:StopPointRef', 'StopPointRef')
				
				distance = self._findtext(rl_elem, 'tc:Distance', 'Distance', '0')
				
				# Parse RouteLocations within this RouteLink (inside Track/Mapping)
				start_rloc_index = self.route_location_uid_counter
				
				track_elem = self._find_element(rl_elem, 'tc:Track', 'Track')
				if track_elem is not None:
					mapping_elem = self._find_element(track_elem, 'tc:Mapping', 'Mapping')
					if mapping_elem is not None:
						for rloc_elem in (mapping_elem.findall('tc:Location', self.ns) or mapping_elem.findall('Location')):
							rloc_id = rloc_elem.get('id', '')
							longitude = self._findtext(rloc_elem, 'tc:Longitude', 'Longitude', '0')
							latitude = self._findtext(rloc_elem, 'tc:Latitude', 'Latitude', '0')
							
							route_location = Route_Location(
								uid=self.route_location_uid_counter,
								location_id=rloc_id,
								longitude=float(longitude),
								latitude=float(latitude),
								global_seq=self.route_location_uid_counter,
							)
							self.route_locations.append(route_location)
							self.route_location_uid_counter += 1
				
				end_rloc_index = self.route_location_uid_counter - 1
				if end_rloc_index < start_rloc_index:  # no locations parsed
					start_rloc_index = -1
					end_rloc_index = -1
				
				route_link = Route_Link(
					uid=self.route_link_uid_counter,
					link_id=rl_id,
					from_bus_stop_point_uid=self.stop_point_ref_to_uid.get(from_stop, -1),
					from_stop_point_ref=from_stop,
					to_bus_stop_point_uid=self.stop_point_ref_to_uid.get(to_stop, -1),
					to_stop_point_ref=to_stop,
					distance=int(distance) if distance else 0,
					start_route_location_index=start_rloc_index,
					end_route_location_index=end_rloc_index,
					global_seq=self.route_link_uid_counter,
				)
				self.route_links.append(route_link)
				if rl_id and rl_id not in self.current_route_link_uid_map:
					self.current_route_link_uid_map[rl_id] = route_link.uid
				self.route_link_uid_counter += 1
			
			end_rl_index = self.route_link_uid_counter - 1
			if rs_id:
				self.route_section_def_map[rs_id] = Route_Section(
					uid=0,
					section_id=rs_id,
					start_route_link_index=start_rl_index,
					end_route_link_index=end_rl_index,
				)

	def parse_routes(self):
		"""Parse Routes section"""
		routes_elem = self._find_element(self.root, 'tc:Routes', 'Routes')
		if routes_elem is None:
			return
		
		for route_elem in (routes_elem.findall('tc:Route', self.ns) or routes_elem.findall('Route')):
			route_id = route_elem.get('id', '')
			private_code = self._findtext(route_elem, 'tc:PrivateCode', 'PrivateCode')
			description = self._findtext(route_elem, 'tc:Description', 'Description')
			route_uid = self.route_uid_counter
			self.route_uid_counter += 1

			route_section_refs = []
			for rs_ref_elem in (route_elem.findall('tc:RouteSectionRef', self.ns) or route_elem.findall('RouteSectionRef')):
				ref_val = (rs_ref_elem.text or '').strip() or rs_ref_elem.get('ref', '') or ''
				if ref_val:
					route_section_refs.append(ref_val)

			start_index = -1
			end_index = -1
			if route_section_refs:
				start_index = self.route_section_uid_counter
				for route_section_ref in route_section_refs:
					def_section = self.route_section_def_map.get(route_section_ref)
					if def_section is None:
						continue
					self.route_sections.append(
						Route_Section(
							uid=self.route_section_uid_counter,
							section_id=def_section.section_id,
							start_route_link_index=def_section.start_route_link_index,
							end_route_link_index=def_section.end_route_link_index,
						)
					)
					self.route_section_uid_counter += 1
				end_index = self.route_section_uid_counter - 1

			route = Route(
				uid=route_uid,
				route_id=route_id,
				private_code=private_code if private_code else None,
				description=description,
				route_section_start_index=start_index,
				route_section_end_index=end_index,
			)
			self.routes.append(route)
			if route_id and route_id not in self.current_route_uid_map:
				self.current_route_uid_map[route_id] = route.uid
	
	def parse_journey_pattern_sections(self):
		"""Parse JourneyPatternSections with JourneyPatternTimingLinks"""
		jp_sections_elem = self._find_element(self.root, 'tc:JourneyPatternSections', 'JourneyPatternSections')
		if jp_sections_elem is None:
			return
		
		for jps_elem in (jp_sections_elem.findall('tc:JourneyPatternSection', self.ns) or jp_sections_elem.findall('JourneyPatternSection')):
			jps_id = jps_elem.get('id', '')
			
			start_jpl_index = self.journey_pattern_link_uid_counter
			
			for jptl_elem in (jps_elem.findall('tc:JourneyPatternTimingLink', self.ns) or jps_elem.findall('JourneyPatternTimingLink')):
				jptl_id = jptl_elem.get('id', '')

				# Parse detailed From/To elements when available (SequenceNumber, Activity, StopPointRef, etc.)
				from_elem = self._find_element(jptl_elem, 'tc:From', 'From')
				to_elem = self._find_element(jptl_elem, 'tc:To', 'To')
			
				def _parse_jp_point(elem):
					if elem is None:
						return None
					point_id = self._findtext(elem, 'tc:StopPointRef', 'StopPointRef')
					seq_attr = elem.get('SequenceNumber') or self._findtext(elem, 'tc:SequenceNumber', 'SequenceNumber')
					try:
						seq = int(seq_attr) if seq_attr else 0
					except ValueError:
						seq = 0
					activity = self._findtext(elem, 'tc:Activity', 'Activity')
					dest = self._findtext(elem, 'tc:DynamicDestinationDisplay', 'DynamicDestinationDisplay') or self._findtext(elem, 'tc:DestinationDisplay', 'DestinationDisplay')
					timing_status = self._findtext(elem, 'tc:TimingStatus', 'TimingStatus')
					fare_stage = self._findtext(elem, 'tc:FareStageNumber', 'FareStageNumber')
					return Journey_Pattern_Link.JP_Point(
						point_id=point_id,
						sequence_num=seq,
						activity=activity,
						destination_display=dest if dest else None,
						timing_status=timing_status,
						fare_stage_num=fare_stage if fare_stage else None,
					)

				route_link_ref = self._findtext(jptl_elem, 'tc:RouteLinkRef', 'RouteLinkRef')
				run_time = self._findtext(jptl_elem, 'tc:RunTime', 'RunTime', 'PT0M0S')
			
				# resolve route_link_ref to uid when possible
				route_link_uid_val = 0
				if route_link_ref:
					route_link_uid_val = self.current_route_link_uid_map.get(route_link_ref, 0)

				journey_pattern_link = Journey_Pattern_Link(
					id=jptl_id,
					uid=self.journey_pattern_link_uid_counter,
					from_JP_point=_parse_jp_point(from_elem),
					to_JP_point=_parse_jp_point(to_elem),
					route_link_ref=route_link_ref,
					route_link_uid=route_link_uid_val,
					run_time=run_time,
					global_seq=self.journey_pattern_link_uid_counter,
				)
				self.journey_pattern_links.append(journey_pattern_link)
				if jptl_id and jptl_id not in self.current_journey_pattern_link_uid_map:
					self.current_journey_pattern_link_uid_map[jptl_id] = journey_pattern_link.uid
				self.journey_pattern_link_uid_counter += 1
			
			end_jpl_index = self.journey_pattern_link_uid_counter - 1
			
			journey_pattern_section = Journey_Pattern_Section(
				uid=self.journey_pattern_section_uid_counter,
				section_id=jps_id,
				start_JP_link_index=start_jpl_index,
				end_JP_link_index=end_jpl_index
			)
			self.journey_pattern_sections.append(journey_pattern_section)
			if jps_id:
				self.current_journey_pattern_section_uid_map[jps_id] = journey_pattern_section.uid
			self.journey_pattern_section_uid_counter += 1
	
	def parse_services(self):
		"""Parse Services section"""
		services_elem = self._find_element(self.root, 'tc:Services', 'Services')
		if services_elem is None:
			return
		
		for service_elem in (services_elem.findall('tc:Service', self.ns) or services_elem.findall('Service')):
			service_code = self._findtext(service_elem, 'tc:ServiceCode', 'ServiceCode')
			operator_ref = self._findtext(service_elem, 'tc:RegisteredOperatorRef', 'RegisteredOperatorRef')
			service_uid = self.service_uid_counter
			
			# Get line info
			lines_elem = self._find_element(service_elem, 'tc:Lines', 'Lines')
			origin = ''
			destination = ''
			
			if lines_elem is not None:
				for line_elem in (lines_elem.findall('tc:Line', self.ns) or lines_elem.findall('Line')):
					line_id = line_elem.get('id', '')
					line_name = self._findtext(line_elem, 'tc:LineName', 'LineName')

					out_desc = self._find_element(line_elem, 'tc:OutboundDescription', 'OutboundDescription')
					out_origin = ''
					out_destination = ''
					out_desc_description = ''
					if out_desc is not None:
						out_origin = self._findtext(out_desc, 'tc:Origin', 'Origin')
						out_destination = self._findtext(out_desc, 'tc:Destination', 'Destination')
						out_desc_description = self._findtext(out_desc, 'tc:Description', 'Description')
					# inbound description if provided
					in_desc = self._find_element(line_elem, 'tc:InboundDescription', 'InboundDescription')
					in_origin = ''
					in_destination = ''
					in_desc_description = ''
					if in_desc is not None:
						in_origin = self._findtext(in_desc, 'tc:Origin', 'Origin')
						in_destination = self._findtext(in_desc, 'tc:Destination', 'Destination')
						in_desc_description = self._findtext(in_desc, 'tc:Description', 'Description')
					if not in_origin:
						in_origin = out_destination
					if not in_destination:
						in_destination = out_origin

					line = Line(
						uid=self.line_uid_counter,
						line_id=line_id,
						line_name=line_name,
						out_bound=Line.S_Bound(
							origin=out_origin if out_origin else None,
							destination=out_destination if out_destination else None,
							description=out_desc_description if out_desc_description else None,
						),
						in_bound=Line.S_Bound(
							origin=in_origin if in_origin else None,
							destination=in_destination if in_destination else None,
							description=in_desc_description if in_desc_description else None,
						),
						parent_service_code=service_code,
						parent_service_uid=service_uid,
					)
					self.lines.append(line)
					if line_id:
						self.current_line_uid_map[line_id] = self.line_uid_counter
					self.line_uid_counter += 1

					if not origin:
						origin = out_origin
					if not destination:
						destination = out_destination

			# StandardService origin/destination fallback
			standard_service_elem = self._find_element(service_elem, 'tc:StandardService', 'StandardService')
			ss_origin = ''
			ss_destination = ''
			if standard_service_elem is not None:
				ss_origin = self._findtext(standard_service_elem, 'tc:Origin', 'Origin')
				ss_destination = self._findtext(standard_service_elem, 'tc:Destination', 'Destination')
			if not origin:
				origin = ss_origin
			if not destination:
				destination = ss_destination
			
			# Get operating period
			op_period = self._find_element(service_elem, 'tc:OperatingPeriod', 'OperatingPeriod')
			start_date = ''
			end_date = ''
			if op_period is not None:
				start_date = self._findtext(op_period, 'tc:StartDate', 'StartDate')
				end_date = self._findtext(op_period, 'tc:EndDate', 'EndDate')
			
			service = Service(
				uid=service_uid,
				service_code=service_code,
				start_date=start_date,
				end_date=end_date if end_date else None,
				operator_ref=operator_ref,
				operator_uid=self.current_operator_uid_map.get(operator_ref, 0),
				origin=origin,
				destination=destination,
			)
			self.services.append(service)
			if service_code and service_code not in self.current_service_uid_map:
				self.current_service_uid_map[service_code] = service.uid
			self._parse_journey_patterns_for_service(service_elem, service)
			self.service_uid_counter += 1

	def _parse_journey_patterns_for_service(self, service_elem, service: Service):
		standard_service_elem = self._find_element(service_elem, 'tc:StandardService', 'StandardService')
		if standard_service_elem is None:
			return
		
		for jp_elem in (standard_service_elem.findall('tc:JourneyPattern', self.ns) or standard_service_elem.findall('JourneyPattern')):
			journey_pattern_id = jp_elem.get('id', '')
			destination_display = self._findtext(jp_elem, 'tc:DestinationDisplay', 'DestinationDisplay')
			direction = self._findtext(jp_elem, 'tc:Direction', 'Direction')
			operator_ref = self._findtext(jp_elem, 'tc:OperatorRef', 'OperatorRef') or service.operator_ref
			operator_uid = self.current_operator_uid_map.get(operator_ref, 0)
			description = self._findtext(jp_elem, 'tc:Description', 'Description')
		
			route_ref = self._findtext(jp_elem, 'tc:RouteRef', 'RouteRef')
			route_uid = 0
			if route_ref:
				route_uid = self.current_route_uid_map.get(route_ref, 0)
		
			jp_section_refs: list[str] = []
			jp_refs_elems = jp_elem.findall('tc:JourneyPatternSectionRefs', self.ns) or jp_elem.findall('JourneyPatternSectionRefs')
			for jp_refs_container in jp_refs_elems:
				if list(jp_refs_container):
					# Standard format: <JourneyPatternSectionRefs><JourneyPatternSectionRef>JPS1</JourneyPatternSectionRef>...</JourneyPatternSectionRefs>
					for child in jp_refs_container:
						ref_val = (child.text or '').strip()
						if ref_val:
							jp_section_refs.append(ref_val)
				else:
					# Non-standard: <JourneyPatternSectionRefs>JPS1</JourneyPatternSectionRefs> (text directly)
					ref_text = (jp_refs_container.text or '').strip()
					if ref_text:
						jp_section_refs.extend(ref_text.split())
			if not jp_section_refs:
				for jps_ref_elem in (jp_elem.findall('tc:JourneyPatternSectionRef', self.ns) or jp_elem.findall('JourneyPatternSectionRef')):
					jp_ref = jps_ref_elem.text or jps_ref_elem.get('ref', '') or ''
					if jp_ref:
						jp_section_refs.append(jp_ref)
		
			# One JP row per element: store the first and last section ref/uid as a range
			jp_section_start_ref = jp_section_refs[0] if jp_section_refs else ""
			jp_section_end_ref   = jp_section_refs[-1] if jp_section_refs else ""
			jp_section_start_uid = self.current_journey_pattern_section_uid_map.get(jp_section_start_ref, 0) if jp_section_start_ref else 0
			jp_section_end_uid   = self.current_journey_pattern_section_uid_map.get(jp_section_end_ref, 0) if jp_section_end_ref else 0

			journey_pattern = Journey_Pattern(
				journey_pattern_id=journey_pattern_id,
				uid=self.journey_pattern_uid_counter,
				destination_display=destination_display,
				operatior_ref=operator_ref,
				operator_uid=operator_uid,
				direction=direction,
				description=description if description else None,
				route_ref=route_ref,
				route_uid=route_uid,
				JP_section_start_ref=jp_section_start_ref,
				JP_section_start_uid=jp_section_start_uid,
				JP_section_end_ref=jp_section_end_ref,
				JP_section_end_uid=jp_section_end_uid,
				parent_service_code=service.service_code,
				parent_service_uid=service.uid,
			)
			self.journey_patterns.append(journey_pattern)
			if journey_pattern_id and journey_pattern_id not in self.current_journey_pattern_uid_map:
				self.current_journey_pattern_uid_map[journey_pattern_id] = journey_pattern.uid
			self.journey_pattern_uid_counter += 1
	
	def parse_vehicle_journeys(self):
		"""Parse VehicleJourneys section"""
		vj_elem = self._find_element(self.root, 'tc:VehicleJourneys', 'VehicleJourneys')
		if vj_elem is None:
			return
		
		for vj_child in (vj_elem.findall('tc:VehicleJourney', self.ns) or vj_elem.findall('VehicleJourney')):
			seq_num = int(vj_child.get('SequenceNumber', 0) or 0)
			
			vj_uid = self.vehicle_journey_uid_counter
			private_code = self._findtext(vj_child, 'tc:PrivateCode', 'PrivateCode')
			operator_ref = self._findtext(vj_child, 'tc:OperatorRef', 'OperatorRef')
			operator_uid = self.current_operator_uid_map.get(operator_ref, 0)
			vehicle_journey_code = self._findtext(vj_child, 'tc:VehicleJourneyCode', 'VehicleJourneyCode')
			service_ref = self._findtext(vj_child, 'tc:ServiceRef', 'ServiceRef')
			line_ref = self._findtext(vj_child, 'tc:LineRef', 'LineRef')
			journey_pattern_ref = self._findtext(vj_child, 'tc:JourneyPatternRef', 'JourneyPatternRef')
			departure_time = self._findtext(vj_child, 'tc:DepartureTime', 'DepartureTime')
			garage_ref = self._findtext(vj_child, 'tc:GarageRef', 'GarageRef')
			
			service_uid = self.current_service_uid_map.get(service_ref, 0)
			line_uid = self.current_line_uid_map.get(line_ref, -1)
			journey_pattern_uid = self.current_journey_pattern_uid_map.get(journey_pattern_ref, 0)
			garage_uid = self.global_garage_uid_map.get(garage_ref, -1)
			
			days_of_week_uid = -1
			special_days_operation_start_index = None
			special_days_operation_end_index = None
			bank_holiday_operation_uid = None
			serviced_organisation_ref = ""
			serviced_organisation_uid = -1
			
			op_profile_elem = self._find_element(vj_child, 'tc:OperatingProfile', 'OperatingProfile')
			if op_profile_elem is not None:
				# RegularDayType -> DaysOfWeek
				rdt_elem = self._find_element(op_profile_elem, 'tc:RegularDayType', 'RegularDayType')
				if rdt_elem is not None:
					dow_elem = self._find_element(rdt_elem, 'tc:DaysOfWeek', 'DaysOfWeek')
					if dow_elem is not None:
						days = Days_Of_Week(
							uid=self.days_of_week_uid_counter,
							parent_VJ_uid=vj_uid,
							monday=(dow_elem.find('tc:Monday', self.ns) is not None or dow_elem.find('Monday') is not None),
							tuesday=(dow_elem.find('tc:Tuesday', self.ns) is not None or dow_elem.find('Tuesday') is not None),
							wednesday=(dow_elem.find('tc:Wednesday', self.ns) is not None or dow_elem.find('Wednesday') is not None),
							thursday=(dow_elem.find('tc:Thursday', self.ns) is not None or dow_elem.find('Thursday') is not None),
							friday=(dow_elem.find('tc:Friday', self.ns) is not None or dow_elem.find('Friday') is not None),
							saturday=(dow_elem.find('tc:Saturday', self.ns) is not None or dow_elem.find('Saturday') is not None),
							sunday=(dow_elem.find('tc:Sunday', self.ns) is not None or dow_elem.find('Sunday') is not None),
						)
						self.days_of_weeks.append(days)
						days_of_week_uid = self.days_of_week_uid_counter
						self.days_of_week_uid_counter += 1
				# SpecialDaysOperation
				sdo_elem = self._find_element(op_profile_elem, 'tc:SpecialDaysOperation', 'SpecialDaysOperation')
				if sdo_elem is not None:
					def _collect_date_ranges(parent, child_name):
						ranges = []
						container = self._find_element(parent, f'tc:{child_name}', child_name)
						if container is None:
							return []
						for dr_elem in (container.findall('tc:DateRange', self.ns) or container.findall('DateRange')):
							start = self._findtext(dr_elem, 'tc:StartDate', 'StartDate')
							end = self._findtext(dr_elem, 'tc:EndDate', 'EndDate')
							ranges.append((start, end))
						return ranges

					operate_ranges = _collect_date_ranges(sdo_elem, 'DaysOfOperation')
					non_operate_ranges = _collect_date_ranges(sdo_elem, 'DaysOfNonOperation')
				
					for start, end in operate_ranges:
						if special_days_operation_start_index is None:
							special_days_operation_start_index = self.special_days_operation_uid_counter
						special_days = Special_Days_Operation(
							uid=self.special_days_operation_uid_counter,
							parent_VJ_uid=vj_uid,
							do_operate=True,
							start_date=start,
							end_date=end,
						)
						self.special_days_operations.append(special_days)
						special_days_operation_end_index = self.special_days_operation_uid_counter
						self.special_days_operation_uid_counter += 1

					for start, end in non_operate_ranges:
						if special_days_operation_start_index is None:
							special_days_operation_start_index = self.special_days_operation_uid_counter
						special_days = Special_Days_Operation(
							uid=self.special_days_operation_uid_counter,
							parent_VJ_uid=vj_uid,
							do_operate=False,
							start_date=start,
							end_date=end,
						)
						self.special_days_operations.append(special_days)
						special_days_operation_end_index = self.special_days_operation_uid_counter
						self.special_days_operation_uid_counter += 1
				# ServicedOrganisationDayType -> ServicedOrganisationRef
				sodt_elem = self._find_element(op_profile_elem, 'tc:ServicedOrganisationDayType', 'ServicedOrganisationDayType')
				if sodt_elem is not None:
					for container_name in ['DaysOfOperation', 'DaysOfNonOperation']:
						container = self._find_element(sodt_elem, f'tc:{container_name}', container_name)
						if container is not None:
							for wdays_tag in ['WorkingDays', 'HolidayDays']:
								wdays = self._find_element(container, f'tc:{wdays_tag}', wdays_tag)
								if wdays is not None:
									ref = self._findtext(wdays, 'tc:ServicedOrganisationRef', 'ServicedOrganisationRef')
									if ref:
										serviced_organisation_ref = ref
										serviced_organisation_uid = self.global_serviced_org_uid_map.get(ref, -1)
										break
							if serviced_organisation_ref:
								break

				# BankHolidayOperation -> collect named child tags under DaysOfOperation / DaysOfNonOperation
				bho_elem = self._find_element(op_profile_elem, 'tc:BankHolidayOperation', 'BankHolidayOperation')
				if bho_elem is not None:
					def _collect_day_names(parent, child_name):
						col = []
						container = self._find_element(parent, f'tc:{child_name}', child_name)
						if container is None:
							return []
						for child in list(container):
							name = child.tag.split('}')[-1]
							col.append(name)
						return col
					days_op = _collect_day_names(bho_elem, 'DaysOfOperation')
					days_non_op = _collect_day_names(bho_elem, 'DaysOfNonOperation')
					bank_holiday = Bank_Holiday_Operation(
						uid=self.bank_holiday_operation_uid_counter,
						parent_VJ_uid=vj_uid,
						days_of_operation=days_op or None,
						days_of_non_operation=days_non_op or None,
					)
					self.bank_holiday_operations.append(bank_holiday)
					bank_holiday_operation_uid = self.bank_holiday_operation_uid_counter
					self.bank_holiday_operation_uid_counter += 1
			
			vj_link_start = self.vehicle_journey_link_uid_counter
			vehicle_journey = Vehicle_Journey(
				uid=vj_uid,
				private_code=private_code,
				sequence_number=seq_num,
				operator_ref=operator_ref,
				operator_uid=operator_uid,
				days_of_week_uid=days_of_week_uid,
				special_days_operation_start_index=special_days_operation_start_index,
				special_days_operation_end_index=special_days_operation_end_index,
				bank_holiday_operation_uid=bank_holiday_operation_uid,
				serviced_organisation_ref=serviced_organisation_ref,
				serviced_organisation_uid=serviced_organisation_uid,
				garage_ref=garage_ref,
				garage_uid=garage_uid,
				VJ_code=vehicle_journey_code,
				service_ref=service_ref,
				service_uid=service_uid,
				line_ref=line_ref,
				line_uid=line_uid,
				JP_ref=journey_pattern_ref,
				JP_uid=journey_pattern_uid,
				departure_time=departure_time,
				VJ_link_start_index=vj_link_start,
				VJ_link_end_index=-1,  # filled in after links are parsed
			)
			self.vehicle_journeys.append(vehicle_journey)
			self.vehicle_journey_uid_counter += 1
			self._parse_vehicle_journey_links(vj_child, vj_uid, vehicle_journey_code)
			vj_link_end = self.vehicle_journey_link_uid_counter - 1
			if vj_link_end < vj_link_start:  # no VJ links parsed
				vj_link_start = -1
				vj_link_end = -1
				vehicle_journey.VJ_link_start_index = -1
			vehicle_journey.VJ_link_end_index = vj_link_end

	def _parse_vehicle_journey_links(self, vj_child, parent_vj_uid: int, parent_vj_code: str):
		links_parent = self._find_element(vj_child, 'tc:VehicleJourneyTimingLinks', 'VehicleJourneyTimingLinks')
		link_elems = []
		link_elems = links_parent.findall('tc:VehicleJourneyTimingLink', self.ns) if links_parent is not None else []
		if not link_elems:
			link_elems = vj_child.findall('tc:VehicleJourneyTimingLink', self.ns) or vj_child.findall('VehicleJourneyTimingLink')
		
		for vjtl_elem in link_elems:
			link_id_raw = vjtl_elem.get('id', '')
			link_id = link_id_raw or f"vjl_{self.vehicle_journey_link_uid_counter}"
			jp_link_ref = self._findtext(vjtl_elem, 'tc:JourneyPatternTimingLinkRef', 'JourneyPatternTimingLinkRef')
			runtime = self._findtext(vjtl_elem, 'tc:RunTime', 'RunTime', '')
			from_activity = ""
			to_activity = ""
			from_elem = self._find_element(vjtl_elem, 'tc:From', 'From')
			to_elem = self._find_element(vjtl_elem, 'tc:To', 'To')
			if from_elem is not None:
				from_activity = self._findtext(from_elem, 'tc:Activity', 'Activity')
			if to_elem is not None:
				to_activity = self._findtext(to_elem, 'tc:Activity', 'Activity')
			
			jp_link_uid = 0
			if jp_link_ref:
				jp_link_uid = self.current_journey_pattern_link_uid_map.get(jp_link_ref, 0)
			
			vj_link = Vehicle_Journey_Link(
				uid=self.vehicle_journey_link_uid_counter,
				parent_VJ_code=parent_vj_code,
				parent_VJ_uid=parent_vj_uid,
				link_id=link_id,
				JP_link_ref=jp_link_ref,
				JP_link_uid=jp_link_uid,
				runtime=runtime,
				from_activity=from_activity,
				to_activity=to_activity,
				global_seq=self.vehicle_journey_link_uid_counter,
			)
			self.vehicle_journey_links.append(vj_link)
			self.vehicle_journey_link_uid_counter += 1
	
	def save_to_csv(self):
		"""Save all parsed objects to CSV files"""
		self.save_operators_csv()
		self.save_garages_csv()
		self.save_serviced_organisations_csv()
		self.save_serviced_organisation_date_ranges_csv()
		self.save_bus_stop_points_csv()
		self.save_services_csv()
		self.save_lines_csv()
		self.save_routes_csv()
		self.save_route_sections_csv()
		self.save_route_links_csv()
		self.save_route_locations_csv()
		self.save_journey_pattern_links_csv()
		self.save_journey_pattern_sections_csv()
		self.save_journey_patterns_csv()
		self.save_days_of_weeks_csv()
		self.save_special_days_operations_csv()
		self.save_bank_holiday_operations_csv()
		self.save_vehicle_journey_links_csv()
		self.save_vehicle_journeys_csv()

	def clear_parsed_data(self):
		"""Clear parsed lists to release memory between batches"""
		self.operators.clear()
		self.garages.clear()
		self.serviced_organisations.clear()
		self.serviced_organisation_date_ranges.clear()
		self.services.clear()
		self.lines.clear()
		self.stop_points.clear()
		self.routes.clear()
		self.route_sections.clear()
		self.route_links.clear()
		self.route_locations.clear()
		self.journey_pattern_links.clear()
		self.journey_pattern_sections.clear()
		self.journey_patterns.clear()
		self.days_of_weeks.clear()
		self.special_days_operations.clear()
		self.bank_holiday_operations.clear()
		self.vehicle_journeys.clear()
		self.vehicle_journey_links.clear()
		self.route_section_def_map.clear()
	
	def save_operators_csv(self):
		"""Save operators to CSV"""
		filepath = os.path.join(self.output_dir, 'operator.csv')
		if not self.operators:
			return

		header = [field.name for field in fields(self.operators[0])]
		compare_fields = [name for name in header if name != "uid"]

		file_exists = os.path.exists(filepath)
		file_empty = not file_exists or os.path.getsize(filepath) == 0

		seen: set[tuple[str, ...]] = set()
		if not file_empty:
			with open(filepath, "r", newline="", encoding="utf-8") as f:
				reader = csv.DictReader(f)
				for row in reader:
					key = tuple((row.get(field) or "") for field in compare_fields)
					seen.add(key)

		with open(filepath, "a", newline="", encoding="utf-8") as f:
			writer = csv.DictWriter(f, fieldnames=header)
			if file_empty:
				writer.writeheader()
			written = 0
			for operator in self.operators:
				row = asdict(operator)
				key = tuple(("" if row.get(field) is None else str(row.get(field))) for field in compare_fields)
				if key in seen:
					continue
				writer.writerow(row)
				seen.add(key)
				written += 1

		if self.verbose:
			print(f"Saved {written} records to {filepath}")

	def save_garages_csv(self):
		"""Save garages to CSV"""
		filepath = os.path.join(self.output_dir, 'garage.csv')
		csv_save(filepath, self.garages, verbose=self.verbose)

	def save_serviced_organisations_csv(self):
		"""Save serviced organisations to CSV"""
		filepath = os.path.join(self.output_dir, 'serviced_organisation.csv')
		csv_save(filepath, self.serviced_organisations, verbose=self.verbose)

	def save_serviced_organisation_date_ranges_csv(self):
		"""Save serviced organisation date ranges to CSV"""
		filepath = os.path.join(self.output_dir, 'serviced_organisation_date_range.csv')
		csv_save(filepath, self.serviced_organisation_date_ranges, verbose=self.verbose)
	
	def save_bus_stop_points_csv(self):
		"""Save bus stop points to CSV (avoid conflict with NAPTAN stop_point.csv)"""
		filepath = os.path.join(self.output_dir, 'bus_stop_point.csv')
		csv_save(filepath, self.stop_points, verbose=self.verbose)
	
	def save_services_csv(self):
		"""Save services to CSV"""
		filepath = os.path.join(self.output_dir, 'service.csv')
		csv_save_nested(filepath, self.services, verbose=self.verbose)

	def save_lines_csv(self):
		"""Save lines to CSV"""
		filepath = os.path.join(self.output_dir, 'line.csv')
		csv_save_nested(filepath, self.lines, verbose=self.verbose)

	def save_routes_csv(self):
		"""Save routes to CSV"""
		filepath = os.path.join(self.output_dir, 'route.csv')
		csv_save(filepath, self.routes, verbose=self.verbose)

	def save_route_sections_csv(self):
		"""Save route sections to CSV"""
		filepath = os.path.join(self.output_dir, 'route_section.csv')
		csv_save(filepath, self.route_sections, verbose=self.verbose)
	
	def save_route_links_csv(self):
		"""Save route links to CSV"""
		filepath = os.path.join(self.output_dir, 'route_link.csv')
		csv_save(filepath, self.route_links, verbose=self.verbose)
	
	def save_route_locations_csv(self):
		"""Save route locations to CSV"""
		filepath = os.path.join(self.output_dir, 'route_location.csv')
		csv_save(filepath, self.route_locations, verbose=self.verbose)
	
	def save_journey_pattern_links_csv(self):
		"""Save journey pattern links to CSV"""
		filepath = os.path.join(self.output_dir, 'journey_pattern_link.csv')
		csv_save_nested(filepath, self.journey_pattern_links, verbose=self.verbose)
	
	def save_journey_pattern_sections_csv(self):
		"""Save journey pattern sections to CSV"""
		filepath = os.path.join(self.output_dir, 'journey_pattern_section.csv')
		csv_save(filepath, self.journey_pattern_sections, verbose=self.verbose)

	def save_journey_patterns_csv(self):
		"""Save journey patterns to CSV"""
		filepath = os.path.join(self.output_dir, 'journey_pattern.csv')
		csv_save(filepath, self.journey_patterns, verbose=self.verbose)

	def save_days_of_weeks_csv(self):
		"""Save days of week to CSV"""
		filepath = os.path.join(self.output_dir, 'days_of_week.csv')
		csv_save(filepath, self.days_of_weeks, verbose=self.verbose)

	def save_special_days_operations_csv(self):
		"""Save special days operations to CSV"""
		filepath = os.path.join(self.output_dir, 'special_days_operation.csv')
		csv_save(filepath, self.special_days_operations, verbose=self.verbose)

	def save_bank_holiday_operations_csv(self):
		"""Save bank holiday operations to CSV"""
		filepath = os.path.join(self.output_dir, 'bank_holiday_operation.csv')
		csv_save_nested(filepath, self.bank_holiday_operations, verbose=self.verbose)

	def save_vehicle_journey_links_csv(self):
		"""Save vehicle journey links to CSV"""
		filepath = os.path.join(self.output_dir, 'vehicle_journey_link.csv')
		csv_save(filepath, self.vehicle_journey_links, verbose=self.verbose)
	
	def save_vehicle_journeys_csv(self):
		"""Save vehicle journeys to CSV"""
		filepath = os.path.join(self.output_dir, 'vehicle_journey.csv')
		csv_save(filepath, self.vehicle_journeys, verbose=self.verbose)


def main():
	"""Main function to parse two XML files and save to CSV"""
	verbose = False

	# Get the parent directory of the script
	script_dir = Path(__file__).parent.absolute()
	
	# Get all XML files from BusTimeTable directory
	bus_timetable_dir = script_dir / "DATA" / "ZIP_XML"
	xml_files = sorted([f for f in bus_timetable_dir.glob("*.xml")])
	
	if len(xml_files) < 2:
		print(f"Error: Found only {len(xml_files)} XML file(s). Need at least 2.")
		return
	
	# Take all XML files for parsing
	xml_files_to_parse = xml_files 

	# Output directory
	output_dir = script_dir / "CSV_BUS"
	
	# Create output directory if it doesn't exist
	os.makedirs(output_dir, exist_ok=True)

	# Initialize/clear CSV files
	output_files = [
		"operator.csv",
		"garage.csv",
		"serviced_organisation.csv",
		"serviced_organisation_date_range.csv",
		"bus_stop_point.csv",
		"service.csv",
		"line.csv",
		"route.csv",
		"route_section.csv",
		"route_link.csv",
		"route_location.csv",
		"journey_pattern_link.csv",
		"journey_pattern_section.csv",
		"journey_pattern.csv",
		"days_of_week.csv",
		"special_days_operation.csv",
		"bank_holiday_operation.csv",
		"vehicle_journey_link.csv",
		"vehicle_journey.csv",
	]
	for filename in output_files:
		csv_init(str(output_dir / filename))
	
	# Create parser with first file
	parser = XMLParser(str(xml_files_to_parse[0]), str(output_dir), verbose=verbose)
	
	# Parse files in batches and append to CSV
	batch_size = 10
	for i, xml_file in enumerate(xml_files_to_parse, 1):
		print(f"[File {i}/ {len(xml_files_to_parse)}]\tParsing XML file:\t{xml_file.name}")
		if i == 1:
			# First file already initialized in parser
			parser.parse()
		else:
			# For subsequent files, update the parser's XML file reference and re-parse
			parser.xml_file = str(xml_file)
			parser.tree = ET.parse(str(xml_file))
			parser.root = parser.tree.getroot()
			parser.parse()
		
		if i % batch_size == 0:
			if verbose:
				print("\n" + "="*60)
				print(f"Saving batch ending at file {i} to CSV files...")
			parser.save_to_csv()
			parser.clear_parsed_data()

	if xml_files_to_parse and (len(xml_files_to_parse) % batch_size != 0):
		if verbose:
			print("\n" + "="*60)
			print("Saving final batch to CSV files...")
		parser.save_to_csv()
		parser.clear_parsed_data()

	if verbose:
		print(f"\nAll files saved to {output_dir}")