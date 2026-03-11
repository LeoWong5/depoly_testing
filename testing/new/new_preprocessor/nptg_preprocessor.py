import json
import xml.etree.ElementTree as ET
import csv
import os
import zipfile
import requests
from dataclasses import dataclass, asdict, fields

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

@dataclass
class Region:
	region_code : str
	name : str
	country : str
	admin_areas_list : list[int] # List of index as references to child authority

	def __init__(   self,
					in_region : str,
					in_name : str,
					in_country : str):
		self.region_code        = in_region
		self.name               = in_name
		self.country            = in_country
		self.admin_areas_list   = list()

@dataclass
class Authority:
	admin_area_code : str
	atco_area_code : str
	name : str
	short_name : str
	national : bool # All national will be false, but if atco_area_code start with 9, it is national
	parent_index : int  # index to parent region
	nptg_districts_list : list[int] # list of index to child nptg_district
	alpha_prefix_list : list[str]   # list of AlphaPrefix of authority

	def __init__(   self,
					in_admin_area_code : str,
					in_atco_area_code : str,
					in_name : str,
					in_short_name : str,
					in_national : bool,
					in_parent_index : int):
		self.admin_area_code    = in_admin_area_code
		self.atco_area_code     = in_atco_area_code
		self.name               = in_name
		self.short_name         = in_short_name
		self.national           = in_national
		self.parent_index       = in_parent_index
		self.nptg_districts_list= list()
		self.alpha_prefix_list  = list()

@dataclass
class District:
	nptg_district_code : str
	name : str
	parent_index : int  # index to parent authority

	def __init__(   self,
					in_code : str,
					in_name : str,
					in_index : int):
		self.nptg_district_code = in_code
		self.name               = in_name
		self.parent_index       = in_index

@dataclass
class Locality:
	nptg_locality_code : str
	locality_name : str		# name
	qualifier_name : str | None	# if duplicate locality_name, this field provide extra detail
	parent_nptg_locality_ref : str | None	# ref to parent (larger) Locality, None for no ref
	authority_ref : str		# ref to belonged authority
	nptg_district_ref : str # ref to belonged district
	locality_type : str
	longitude : float
	latitude : float

	def __init__(	self,
			  		in_locality_code : str,
					in_locality_name : str,
					in_qualifier_name : str | None,
					in_parent_ref : str | None,
					in_authority_ref : str,
					in_district_ref : str,
					in_locality_type : str,
					in_longitude : float,
					in_latitude : float):
		self.nptg_locality_code 		= in_locality_code
		self.locality_name 				= in_locality_name
		self.qualifier_name				= in_qualifier_name
		self.parent_nptg_locality_ref 	= in_parent_ref
		self.authority_ref				= in_authority_ref
		self.nptg_district_ref 			= in_district_ref
		self.locality_type				= in_locality_type
		self.longitude					= in_longitude
		self.latitude					= in_latitude

# SP == Stop_Point

@dataclass
class SP_descriptor:
	common_name : str
	landmark : str
	street : str
	indicator : str

	def __init__(	self,
					in_name			: str,
					in_landmark 	: str,
					in_street 		: str,
					indicator 		: str):
		self.common_name 		= in_name
		self.landmark			= in_landmark
		self.street				= in_street
		self.indicator			= indicator

@dataclass
class SP_place:
	nptg_locality_ref : str
	longitude : float
	latitude : float
	missing_data	: bool

@dataclass
class SP_stop:
	stop_type : str
	bus_stop_type 	: str | None
	timing_status 	: str | None
	compass_point 	: str | None
	degrees 		: int | None

	# StopAreas, PlusbusZones

	# stop_type == BCT -> OnStreet
	# bus_stop_type, timing_status, compass_point, degrees (Optional)

	# stop_type == BCS -> OffStreet
	# timing_status

	# stop_type == RSE -> OffStreet
	# Entrance (All None, so not exist)

	# stop_type == TMU -> OffStreet
	# Entrance (All None, so not exist)

	# stop_type == TXR -> OnStreet
	# TaxiRank (All None, so not exist)

	# stop_type == FTD -> OffStreet
	# Entrance (All None, so not exist)

@dataclass
class Stop_Point:
	atco_code : str
	nptg_authority_ref : str
	descriptor : SP_descriptor
	place : SP_place
	stop : SP_stop

	naptan_code : str | None
	plus_bus_zone_ref : str | None # ref to other source
	stop_areas_ref : str | None # ref to naptan stop area
	stop_start_date : str | None

	def to_flat_dict(self) -> dict:
        # 1. Start with the top-level fields
		flat_data = {
			"atco_code": self.atco_code,
			"nptg_authority_ref": self.nptg_authority_ref,
			"naptan_code": self.naptan_code,
			"plus_bus_zone_ref": self.plus_bus_zone_ref,
			"stop_areas_ref": self.stop_areas_ref,
			"stop_start_date": self.stop_start_date,
		}

		# 2. Extract and "prefix" nested fields to keep them organized
		# From Descriptor
		flat_data.update({f"desc_{k}": v for k, v in asdict(self.descriptor).items()})

		# From Place
		flat_data.update({f"place_{k}": v for k, v in asdict(self.place).items()})

		# From Stop
		flat_data.update({f"stop_{k}": v for k, v in asdict(self.stop).items()})

		return flat_data

@dataclass
class Stop_Area:
	stop_area_code : str
	name : str
	nptg_authority_ref : str
	stop_area_type : str
	longitude : float
	latitude : float


def parse_nptg(file_path : str) -> tuple[list[Region], list[Authority], list[District], list[Locality]]:
	tree = ET.parse(file_path)
	root = tree.getroot()
	
	# for sub in root:
	# 	print(sub.tag, sub.attrib, sub.text)

	def process_nptg_Region_section(root : ET.Element) -> tuple[list[Region], list[Authority], list[District]]:
		# loop_p(root, 1, 6)
		NAME_SPACE : str = "{http://www.naptan.org.uk/}"

		def sub_extract_item(element_node : ET.Element, TAGS_LIST : list[str]) -> list[str]:
			data_list : list[str] = list()

			for tags in TAGS_LIST:
				result = element_node.find(f"{NAME_SPACE}{tags}")
				if (result is not None and result.text is not None):
					data_list.append(result.text)

			return data_list

		def sub_add_Region(
			element_node : ET.Element,
			target_list : list[Region]
		) -> None:
			TAGS_REGION_NODE : list[str] = ["RegionCode", "Name", "Country"]
			local_data = sub_extract_item(element_node=element_node, TAGS_LIST=TAGS_REGION_NODE)
			target_list.append(Region(local_data[0], local_data[1], local_data[2]))

		def sub_add_Authority(
			element_node : ET.Element,
			target_list : list[Authority],
			region_parent_index : int
		) -> None:
			TAGS_AUTHORITY_NODE : list[str] = ["AdministrativeAreaCode", "AtcoAreaCode", "Name", "ShortName", "National"]
			local_data = sub_extract_item(element_node=element_node, TAGS_LIST=TAGS_AUTHORITY_NODE)
			auth =  Authority(
					local_data[0],
					local_data[1],
					local_data[2],
					local_data[3],
					bool(int(local_data[4])),
					region_parent_index)
			
			prefix_list = element_node.find(f"{NAME_SPACE}NaptanPrefixes")
			if (prefix_list is not None):
				for prefix in prefix_list:
					if (prefix.text is None): continue
					auth.alpha_prefix_list.append(prefix.text)

			target_list.append(auth)
					
		def sub_add_District(
			element_node : ET.Element,
			target_list : list[District],
			authority_parent_index : int
		) -> None:
			TAGS_DISTRICT_NODE : list[str] = ["NptgDistrictCode", "Name"]
			local_data = sub_extract_item(element_node=element_node, TAGS_LIST=TAGS_DISTRICT_NODE)
			target_list.append(District(local_data[0], local_data[1], authority_parent_index))

		region_list : list[Region] = list()
		region_index : int = 0

		authority_list : list[Authority] = list()
		authority_index : int = 0

		district_list : list[District] = list()
		district_index : int = 0

		for region_section in root:
			sub_add_Region(region_section, region_list)

			authority_section = region_section.find(f"{NAME_SPACE}AdministrativeAreas")
			if (authority_section is None): continue

			for authority_node in authority_section:
				sub_add_Authority(authority_node, authority_list, region_index)
				region_list[-1].admin_areas_list.append(authority_index)
				
				authority_index += 1

				district_section = authority_node.find(f"{NAME_SPACE}NptgDistricts")
				if (district_section is None): continue

				for district_node in district_section:
					sub_add_District(district_node, district_list, authority_index - 1)
					authority_list[-1].nptg_districts_list.append(district_index)

					district_index += 1

			region_index += 1


		# for i in region_list:
		# 	print(i, "\n")

		# for i in authority_list:
			# print(i, "\n")

		# for i in district_list:
		# 	print(i, "\n")

		return (region_list, authority_list, district_list)

	def process_nptg_Locality_section(root : ET.Element) -> list[Locality]:
		NAME_SPACE : str = "{http://www.naptan.org.uk/}"

		TAGS_LOCALITY_NODE : list[str] = [
			"NptgLocalityCode", "ParentNptgLocalityRef", "AdministrativeAreaRef",
			"NptgDistrictRef", "SourceLocalityType"
		]

		def sub_extract_item(
				element_node : ET.Element,
				TAGS_LIST : list[str]
				) -> list[str | None]:
			data_list : list[str | None] = list()

			for tags in TAGS_LIST:
				result = element_node.find(f"{NAME_SPACE}{tags}")
				if (result is not None and result.text is not None):
					data_list.append(result.text)
				else: 
					data_list.append(None)

			return data_list

		def sub_name_extract(element_node : ET.Element) -> tuple[str, str | None]:
			TAGS_NAME_NODE : list[str] = ["LocalityName", "QualifierName"]

			NS_LANG : str = "{http://www.w3.org/XML/1998/namespace}"

			field = element_node.find(f"{NAME_SPACE}Descriptor")
			loc_name = field.findtext(f"{NAME_SPACE}{TAGS_NAME_NODE[0]}")

			field = field.find(f"{NAME_SPACE}Qualify")
			if(field is None):
				return (loc_name, None)
			qua_name = field.findtext(f"{NAME_SPACE}{TAGS_NAME_NODE[1]}")

			# xml_print(qua_name)

			return (loc_name, qua_name)
		
		def sub_location_extract(element_node : ET.Element) -> tuple[str, str]:
			TAGS_LOCATION_NODE : list[str] = ["Longitude", "Latitude"]

			Location_field = element_node.find(f"{NAME_SPACE}Location")
			Translation_field = Location_field.find(f"{NAME_SPACE}Translation")

			long = Translation_field.findtext(f"{NAME_SPACE}{TAGS_LOCATION_NODE[0]}")
			lati = Translation_field.findtext(f"{NAME_SPACE}{TAGS_LOCATION_NODE[1]}")

			return (long, lati)

		locality_list : list[Locality] = list()

		for locality_section in root:
			locality_data = sub_extract_item(locality_section, TAGS_LOCALITY_NODE)

			name_data = sub_name_extract(locality_section)

			loc_data = sub_location_extract(locality_section)

			locality_list.append(
				Locality(
					locality_data[0],
					name_data[0],
					name_data[1],
					locality_data[1],
					locality_data[2],
					locality_data[3],
					locality_data[4],
					float(loc_data[0]),
					float(loc_data[1])
				)
			)

		# count = 0
		# for i in locality_list:
		# 	print(i, "\n")
		# 	count += 1

		# print(f"Count {count}")

		return locality_list


	region_list, authority_list, district_list = process_nptg_Region_section(root=root[0])
	locality_list = process_nptg_Locality_section(root=root[1])

	return (region_list, authority_list, district_list, locality_list)

def parse_naptan(file_path : str) -> tuple[list[Stop_Point], list[Stop_Area]]:

	def process_StopPoint(root : ET.Element) -> list[Stop_Point]:
		NAME_SPACE : str = "{http://www.naptan.org.uk/}"
		NS_DICT : dict[str, str] = {'n': 'http://www.naptan.org.uk/'}

		def sub_extract_item(
			element_node : ET.Element,
			TAGS_LIST : list[str],
			NAME_SPACE : str
			) -> list:

			data_list : list = list()

			for tags in TAGS_LIST:
				result = element_node.find(f"{NAME_SPACE}{tags}")
				if (result is not None and result.text is not None):
					data_list.append(result.text)
				else:
					data_list.append(None)

			return data_list

		def process_sp(ele : ET.Element) -> list:
			TAGS : list[str] = ["AtcoCode", "AdministrativeAreaRef"]
			OPT_TAGS : list[str] = ["NaptanCode", "PlusbusZoneRef", "StopAreaRef", "StartDate"]

			data_list = sub_extract_item(ele, TAGS, NAME_SPACE)

			for i in range(len(OPT_TAGS)):
				raw_date = ele.findtext(f'.//n:{OPT_TAGS[i]}', namespaces=NS_DICT)
			
				if(raw_date is not None):
					data_list.append(raw_date)
				else:
					data_list.append(None)

			return data_list
		
		def process_ds(ele : ET.Element) -> SP_descriptor:
			TAGS : list[str] = ["CommonName", "Landmark", "Street", "Indicator"]

			ds = ele.find(f"{NAME_SPACE}Descriptor")

			data_list = sub_extract_item(ds, TAGS, NAME_SPACE)

			return SP_descriptor(data_list[0], data_list[1], data_list[2], data_list[3])

		def process_pl(ele : ET.Element) -> SP_place:
			TAGS : list[str] = [
				"NptgLocalityRef", "Longitude", "Latitude"
			]

			data_list = []

			for tag in TAGS:
				raw_date = ele.findtext(f".//n:{tag}", namespaces=NS_DICT)
				data_list.append(raw_date)

			if data_list[1] == None or data_list[2] == None:
				data_list[1] = -999.999
				data_list[2] = -999.999
				data_list.append( True )
			else:
				data_list.append( False )

			return SP_place(
				data_list[0],
				float(data_list[1]),
				float(data_list[2]),
				data_list[3]
			)

		def process_st(ele: ET.Element) -> SP_stop:
			TAGS : list[str] = [
				"StopType", "BusStopType", "TimingStatus",
				"CompassPoint", "Degrees"
			]

			data_list = []

			for tag in TAGS:
				raw_date = ele.findtext(f".//n:{tag}", namespaces=NS_DICT)

				if(raw_date == ''):
					raw_date = None

				data_list.append(raw_date)

			# counter = 0
			# for i in data_list:

			# 	if (i == None):
			# 		counter += 1

			# if(counter == 6):
			# 	print(data_list)

			deg : int | None
			if (isinstance(data_list[4], str)):
				deg = int(data_list[4])
			else:
				deg = None

			return SP_stop(
				stop_type=		data_list[0],
				bus_stop_type=	data_list[1],
				timing_status=	data_list[2],
				compass_point=	data_list[3],
				degrees=		deg,
			)

		sp_list : list[Stop_Point] = []

		for i in range(len(root)):
			data_list = process_sp(root[i])

			sp_list.append(Stop_Point(
				atco_code=			data_list[0],
				nptg_authority_ref=	data_list[1],
				descriptor=			process_ds(root[i]),
				place=				process_pl(root[i]),
				stop=				process_st(root[i]),

				naptan_code=		data_list[2],
				plus_bus_zone_ref=	data_list[3],
				stop_areas_ref=		data_list[4],
				stop_start_date=	data_list[5]
			))

		# loop_lp(sp_list)

		return sp_list

	def process_StopArea(root : ET.Element) -> list[Stop_Area]:
		NS_DICT : dict[str, str] = {'n': 'http://www.naptan.org.uk/'}

		def process_sa(ele : ET.Element) -> Stop_Area:
			TAGS : list[str] = [
				"StopAreaCode", "Name", "AdministrativeAreaRef",
				"StopAreaType", "Longitude", "Latitude"
			]

			data_list = []

			for tag in TAGS:
				raw_data = ele.findtext(f".//n:{tag}", namespaces=NS_DICT)
				data_list.append(raw_data)

			return Stop_Area(
				stop_area_code=		data_list[0],
				name=				data_list[1],
				nptg_authority_ref=	data_list[2],
				stop_area_type=		data_list[3],
				longitude=			data_list[4],
				latitude=			data_list[5]
			)

		sa_list : list[Stop_Area] = []

		for ele in root:
			sa_list.append(process_sa(ele))

		return sa_list


	tree = ET.parse(file_path)
	root = tree.getroot()

	sp_list = process_StopPoint(root[0])
	sa_list = process_StopArea(root[1])

	return (sp_list, sa_list)

def sub_extract_item(
	element_node : ET.Element,
	TAGS_LIST : list[str],
	NAME_SPACE : str
	) -> list:

	data_list : list = list()

	for tags in TAGS_LIST:
		result = element_node.find(f"{NAME_SPACE}{tags}")
		if (result is not None and result.text is not None):
			data_list.append(result.text)
		else:
			data_list.append(None)

	return data_list

def unzip_file(folder_path: str) -> None:
	for file in os.listdir(folder_path):
		if file.endswith(".zip"):
			file_path = os.path.join(folder_path, file)
			with zipfile.ZipFile(file_path, 'r') as zip_ref:
				zip_ref.extractall(folder_path)
			print(f"\tUnzipped: {file_path}")	

def download_bus_timetable(operator : str, data_json : dict) -> None:

	PATH : str = os.path.join(SCRIPT_DIR, "DATA", "BusTimeTable")

	results = data_json.get('results', [])
	for i, record in enumerate(results, 1):
		URL = record.get('url')
		TYPE = record.get('extension')
		download_file(URL, f"{operator}_{i}", TYPE, PATH) 


def xml_print(node):
	print(node.tag, node.attrib, node.text)
	# print(node.attrib, node.text)

def loop_p(node, counter, depth, limit):
	i = 0
	for child in node:
		print(f"{counter} loop")
		# xml_print(child)
		print(child)
		if(depth > 0):
			loop_p(node=child, counter=counter+1, depth=depth-1, limit=-1)

		i += 1
		# if(counter == 1):
		# 	print("\n")

		if(i == limit and counter == 1):
			return
		
def loop_lp(l : list):
	for i in l:
		print(i)


def csv_save(file_name : str, item_list : list) -> None:
	if (item_list is None):
		return

	header = []
	for f in fields(item_list[0]):
		header.append(f.name)
		# print(f"{f.name}")

	with open(file_name, "w+", newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=header)

		writer.writeheader()

		for item in item_list:
			writer.writerow(asdict(item))

	print(f"Save data into {file_name}")

def csv_save_sp(file_name : str, item_list : list[Stop_Point]) -> None:
	if (item_list is None):
		return

	sample_dict = item_list[0].to_flat_dict()
	header = list(sample_dict.keys())

	# print(header)

	with open(file_name, "w+", newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=header)

		writer.writeheader()

		for item in item_list:
			writer.writerow(item.to_flat_dict())

	print(f"Save data into {file_name}")

def download_file(URL : str, file_name : str, file_type : str, file_path : str) -> str:
	os.makedirs(file_path, exist_ok=True)
	file_name = f"{file_path}/{file_name}.{file_type}"

	if (os.path.isfile(file_name)):
		print(f"\t[Download]\t{file_name} exist")
		return file_name

	print(f"\t[Download]\t{URL}")

	CHUNK_SIZE = 1024 * 1024  # 1 MB chunks

	with requests.get(URL, stream=True, timeout=300) as get_response:
		get_response.raise_for_status()

		total = int(get_response.headers.get("content-length", 0))
		downloaded = 0

		with open(file_name, "wb") as f:
			for chunk in get_response.iter_content(chunk_size=CHUNK_SIZE):
				if chunk:
					f.write(chunk)
					downloaded += len(chunk)
					if total:
						pct = downloaded / total * 100
						print(f"\r\t[Download]\t{downloaded // (1024*1024)} / {total // (1024*1024)} MB  ({pct:.1f}%)", end="", flush=True)

	return file_name

def main() -> None:
	# NPTG_URL : str =  "http://transport.scc.lancs.ac.uk/nptg/nptg.xml"
	# NAPTAN_URL : str = "http://transport.scc.lancs.ac.uk/nptg/naptan-full.xml"

	# UK Gov DfT official sources
	NPTG_URL : str = "https://naptan.api.dft.gov.uk/v1/nptg"
	NAPTAN_URL : str = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=xml"

	NPTG_FILE = download_file(NPTG_URL, "nptg", "xml", os.path.join(SCRIPT_DIR, "DATA"))
	NAPTAN_FILE = download_file(NAPTAN_URL, "naptan_full", "xml", os.path.join(SCRIPT_DIR, "DATA"))


	region_list, authority_list, district_list, locality_list = parse_nptg(NPTG_FILE)
	sp_list, sa_list = parse_naptan(NAPTAN_FILE)

	PATH : str = os.path.join(SCRIPT_DIR, "CSV_NPTG") + os.sep
	os.makedirs(PATH, exist_ok=True)

	csv_save(f"{PATH}region.csv", region_list)
	csv_save(f"{PATH}authority.csv", authority_list)
	csv_save(f"{PATH}district.csv", district_list)
	csv_save(f"{PATH}locality.csv", locality_list)

	csv_save_sp(f"{PATH}stop_point.csv", sp_list)

	csv_save(f"{PATH}stop_area.csv", sa_list)