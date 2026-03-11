import zipfile
import requests
import json
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def parse_bus_timetable( download : bool ) -> None:
	OPERATOR_LIST : list[str] = ["ARCT", "BLAC", "KLCO", "SCCU", "SCMY", "NUTT"]
	BUS_TIMETABLE_URL : str = "http://transport.scc.lancs.ac.uk/bus/times/"
	LANCASHIRE_ATCO_CODE : str = "250"

	for operator in OPERATOR_LIST:
		download_file(f"{BUS_TIMETABLE_URL}{operator}", operator, "json", os.path.join(SCRIPT_DIR, "DATA"))


	if (not download): return

	for operator in OPERATOR_LIST:
		file_name = os.path.join(SCRIPT_DIR, "DATA", f"{operator}.json")
		with open(file_name, "r", encoding="utf-8") as f:
			data_json = json.load(f)

		results = data_json.get("results", [])
		filtered_results = []
		for record in results:
			admin_areas = record.get("adminAreas", [])
			if any(
				area.get("atco_code") == LANCASHIRE_ATCO_CODE
				or area.get("name") == "Lancashire"
				for area in admin_areas
			):
				filtered_results.append(record)

		# Only download records that include Lancashire in adminAreas.
		data_json["results"] = filtered_results
		download_bus_timetable(operator, data_json)

	unzip_file(os.path.join(SCRIPT_DIR, "DATA", "ZIP_XML"))

	

def unzip_file(folder_path: str) -> None:
	for file in os.listdir(folder_path):
		if file.endswith(".zip"):
			file_path = os.path.join(folder_path, file)
			with zipfile.ZipFile(file_path, 'r') as zip_ref:
				zip_ref.extractall(folder_path)
			print(f"\tUnzipped: {file_path}")	

def download_bus_timetable(operator : str, data_json : dict) -> None:

	PATH : str = os.path.join(SCRIPT_DIR, "DATA", "ZIP_XML")

	results = data_json.get('results', [])
	for i, record in enumerate(results, 1):
		URL = record.get('url')
		TYPE = record.get('extension')
		download_file(URL, f"{operator}_{i}", TYPE, PATH) 

def download_file(URL : str, file_name : str, file_type : str, file_path : str) -> str:
	file_name = f"{file_path}/{file_name}.{file_type}"

	if (os.path.isfile(file_name)):
		print(f"\t[Download]\t{file_name} exist")
		return file_name

	get_response = requests.get(URL)

	print(f"\t[Download]\t{URL}")

	with open(file_name, "wb") as f:
		for chunk in get_response.iter_content(chunk_size=1024):
			if chunk: # filter out keep-alive new chunks
				f.write(chunk)

	return file_name


def main() -> None:
	parse_bus_timetable(True)