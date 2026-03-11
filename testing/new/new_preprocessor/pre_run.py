import nptg_preprocessor as nptg_p
import bus_preprocessor as bus_p
import download_unzip as down_zip
import filter as filter
import detector as detector

def _step(label: str):
	print(f"  --> {label}")

def main():
	_step("download_unzip")
	down_zip.main()
	_step("nptg_preprocessor")
	nptg_p.main()
	_step("bus_preprocessor")
	bus_p.main()
	_step("filter")
	filter.main()
	_step("detector")
	detector.main()
	

# if __name__=="__main__":
# 	main()