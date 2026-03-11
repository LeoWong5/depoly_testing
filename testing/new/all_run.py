import sys
import os
import time
import threading
import socket
import subprocess
import platform

# Get the absolute path to the folder containing your code
# (e.g., the folder where your 'preprocessor.py' lives)
# Get the directory where THIS script is located
base_dir = os.path.dirname(os.path.abspath(__file__))

# Build absolute paths relative to the script location
# Assuming 'new_database', etc., are siblings to the current folder's parent
paths = [
	os.path.join(base_dir, ".", "new_database"),
	os.path.join(base_dir, ".", "new_preprocessor"),
	os.path.join(base_dir, ".", "new_webserver")
]

for p in paths:
	normalized_p = os.path.normpath(p) # Cleans up the '..'
	if normalized_p not in sys.path:
		sys.path.append(normalized_p)

# Now import using the module names only
import db_run
import pre_run
import webserver # Changed from webserver.py


def get_local_ip() -> str:
	"""Return the machine's LAN IP address (cross-platform)."""
	try:
		# Trick: connect to an external address so the OS picks the right outbound interface.
		# No data is actually sent.
		with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
			s.connect(("8.8.8.8", 80))
			return s.getsockname()[0]
	except Exception:
		return "127.0.0.1"


def open_firewall(port: int) -> None:
	"""Open a TCP port in the system firewall (Linux ufw/iptables or Windows netsh)."""
	system = platform.system()
	try:
		if system == "Linux":
			# Prefer ufw if available, otherwise fall back to iptables
			ufw_available = subprocess.call(
				["which", "ufw"],
				stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
			) == 0
			if ufw_available:
				subprocess.run(["sudo", "ufw", "allow", f"{port}/tcp"], check=True)
				print(f"[Firewall] ufw: allowed TCP port {port}")
			else:
				subprocess.run(
					["sudo", "iptables", "-I", "INPUT", "-p", "tcp", "--dport", str(port), "-j", "ACCEPT"],
					check=True
				)
				print(f"[Firewall] iptables: allowed TCP port {port}")
		elif system == "Windows":
			subprocess.run([
				"netsh", "advfirewall", "firewall", "add", "rule",
				f"name=TransportApp-{port}",
				"protocol=TCP", "dir=in",
				f"localport={port}",
				"action=allow"
			], check=True)
			print(f"[Firewall] Windows Firewall: allowed TCP port {port}")
		else:
			print(f"[Firewall] Unsupported OS '{system}', skipping firewall rule.")
	except subprocess.CalledProcessError as e:
		print(f"[Firewall] WARNING: could not open port {port}: {e}")
	except FileNotFoundError as e:
		print(f"[Firewall] WARNING: firewall tool not found: {e}")


def print_access_info(port: int) -> None:
	"""Print the LAN URL that other devices can use to reach the webserver."""
	ip = get_local_ip()
	print(f"\n{'='*60}")
	print(f"  Webserver accessible at:")
	print(f"    http://{ip}:{port}/")
	print(f"{'='*60}\n")


def db_exists() -> bool:
	"""Return True if the database file exists and is non-empty."""
	db_path = os.path.join(base_dir, "new_database", "nptg_naptan.db")
	return os.path.isfile(db_path) and os.path.getsize(db_path) > 0

def _step(label: str):
	print(f"\n{'='*60}")
	print(f"  STEP: {label}")
	print(f"{'='*60}")

def preprocessor():
	_step("1/3 — Preprocessor (download + parse + filter)")
	try:
		pre_run.main()
		print("[OK] pre_run finished")
	except Exception as e:
		print(f"[ERROR] pre_run failed: {e}")
		import traceback; traceback.print_exc()
		return
	
def database():
	_step("2/3 — Database (create schema + load + test)")
	try:
		db_run.main()
		print("[OK] db_run finished")
	except Exception as e:
		print(f"[ERROR] db_run failed: {e}")
		import traceback; traceback.print_exc()
		return

def webserver_run():
	_step("3/3 — Webserver")
	port = 8080
	open_firewall(port)
	print_access_info(port)
	# Run webserver in a daemon thread so it doesn't block this process.
	# Daemon=True means it will be killed automatically when the main process exits.
	server_thread = threading.Thread(target=webserver.main, daemon=True)
	server_thread.start()
	print("Webserver started in background thread. Press Ctrl+C to stop.")
	server_thread.join()  # wait for Ctrl+C / shutdown

def run():

	if (db_exists()):
		webserver_run()
	else:
		preprocessor()
		database()
		webserver_run()

	
	
if __name__ == "__main__":
	run()