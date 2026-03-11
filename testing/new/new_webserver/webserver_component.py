"""
webserver_component.py
======================
Reusable components for the Transport Application web server.
All dependencies are Python standard library only.
"""

import os
import threading
from datetime import datetime, timezone


class Server_Logger:
	LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server_log.log")

	def __init__(self):
		self._counter: int = 0
		self._lock = threading.Lock()
		self.log_level : list[str] = ["INFO", "WARN", "ERROR"]

	# ── Internal helpers ──────────────────────────────────────────────────────

	@staticmethod
	def _now() -> str:
		"""Return the current UTC time as an ISO-8601 string."""
		return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

	def _next(self) -> int:
		"""Increment and return the internal counter (call only while _lock is held)."""
		n = self._counter
		self._counter += 1
		return n

	def _level_write(self, level: str, tag: str, message: str) -> int:
		"""Format one log line and append it to the log file (thread-safe)."""

		with self._lock:
			id = self._next()
			line = f"[{id:>8d},{level:>6},{tag:>10},{self._now()}]\t{message}\n"
			with open(self.LOG_FILE, "a", encoding="utf-8") as f:
				f.write(line)

		return id
	
	def _event_level_write(
			self,
			level: str,
			tag: str,
			message: str,
			event_type : int,
			event_id : int = -1
		) -> int:
			
		"""Format one log line and append it to the log file (thread-safe)."""

		with self._lock:
			id = self._next()

			if (event_type == 1):
				line = f"[{id:>8d},{level:>6},{tag:>10},{self._now()}]\t[Create Event\t:{id:>8d}] {message}\n"
			elif (event_type == 2):
				line = f"[{id:>8d},{level:>6},{tag:>10},{self._now()}]\t[Continue Event\t:{event_id:>8d}] {message}\n"
			else:
				line = f"[{id:>8d},{level:>6},{tag:>10},{self._now()}]\t[Finish Event\t:{event_id:>8d}] {message}\n"

			with open(self.LOG_FILE, "a", encoding="utf-8") as f:
				f.write(line)

		return id

			
	# ── Public API ────────────────────────────────────────────────────────────

	def log_init(self, PORT: int) -> None:
		"""
		Create (or clear) server_log.log and reset the counter to 1.
		Writes the first entry: [START, <time>] Server start
		"""
		with self._lock:
			self._counter = 1
			# 'w' truncates (clears) the file if it already exists
			with open(self.LOG_FILE, "w", encoding="utf-8") as f:
				pass

		self._level_write(self.log_level[0], "START", f"Server start with port = {PORT}")

	def log_request(self, message : str) -> int:
		# return the value of counter as Event_id
		return self._event_level_write(self.log_level[0], "REQUEST", message, 1)

	def log_continue(self, message : str, event_id : int) -> None:
		self._event_level_write(self.log_level[0], "CONTINUE", message, 2, event_id)
		
	def log_response(self, message : str, event_id : int) -> None:
		self._event_level_write(self.log_level[0], "RESPONSE", message, 3, event_id)


# ── Data-transformation helpers ──────────────────────────────────────────────

def stop_to_node(stop: dict) -> dict:
	"""Convert a query.py stop dict to an API Node object."""
	return {
		"id":       stop["SP_atco_code"],
		"type":     "bus",
		"name":     stop["SP_name"],
		"location": {
			"lat":  stop["SP_latitude"],
			"long": stop["SP_longitude"]
		}
	}
