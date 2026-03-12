/**
 * @author Sam
 * @description API module for sending Transport requests to server and validating responses.
 * @ai Copilot assisted
 * 
 * specifically, exposes:
 * - requestMap(lat, long)
 * - requestMap(lat, long, options)
 * - requestRoutes(startNodeId, endNodeId, time)
 * - requestRoutes(startNodeId, endNodeId)
 * - now()
 * - clearNodeCache()
 * - requestLiveTracking(nodeId)
 * - requestTimetable(time, nodeId)
 * - requestTimetablesForNodes(time, nodeIds)
 * - requestServicesThroughNode(nodeId)
 * - requestWeather(lat, long)
 * see individual function docs for details.
 */
import axios from "https://cdn.jsdelivr.net/npm/axios@1.6.8/+esm";

const MIN_API_VERSION = "1.2.0";
// increment when changes made to Node data structure
const MIN_API_NODE_CACHE_VERSION = 3;
export const API_VERSION = "1.2.0";
// regex to validate version in format "major.minor.patch", e.g. "1.0.0"
const VERSION_REGEX = /^\d+\.\d+\.\d+$/;
// flag to track if server has requested slower requests due to DoS protection
// note that this is a client-side flag so is not *trustworthy*
// but aims to prevent overwhelming server from legitimate clients
let serverEnforcedCooldown = false;

// IndexedDB for caching node-map lookups
let dbCache = undefined;
const dbRequest = window.indexedDB.open(`lazy-nodes`, MIN_API_NODE_CACHE_VERSION);

function clearNodeStore(database) {
	return new Promise((resolve) => {
		try {
			const transaction = database.transaction(["nodes"], "readwrite");
			const nodeStore = transaction.objectStore("nodes");
			nodeStore.clear();

			transaction.oncomplete = () => {
				console.log("Node cache cleared manually.");
				resolve(true);
			};
			transaction.onerror = (error) => {
				console.warn("Failed to clear node cache:", error);
				resolve(false);
			};
		} catch (error) {
			console.warn("Failed to clear node cache:", error);
			resolve(false);
		}
	});
}

/**
 * Clears cached map nodes from IndexedDB for manual testing.
 * @returns {Promise<boolean>} true if cache was cleared, false if unavailable or failed
 */
export async function clearNodeCache() {
	if (!window.indexedDB) {
		return false;
	}

	if (dbCache === null) {
		return false;
	}

	if (dbCache) {
		return clearNodeStore(dbCache);
	}

	return new Promise((resolve) => {
		const onSuccess = async () => {
			try {
				const cleared = await clearNodeStore(dbRequest.result);
				resolve(cleared);
			} catch {
				resolve(false);
			}
		};

		const onError = () => resolve(false);

		dbRequest.addEventListener("success", onSuccess, { once: true });
		dbRequest.addEventListener("error", onError, { once: true });
	});
}

if (typeof window !== "undefined") {
	window.clearTransportNodeCache = clearNodeCache;
}

function resolveApiBaseUrl() {
	if (typeof window !== "undefined" && window.location) {
		const { protocol, hostname, port } = window.location;
		if (protocol && hostname) {
			// In development the dev server proxies /api, but the backend runs on
			// :8080, so keep the explicit port only when running locally.
			const localPort = port || (protocol === "https:" ? "443" : "80");
			const isLocal = hostname === "localhost" || hostname === "127.0.0.1";
			return isLocal
				? `${protocol}//${hostname}:8080`
				: `${protocol}//${hostname}`;
		}
	}
	// default to localhost for non-browser environments, e.g. testing
	return "http://localhost:8080";
}

const api = axios.create({
	baseURL: resolveApiBaseUrl(),
	timeout: 15000 // max 15 seconds for long requests, e.g. routing
});

// intercept requests to implement server-enforced cooldown if needed
function apiCooldown() {
	return new Promise((resolve) => {
		setTimeout(resolve, 4000);
		serverEnforcedCooldown = false;
	});
}

/**
 * Checks if server version is acceptable - assuming server version formatted correctly.
 * @param serverVersion version in format "major.minor.patch", e.g. "1.0.0"
 * @returns true only if server version above minimum required version
 */
function isVersionAcceptable(serverVersion) {
	// simple version comparison: check if server version is above minimum required version
	const [sMajor, sMinor, sPatch] = serverVersion.split(".").map(Number);
	const [mMajor, mMinor, mPatch] = MIN_API_VERSION.split(".").map(Number);
	if (sMajor > mMajor) return true;
	if (sMajor < mMajor) return false;
	if (sMinor > mMinor) return true;
	if (sMinor < mMinor) return false;
	return sPatch >= mPatch;
}

/**
 * @param version version string in format "major.minor.patch" returned "from server" (or undefined)
 * @returns true if version is in correct format
 */
function isVersionStringValid(version) {
	return typeof version === "string" && VERSION_REGEX.test(version);
}

/**
 * Sends given request to server, validates response, and returns it if valid.
 * Valid responses are 200s with correct API version and a response-type field.
 * @return a promise that resolves to response data if valid, or rejects with error if invalid or if server returns error status.
 * @throws error (with statusCode attribute) if server returns non-200 status, if API version is invalid or unacceptable, or if response is missing required fields.
 * @logs any errors to console before rethrowing for caller to handle.
 */
function serverRequest(request, requestConfig = undefined) {
	
	// if server has requested slower requests, wait for cooldown before 
	// sending next request
	if (serverEnforcedCooldown) {
		console.warn("Stalling one request for server cooldown.");
		// re-call serverRequest after cooldown
		return apiCooldown().then(() => serverRequest(request));
	}
	
	// send request to server and validate response
	return api.get(request, requestConfig)
		.then((response) => {
			const data = response.data;
			// server sent bad response?
			// validation: all response must include valid API version
			if (!isVersionStringValid(data["api-version"])) {
				throw new Error("Invalid API version string: " + data["api-version"]);
			}
			else if (data["api-version"] !== API_VERSION) {
				// acceptable API version is allowed, otherwise reject
				if (isVersionAcceptable(data["api-version"])) {
					console.warn("API version mismatch: expected " + API_VERSION + ", got " + data["api-version"] + ". Proceeding.");
				} else {
					throw new Error("API version mismatch: expected " + API_VERSION + ", got " + data["api-version"]);
				}
			}

			// validation: all responses must include response-type field
			if (data["response-type"] === undefined) {
				throw new Error("Invalid response: missing response-type");
			} 

			// server response was valid and not an error
			// do not return api version to caller, 
			// as it is for internal use only
			delete data["api-version"];
			return data;
		})
		// log any errors and rethrow to be handled by caller
		.catch((error) => {
			console.error("API request error:", error);
			const statusCode = error?.response?.status ?? error.statusCode;
			if (statusCode === 429) {
				console.warn("Received 429 Too Many Requests. Enforcing cooldown for next request.");
				serverEnforcedCooldown = true;
			}
			// statusCode is 500 if response was *invalid* in terms
			// of formatting (but the server didn't return an error status)
			// also, network errors here
			error.statusCode = statusCode ?? 500;
			// each api request handles statuses differently, so propagate
			throw error;
		});
}

// respond if local cache unavailable
dbRequest.addEventListener("error", () => {
	console.error("Node cache failed to open, unavailable for this session.");
	dbCache = null;
});

// success handler signifies that the database opened successfully
dbRequest.addEventListener("success", () => {
	console.log("Node cache opened successfully");
	// store reference to database in global variable
	dbCache = dbRequest.result;
});

// cache not set up yet, or old version
dbRequest.addEventListener("upgradeneeded", (e) => {
  	// get the new database
	dbCache = e.target.result;

	// Create an objectStore in our database to store notes and an auto-incrementing key
	// An objectStore is similar to a 'table' in a relational database
	const nodeCache = dbCache.createObjectStore("nodes", {
		keyPath: "id",
		autoIncrement: false
	});

	// node cache stores lazy-load nodes as per API spec
	nodeCache.createIndex("name", "name", { unique: false });
	nodeCache.createIndex("type", "type", { unique: false });
	nodeCache.createIndex("lat", "location.lat", { unique: false });
	nodeCache.createIndex("long", "location.long", { unique: false });

	console.log("Node-cache setup complete");
});


/**
 * Checks local node cache for map data and returns it if
 * 1) it exists
 * 2) it is valid (not corrupted, correct API version)
 * 3) it is recent enough
 * Invalidates cache if it fails.
 * @return {Array of Node objects} if cache valid, otherwise null
 */
function checkMapCache() {
	if (!dbCache) {
		return Promise.resolve(null);
	}

	return new Promise((resolve) => {
		const readTx = dbCache.transaction(["nodes"], "readonly");
		const nodeStore = readTx.objectStore("nodes");

		let countLoaded = false;
		let metaLoaded = false;
		let countResult = 0;
		let metaResult = undefined;

		const finishValidation = () => {
			if (!countLoaded || !metaLoaded) {
				return;
			}

			let invalid = false;
			if (countResult === 0) {
				console.warn("Node cache empty or corrupted.");
				invalid = true;
			}

			if (!metaResult || !metaResult.timestamp) {
				console.warn("Cache metadata missing or corrupted.");
				invalid = true;
			} else if (Date.now() - metaResult.timestamp > 12 * 60 * 60 * 1000) {
				console.warn("Node cache is stale. Last updated at " + new Date(metaResult.timestamp).toLocaleString());
				invalid = true;
			}

			if (invalid) {
				const clearTx = dbCache.transaction(["nodes"], "readwrite");
				clearTx.objectStore("nodes").clear();
				clearTx.oncomplete = () => resolve(null);
				clearTx.onerror = () => resolve(null);
				console.warn("Invalid node cache, cleared.");
				return;
			}

			const allReq = nodeStore.getAll();
			allReq.onerror = () => resolve(null);
			allReq.onsuccess = (event) => {
				const records = event.target.result
					.filter(entry => entry.id !== "cache-meta")
					.map(entry => ({
						id: entry.id,
						name: entry.name,
						type: entry.type,
						location: {
							lat: entry.lat,
							long: entry.long
						}
					}));

				if (records.length === 0) {
					console.warn("Node cache contained no node records.");
					resolve(null);
					return;
				}

				console.log(`Looked up node cache successfully: ${records.length} nodes retrieved.`);
				resolve(records);
			};
		};

		const countReq = nodeStore.count();
		countReq.onerror = () => {
			console.warn("Error counting cached nodes.");
			resolve(null);
		};
		countReq.onsuccess = (event) => {
			countLoaded = true;
			countResult = event.target.result;
			finishValidation();
		};

		const metaReq = nodeStore.get("cache-meta");
		metaReq.onerror = (event) => {
			console.warn("Error retrieving cache metadata:", event.target.error);
			resolve(null);
		};
		metaReq.onsuccess = (event) => {
			metaLoaded = true;
			metaResult = event.target.result;
			finishValidation();
		};
	});
}

/**
 * Called when new map data is fetched from server, to update local cache.
 * Overwrites existing cache. Logs errors but does not throw.
 * @param {Array of Node objects} data 
 */
function updateMapCache(data) {
	if (!dbCache) {
		return;
	}

	// open a read/write db transaction, ready for adding the data
    const transaction = dbCache.transaction(["nodes"], "readwrite");
	// get the object store for nodes, where we want to add the data
    const nodeCache = transaction.objectStore("nodes");

	// caching not available
	transaction.onerror = (error) => {
		console.warn("Couldn't write to node cache:", error);
	};

	// clear old cache and replace with latest map nodes
	nodeCache.clear();

	// add the nodes to the cache, overwriting identical IDs
	data.forEach(node => {
		let entry = {
			id: node["id"],
			name: node["name"],
			type: node["type"],
			lat: node["location"]["lat"],
			long: node["location"]["long"]
		};
		nodeCache.put(entry);
	});

	// add an arbitrary record to track cache update time, for cache invalidation purposes
	let metaEntry = {
		id: "cache-meta",
		timestamp: Date.now()
	};
	nodeCache.put(metaEntry);
}	

/**
 * Fetches all transport nodes (stops/stations) and accompanying weather/general data (see API spec).
 * Fetch *may* be sent to the server or be fetched from a local cache.
 * @param lat current latitude of user (for weather data), Number with high precision
 * @param long current longitude of user (for weather data), Number with high precision
 * @param {{forceRefresh?: boolean, clearCacheBeforeRead?: boolean}} options cache control options (optional)
 * @return promise that resolves to null if request was invalid (e.g. bad params) or rejected by server
 * @return promise that resolves to an object containing nodes and information as per API spec
 * @throws error if server has internal error (that is not the client's fault)
 * Note that the internal server error doesn't mean the server is down; could be a transmission error etc.
 */
export async function requestMap(lat, long, options = {}) {
	try {
		const { forceRefresh = false, clearCacheBeforeRead = false } = options;

		if (clearCacheBeforeRead) {
			await clearNodeCache();
		}

		// check cache first - if valid, return cached nodes, otherwise fetch from server
		const cached = forceRefresh ? null : await checkMapCache();
		if (cached && !forceRefresh) {
			console.log("Using cached map data.");

			
			// request just the general info metadata
			const data = await serverRequest("/map?cached=true&lat=" + lat + "&long=" + long);
			
			return {nodes: cached, meta: data};
		}

		console.log("Requesting map data from server.");
		const data = await serverRequest("/map?lat=" + lat + "&long=" + long);
		if (data["response-type"] !== "map") {
			throw new Error("Invalid response type for map lookup: " + data["response-type"]);
		}

		// update cache with new data for next time
		updateMapCache(data["nodes"]);
		return data;
	} catch (error) {
		console.error("Error fetching map data:", error);
		// as per API spec:
		// error status may be: 429
		// or 400/414 if param format is bad
		if (error.statusCode === 400 
			|| error.statusCode === 414 
			|| error.statusCode === 429) {
			return null;
		}
		throw error;
	}
}

/**
 * Get the timetables for a GROUP of specified nodes *from* the given time
 * SEE API spec for details of timetable format.
 * @param {String} time in format "YYYY-MM-DDTHH:mm:ssZ" in UTC (see now()) - defaults to now if unspecified
 * @param {non-empty Array of Strings} nodeIds to fetch timetables for - not too many please, in practice max like 10 at a time
 * @return null if request invalid (e.g. bad params) or rejected by server (e.g. DoS protection)
 * @return promise resolving to an object key-value dictionary of nodeIds and their Timetable objects, see API spec
 * @throws error if server has internal error (that is not the client's fault)
 * @logs IDs of any nodes that were excluded from the response due to invalid node IDs or server issues, for debugging purposes
 * Note that the server does not include timetable entries before {time} in output
 * Note that the server may exclude some nodes from the response if they are invalid or if there are server issues, 
 * but will still return a valid response with the remaining nodes' timetables.
 * Note that ALL NODES IN THE REQUEST MAY BE EXCLUDED, in which case this returns null. 
 */
export async function requestTimetablesForNodes(time, nodeIds) {
	if (nodeIds.length === 0) {
		console.warn("Requesting timetables for empty node ID list.");
		return null;
	}
	if (time === undefined) {
		time = now();
	}

	try {
		const data = await serverRequest("/timetable?time=" + time + "&nodes=" + nodeIds.join(","));
		if (data["response-type"] !== "timetable") {
			throw new Error("Invalid response type for timetables lookup: " + data["response-type"]);
		}

		if (data["excluded"] && data["excluded"].length > 0) {
			console.warn("Excluded nodes:", data["excluded"]);
		}

		// if all nodes were excluded, return null to indicate request 
		// was invalid or rejected, rather than returning empty timetable data
		if (data["excluded"].length === nodeIds.length) {
			return null;
		}

		return data["timetables"];
	} catch (error) {
		console.error("Error fetching timetables data:", error);
		if (error.statusCode === 400 
			|| error.statusCode === 414 
			|| error.statusCode === 429) {
			return null;
		}
		throw error;
	}
}

/**
 * see requestTimetablesForNodes() for details - this is a wrapper for requesting timetable for a single node.
 * @param {String} nodeId ID of single node
 * @return time Timetable object for the given node. (NOT A NODEID DICTIONARY)
 * @return null if request params invalid
 */
export async function requestTimetable(time, nodeId) {
	const dict = await requestTimetablesForNodes(time, [nodeId]);
	if (dict === null) return null;
	else if (dict[nodeId] === undefined) return null;
	else return dict[nodeId];
}

/**
 * Requests the services and their routes that pass through the given stop, e.g. the 100's route, 1A's route...
 * See API spec for details of service [] format.
 * Note that service array *may* be empty, but generally should not be.
 * @param {String} nodeId
 * @return a promise that resolves to null if request was invalid (e.g. bad nodeId) or rejected by server (e.g. DoS protection)
 * @return a promise that resolves to an array of service objects (see API spec))
 * @throws error if server has internal error (that is not the client's fault)
 */
export async function requestServicesThroughNode(nodeId) {
	const requestServiceData = async () => {
		const data = await serverRequest("/services/" + nodeId, { timeout: 30000 });
		if (data["response-type"] !== "service") {
			throw new Error("Invalid response type for services lookup: " + data["response-type"]);
		}
		else if (data["node"] !== nodeId) {
			throw new Error("Services response node mismatch: expected " + nodeId + ", got " + data["node"]);
		}
		return data["services"];
	};

	try {
		return await requestServiceData();
	} catch (error) {
		if (error?.code === "ECONNABORTED") {
			console.warn("Service request timed out, retrying once:", nodeId);
			try {
				return await requestServiceData();
			} catch (retryError) {
				error = retryError;
			}
		}

		console.error("Error fetching services data:", error);
		// as per API spec:
		// error status may be: 429
		// or 404 if param format is bad or nodeId not found
		if (error.statusCode === 404
			|| error.statusCode === 429
			|| error?.code === "ECONNABORTED") {
			return null;
		}
		// server error
		throw error;
	}
}

/**
 * Returns current datetime string formatted according to API spec.
 * @return "YYYY-MM-DDTHH:mm:ssZ" (e.g. "2024-06-01T12:00:00Z")
 * Note that this time is therefore in UTC - convert to display if needed.
 */
export function now() {
	const now = new Date();
	const year = now.getUTCFullYear();
	const month = String(now.getUTCMonth() + 1).padStart(2, "0");
	const day = String(now.getUTCDate()).padStart(2, "0");
	const hours = String(now.getUTCHours()).padStart(2, "0");
	const minutes = String(now.getUTCMinutes()).padStart(2, "0");
	const seconds = String(now.getUTCSeconds()).padStart(2, "0");
	return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}Z`;
}


/**
 * Get live tracking data for vehicles approaching a given node
 * @return Returns VEHICLES [] as per API spec, which may be empty
 * @return null if request invalid (e.g. bad nodeId) or rejected by server (e.g. DoS protection)
 * @throws error if server has internal error (that is not the client's fault)
 */
export async function requestLiveTracking(nodeId) {
	try {
		const data = await serverRequest("/tracking/" + nodeId);
		if (data["response-type"] !== "tracking" && data["response-type"] !== "live-tracking") {
			throw new Error("Invalid response type for live tracking lookup: " + data["response-type"]);
		}
		if (data["node"] !== nodeId) {
			throw new Error("Live tracking response node mismatch: expected " + nodeId + ", got " + data["node"]);
		}
		return data["vehicles"];
	} catch (error) {
		console.error("Error fetching live tracking data:", error);
		// 404 - invalid node ID
		// 429 - DoS protection
		if (error.statusCode === 404  
			|| error.statusCode === 429) {
			return null;
		}
		// server is sad
		throw error;
	}
}

/**
 * Returns a selection of possible routes between given start and end nodes at given time.
 * @param {String} startNodeId ID of source node
 * @param {String} endNodeId ID of destination node
 * @param {String} time time of departure in format "YYYY-MM-DDTHH:mm:ssZ" in UTC (see now())
 * @param time is optional - if not provided, defaults to current time (now())
 * @return promise that resolves to null if request was invalid (e.g. bad params) or rejected by server
 * @return promise that resolves to an ARRAY OF ROUTE OBJECTS as per API spec
 * @throws error if server has internal error (that is not the client's fault) or could not find route in reasonable time
 */
export async function requestRoutes(startNodeId, endNodeId, time) {

	if (time === undefined) {
		time = now();
	}

	try {
		const data = await serverRequest("/routes?source=" + startNodeId + "&dest=" + endNodeId + "&time=" + time);
		if (data["response-type"] !== "route") {
			throw new Error("Invalid response type for routes lookup: " + data["response-type"]);
		}
		else if (data["source"] !== startNodeId || data["destination"] !== endNodeId) {
			throw new Error("Response source/destination mismatch: expected " + startNodeId + " to " + endNodeId + ", got " + data["source"] + " to " + data["destination"]);
		}
		return data["routes"];
	} catch (error) {
		console.error("Error fetching routes:", error);
		// as per API spec:
		// error status may be: 429 DoS
		// or 404 if nodes not found / misformatted or time misformmatted
		// 404 if no route found because routing given nodes makes no sense
		//  (e.g. identical source and destination)
		// 422 if time param is unreasonable (e.g. 1970 or 2029)
		if (error.statusCode === 404 
			|| error.statusCode === 422 
			|| error.statusCode === 429) {
			return null;
		}
		throw error;
	}
}

/**
 * Requests weather information specifically for a given location.
 * See WeatherService in API specification.
 * @param {Number} lat latitude of location to query
 * @param {Number} long longitude of location to query
 * @return weather object if available, or null if request was invalid/rejected
 * @throws error if server has internal error (not caused by invalid params)
 */
export async function requestWeather(lat, long) {
	try {
		const data = await serverRequest("/weather?lat=" + lat + "&long=" + long, { timeout: 10000 });
		if (data["response-type"] === "weather") {
			return data["weather"] ?? null;
		}

		// Compatibility fallback while backend route wiring settles.
		if (data["response-type"] === "map") {
			return data["information"]?.["weather"] ?? data["weather"] ?? null;
		}

		throw new Error("Invalid response type for weather lookup: " + data["response-type"]);
	} catch (error) {
		console.error("Error fetching weather data:", error);
		// as per API spec:
		// error status may be 400/414 for invalid lat/long, or 429 for request limiting
		if (error.statusCode === 400
			|| error.statusCode === 404
			|| error.statusCode === 414
			|| error.statusCode === 429) {
			return null;
		}

		throw error;
	}
}