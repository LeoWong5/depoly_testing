/**
 * @author Sam / walkersi
 * @description API map display functions based on Leaflet library and OpenStreetMap tiles
 * @ai Copilot generated with manual adjustments for error handling
 */

// default map settings
const DEFAULT_ZOOM = 13;
const LANCASTER_CENTER = { lat: 54.0476, long: -2.8015 };
const TILE_URL = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png";
const TILE_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors';

function resolveLeaflet(leafletLib) {
    if (leafletLib) {
        return leafletLib;
    }

    if (typeof window !== "undefined" && window.L) {
        return window.L;
    }

    throw new Error("Leaflet library is required");
}

/**
 * Initializes a Leaflet map in the given container element.
 * @param {HTMLElement} container where the map should be rendered 
 * @param {typeof import('leaflet')} [leafletLib] optional Leaflet library instance
 * @return {L.Map} the initialized Leaflet map instance
 * @throws {Error} if the map fails to initialize
 */
export function initializeMap(container, leafletLib) {
    try {
        if (!container) {
            throw new Error("Map container is required");
        }
        const L = resolveLeaflet(leafletLib);
        const map = L.map(container).setView([LANCASTER_CENTER.lat, LANCASTER_CENTER.long], DEFAULT_ZOOM);
        L.tileLayer(TILE_URL, {
            attribution: TILE_ATTRIBUTION,
        }).addTo(map);
        return map;
    } catch (error) {
        console.error("Failed to initialize map:", error);
        throw new Error("Map initialization failed");
    }
}  

/**
 * Adds a node to the given Leaflet map instance.
 * @param {L.Map} map the Leaflet map instance to add the node to
 * @param {Node object see API spec} node the node object (location, name, id)
 * @param {typeof import('leaflet')} [leafletLib] optional Leaflet library instance
 * @return {L.Marker} the created Leaflet marker instance for the node
 * @throws {Error} if the node fails to be added to the map
 */
export function addNodeToMap(map, node, leafletLib) {
    try {
        const L = resolveLeaflet(leafletLib);
        const marker = L.marker([node.location.lat, node.location.long]).addTo(map);
        marker.bindPopup(node.name);
        return marker;
    } catch (error) {
        console.error("Failed to add node to map:", error);
        throw new Error("Adding node to map failed");
    }
}

export function addAllNodesToMap(map, nodes, leafletLib) {
    if (!Array.isArray(nodes)) {
        throw new Error("Nodes must be an array");
    }
    return nodes.map(node => addNodeToMap(map, node, leafletLib));
}

/**
 * Requests browser geolocation permission and recenters the map to the user location.
 * @param {L.Map} map the Leaflet map instance to recenter
 * @param {{zoom?: number, timeout?: number, maximumAge?: number, enableHighAccuracy?: boolean}} [options]
 * @returns {Promise<{lat: number, long: number}>} resolves with the user coordinates
 */
export function centerMapOnUserLocation(map, options = {}) {
    if (!map) {
        return Promise.reject(new Error("Map instance is required"));
    }

    if (typeof navigator === "undefined" || !navigator.geolocation) {
        return Promise.reject(new Error("Geolocation is not supported"));
    }

    const {
        zoom = DEFAULT_ZOOM,
        timeout = 10000,
        maximumAge = 60000,
        enableHighAccuracy = true,
    } = options;

    return new Promise((resolve, reject) => {
        navigator.geolocation.getCurrentPosition(
            (position) => {
                const lat = position.coords.latitude;
                const long = position.coords.longitude;
                map.setView([lat, long], zoom);
                resolve({ lat, long });
            },
            (error) => {
                reject(error);
            },
            { enableHighAccuracy, timeout, maximumAge }
        );
    });
}