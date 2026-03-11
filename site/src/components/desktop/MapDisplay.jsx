import React, { useEffect, useRef, useState } from 'react'
import './MapDisplay.css'
import 'leaflet/dist/leaflet.css'
import L from 'leaflet'
import { initializeMap } from '../../../api/api-map.js'

const LANCASTER_CENTER = { lat: 54.0476, long: -2.8015 }
const DEFAULT_STOP_STYLE = {
  radius: 8,
  color: '#2563eb',
  fillColor: '#3b82f6',
  weight: 3,
  fillOpacity: 0.85,
}
const SELECTED_STOP_STYLE = {
  radius: 12,
  color: '#b91c1c',
  fillColor: '#ef4444',
  weight: 5,
  fillOpacity: 0.95,
}
const ROUTE_SOURCE_STYLE = {
  radius: 12,
  color: '#b91c1c',
  fillColor: '#ef4444',
  weight: 5,
  fillOpacity: 0.95,
}
const ROUTE_DESTINATION_STYLE = {
  radius: 12,
  color: '#15803d',
  fillColor: '#22c55e',
  weight: 5,
  fillOpacity: 0.95,
}
const ROUTE_PATH_PRIMARY_COLOR = '#dc2626'
const ROUTE_PATH_ALTERNATE_COLOR = '#60a5fa'
const SERVICE_PATH_COLORS = ['#ff0054', '#0057ff', '#00b894', '#ff7a00', '#7b2cff', '#00bcd4', '#e91e63', '#2ecc40', '#ff1744', '#2979ff']
const SERVICE_PATH_MIN_WEIGHT = 3.1
const SERVICE_PATH_WEIGHT_STEP = 0.14
const SERVICE_PATH_OUTLINE_COLOR = '#111827'
const SERVICE_PATH_OUTLINE_EXTRA = 1.2
const SERVICE_PATH_MAX_RENDER_POINTS = 1200
const SELECTED_STOP_ZOOM = 15
const INVALID_ZERO_COORDINATE_EPSILON = 1e-9

function isRenderableCoordinate(lat, long) {
  if (!Number.isFinite(lat) || !Number.isFinite(long)) {
    return false
  }

  if (lat < -90 || lat > 90 || long < -180 || long > 180) {
    return false
  }

  // Ignore known bad dataset coordinates that create routes towards (0, 0).
  if (Math.abs(lat) <= INVALID_ZERO_COORDINATE_EPSILON && Math.abs(long) <= INVALID_ZERO_COORDINATE_EPSILON) {
    return false
  }

  return true
}

function toCoordinatePairList(pathPoints) {
  if (!Array.isArray(pathPoints)) {
    return []
  }

  return pathPoints
    .map((entry) => [Number(entry?.lat), Number(entry?.long)])
    .filter(([lat, long]) => isRenderableCoordinate(lat, long))
}

function appendCoordinates(mergedCoordinates, nextCoordinates) {
  if (!Array.isArray(nextCoordinates) || nextCoordinates.length === 0) {
    return
  }

  if (mergedCoordinates.length === 0) {
    mergedCoordinates.push(...nextCoordinates)
    return
  }

  const [lastLat, lastLong] = mergedCoordinates[mergedCoordinates.length - 1]
  const [firstLat, firstLong] = nextCoordinates[0]
  const joinsAtBoundary = Math.abs(lastLat - firstLat) < 1e-9 && Math.abs(lastLong - firstLong) < 1e-9

  if (joinsAtBoundary) {
    mergedCoordinates.push(...nextCoordinates.slice(1))
  } else {
    mergedCoordinates.push(...nextCoordinates)
  }
}

function extractServicePathCoordinates(service) {
  const mergedCoordinates = []

  if (Array.isArray(service?.segments) && service.segments.length > 0) {
    service.segments.forEach((segment) => {
      appendCoordinates(mergedCoordinates, toCoordinatePairList(segment?.path))
    })
    return mergedCoordinates
  }

  const basePath = service?.path?.path
  if (!Array.isArray(basePath) || basePath.length === 0) {
    return []
  }

  const firstEntry = basePath[0]
  const isNestedPathObject = firstEntry && typeof firstEntry === 'object' && Array.isArray(firstEntry.path)

  if (isNestedPathObject) {
    basePath.forEach((pathSegment) => {
      appendCoordinates(mergedCoordinates, toCoordinatePairList(pathSegment?.path))
    })
    return mergedCoordinates
  }

  return toCoordinatePairList(basePath)
}

function simplifyServicePathCoordinates(coordinates) {
  if (!Array.isArray(coordinates) || coordinates.length <= SERVICE_PATH_MAX_RENDER_POINTS) {
    return coordinates
  }

  const targetInteriorCount = SERVICE_PATH_MAX_RENDER_POINTS - 2
  if (targetInteriorCount <= 0) {
    return [coordinates[0], coordinates[coordinates.length - 1]]
  }

  const interiorLength = coordinates.length - 2
  const step = Math.max(1, Math.ceil(interiorLength / targetInteriorCount))

  const simplified = [coordinates[0]]
  for (let index = 1; index < coordinates.length - 1; index += step) {
    simplified.push(coordinates[index])
  }

  const lastPoint = coordinates[coordinates.length - 1]
  const [lastLat, lastLong] = simplified[simplified.length - 1]
  if (Math.abs(lastLat - lastPoint[0]) > 1e-9 || Math.abs(lastLong - lastPoint[1]) > 1e-9) {
    simplified.push(lastPoint)
  }

  return simplified
}

export default function MapDisplay({
  selectedStopId,
  selectedStopSource,
  routeSourceId,
  routeDestinationId,
  selectedRoute,
  servicePaths,
  nodes,
  userLocation,
  weatherOverlay,
  onSelectStop,
  onMapPickStop,
  mapPickTarget,
  onMapCenterChange,
  centerSelectedStopRequest,
  centerUserLocationRequest,
}) {
  const mapElementRef = useRef(null)
  const mapInstanceRef = useRef(null)
  const nodeMarkersRef = useRef([])
  const markerByStopIdRef = useRef(new Map())
  const onSelectStopRef = useRef(onSelectStop)
  const onMapPickStopRef = useRef(onMapPickStop)
  const mapPickTargetRef = useRef(mapPickTarget)
  const userLocationMarkerRef = useRef(null)
  const servicePathLayerGroupRef = useRef(null)
  const routePathLayerGroupRef = useRef(null)
  const hasCenteredOnUserLocationRef = useRef(false)
  const [isMapReady, setIsMapReady] = useState(false)

  useEffect(() => {
    onSelectStopRef.current = onSelectStop
  }, [onSelectStop])

  useEffect(() => {
    onMapPickStopRef.current = onMapPickStop
  }, [onMapPickStop])

  useEffect(() => {
    mapPickTargetRef.current = mapPickTarget
  }, [mapPickTarget])

  useEffect(() => {
    if (!mapElementRef.current || mapInstanceRef.current) {
      return
    }

    const map = initializeMap(mapElementRef.current, L)
    mapInstanceRef.current = map
    setIsMapReady(true)

    const updateCenter = () => {
      const center = map.getCenter()
      onMapCenterChange?.({ lat: center.lat, long: center.lng })
    }

    updateCenter()
    map.on('moveend', updateCenter)
    map.on('click', () => {
      if (mapPickTargetRef.current) {
        return
      }
      onSelectStopRef.current?.(null)
    })

    return () => {
      map.off('moveend', updateCenter)
      map.off('click')
      nodeMarkersRef.current.forEach((marker) => marker.remove())
      nodeMarkersRef.current = []
      markerByStopIdRef.current = new Map()

      if (userLocationMarkerRef.current) {
        userLocationMarkerRef.current.remove()
        userLocationMarkerRef.current = null
      }

      if (servicePathLayerGroupRef.current) {
        servicePathLayerGroupRef.current.remove()
        servicePathLayerGroupRef.current = null
      }

      if (routePathLayerGroupRef.current) {
        routePathLayerGroupRef.current.remove()
        routePathLayerGroupRef.current = null
      }

      if (mapInstanceRef.current) {
        mapInstanceRef.current.remove()
        mapInstanceRef.current = null
      }

      setIsMapReady(false)
    }
  }, [])

  useEffect(() => {
    if (!mapInstanceRef.current) {
      return
    }

    if (!userLocation) {
      hasCenteredOnUserLocationRef.current = false
      return
    }

    if (!hasCenteredOnUserLocationRef.current) {
      mapInstanceRef.current.setView([userLocation.lat, userLocation.long])
      hasCenteredOnUserLocationRef.current = true
    }
  }, [userLocation])

  useEffect(() => {
    if (!mapInstanceRef.current) {
      return
    }

    if (!userLocation) {
      if (userLocationMarkerRef.current) {
        userLocationMarkerRef.current.remove()
        userLocationMarkerRef.current = null
      }
      return
    }

    if (!userLocationMarkerRef.current) {
      userLocationMarkerRef.current = L.marker(
        [userLocation.lat, userLocation.long],
        {
          icon: L.divIcon({
            className: 'user-location-icon',
            html: '<div class="user-location-ring"><div class="user-location-center"></div></div>',
            iconSize: [20, 20],
            iconAnchor: [10, 10],
          }),
          interactive: false,
        },
      ).addTo(mapInstanceRef.current)
      return
    }

    userLocationMarkerRef.current.setLatLng([userLocation.lat, userLocation.long])
  }, [userLocation])

  useEffect(() => {
    if (!mapInstanceRef.current || !Array.isArray(nodes)) {
      return
    }

    nodeMarkersRef.current.forEach((marker) => marker.remove())
    markerByStopIdRef.current = new Map()

    nodeMarkersRef.current = nodes.map((node) => {
      const stopKey = String(node.id)
      const marker = L.circleMarker(
        [node.location.lat, node.location.long],
        { ...DEFAULT_STOP_STYLE, bubblingMouseEvents: false },
      ).addTo(mapInstanceRef.current)
      marker.on('click', (event) => {
        if (event?.originalEvent) {
          L.DomEvent.stopPropagation(event.originalEvent)
        }

        if (mapPickTargetRef.current) {
          onMapPickStopRef.current?.(node.id)
          return
        }

        onSelectStopRef.current?.(node.id)
      })
      markerByStopIdRef.current.set(stopKey, marker)
      return marker
    })
  }, [nodes])

  useEffect(() => {
    const selectedStopKey = selectedStopId === null || selectedStopId === undefined
      ? null
      : String(selectedStopId)
    const routeSourceKey = routeSourceId === null || routeSourceId === undefined
      ? null
      : String(routeSourceId)
    const routeDestinationKey = routeDestinationId === null || routeDestinationId === undefined
      ? null
      : String(routeDestinationId)

    markerByStopIdRef.current.forEach((marker, stopId) => {
      let style = DEFAULT_STOP_STYLE
      if (stopId === routeDestinationKey) {
        style = ROUTE_DESTINATION_STYLE
      }
      if (stopId === routeSourceKey) {
        style = ROUTE_SOURCE_STYLE
      }
      if (!routeSourceKey && !routeDestinationKey && stopId === selectedStopKey) {
        style = SELECTED_STOP_STYLE
      }

      marker.setStyle(style)

      if (stopId === routeSourceKey || stopId === routeDestinationKey || stopId === selectedStopKey) {
        marker.bringToFront()
      }
    })
  }, [selectedStopId, routeSourceId, routeDestinationId, nodes])

  useEffect(() => {
    if (selectedStopId === null || selectedStopId === undefined || !mapInstanceRef.current) {
      return
    }

    const marker = markerByStopIdRef.current.get(String(selectedStopId))
    if (!marker) {
      return
    }

    const coordinates = marker.getLatLng()
    const currentZoom = mapInstanceRef.current.getZoom()
    const keepUserZoom = selectedStopSource === 'map' && currentZoom > SELECTED_STOP_ZOOM
    const nextZoom = keepUserZoom ? currentZoom : SELECTED_STOP_ZOOM

    mapInstanceRef.current.setView([coordinates.lat, coordinates.lng], nextZoom)
  }, [selectedStopId, selectedStopSource])

  useEffect(() => {
    if (
      centerSelectedStopRequest === 0 ||
      selectedStopId === null ||
      selectedStopId === undefined ||
      !mapInstanceRef.current
    ) {
      return
    }

    const marker = markerByStopIdRef.current.get(String(selectedStopId))
    if (!marker) {
      return
    }

    const coordinates = marker.getLatLng()
    mapInstanceRef.current.setView([coordinates.lat, coordinates.lng], SELECTED_STOP_ZOOM)
  }, [centerSelectedStopRequest, selectedStopId])

  useEffect(() => {
    if (centerUserLocationRequest === 0 || !mapInstanceRef.current || !userLocation) {
      return
    }

    const currentZoom = mapInstanceRef.current.getZoom()
    const nextZoom = Math.max(currentZoom, SELECTED_STOP_ZOOM)
    mapInstanceRef.current.setView([userLocation.lat, userLocation.long], nextZoom)
  }, [centerUserLocationRequest, userLocation])

  useEffect(() => {
    const map = mapInstanceRef.current
    if (!map) {
      return
    }

    if (servicePathLayerGroupRef.current) {
      servicePathLayerGroupRef.current.remove()
      servicePathLayerGroupRef.current = null
    }

    if (!Array.isArray(servicePaths) || servicePaths.length === 0) {
      return
    }

    const layerGroup = L.layerGroup().addTo(map)
    servicePathLayerGroupRef.current = layerGroup

    const renderableServicePaths = servicePaths
      .map((service, index) => {
        const coordinates = simplifyServicePathCoordinates(extractServicePathCoordinates(service))

        if (coordinates.length < 2) {
          return null
        }

        return {
          service,
          index,
          coordinates,
          weight: SERVICE_PATH_MIN_WEIGHT + (index * SERVICE_PATH_WEIGHT_STEP),
        }
      })
      .filter((entry) => entry !== null)
      .sort((firstEntry, secondEntry) => firstEntry.weight - secondEntry.weight)

    renderableServicePaths.forEach(({ service, index, coordinates, weight }) => {
      const serviceColor = SERVICE_PATH_COLORS[index % SERVICE_PATH_COLORS.length]
      const serviceName = service?.['service-name']

      L.polyline(coordinates, {
        color: SERVICE_PATH_OUTLINE_COLOR,
        weight: weight + SERVICE_PATH_OUTLINE_EXTRA,
        opacity: 0.92,
        lineCap: 'round',
        lineJoin: 'round',
      }).addTo(layerGroup)

      const line = L.polyline(coordinates, {
        color: serviceColor,
        weight,
        opacity: 0.9,
        lineCap: 'round',
        lineJoin: 'round',
      }).addTo(layerGroup)

      if (serviceName) {
        line.bindTooltip(String(serviceName), { sticky: true })
      }
    })
  }, [isMapReady, servicePaths])

  useEffect(() => {
    const map = mapInstanceRef.current
    if (!map) {
      return
    }

    if (routePathLayerGroupRef.current) {
      routePathLayerGroupRef.current.remove()
      routePathLayerGroupRef.current = null
    }

    if (!selectedRoute) {
      return
    }

    const routePathSegments = Array.isArray(selectedRoute?.path) ? selectedRoute.path : []
    const routeTravelLegs = Array.isArray(selectedRoute?.travel) ? selectedRoute.travel : []

    if (routePathSegments.length === 0) {
      return
    }

    const layerGroup = L.layerGroup().addTo(map)
    routePathLayerGroupRef.current = layerGroup

    let nonWalkingLegCount = 0

    routePathSegments.forEach((pathSegment, legIndex) => {
      const coordinates = toCoordinatePairList(pathSegment?.path)

      if (coordinates.length < 2) {
        return
      }

      const leg = routeTravelLegs[legIndex]
      const isWalking = String(leg?.type ?? '').toLowerCase() === 'walking'
      const baseColor = nonWalkingLegCount % 2 === 0
        ? ROUTE_PATH_PRIMARY_COLOR
        : ROUTE_PATH_ALTERNATE_COLOR

      if (!isWalking) {
        nonWalkingLegCount += 1
      }

      L.polyline(coordinates, {
        color: baseColor,
        weight: 5,
        opacity: 0.92,
        dashArray: isWalking ? '7 8' : undefined,
        lineCap: 'round',
        lineJoin: 'round',
      }).addTo(layerGroup)
    })
  }, [isMapReady, selectedRoute])

  return (
    <div className="map-container">
      {weatherOverlay?.summary && (
        <div className="map-weather-overlay" role="status" aria-live="polite">
          <span className="map-weather-emoji" aria-hidden="true">{weatherOverlay.emoji}</span>
          <span className="map-weather-text">{weatherOverlay.summary}</span>
        </div>
      )}
      <div ref={mapElementRef} className="map-canvas" aria-label="Leaflet map display" />
    </div>
  )
}