import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import './TransportApp.css'
import SearchBar from './SearchBar'
import StopInformation from './StopInformation'
import MapDisplay from './MapDisplay'
import RouteSelection from './RouteSelection'
import RouteInformation from './RouteInformation'
import {
  now,
  requestMap,
  requestRoutes,
  requestServicesThroughNode,
  requestTimetable,
  requestTimetablesForNodes,
  requestWeather,
} from '../../../api/api-request'
import { rankRoutes, weatherEmoji } from '../../../api/api-data'

const LANCASTER_CENTER = { lat: 54.0476, long: -2.8015 }
const MAX_CLOSEST_STOPS = 6
const TIMETABLE_WINDOW_MS = 24 * 60 * 60 * 1000
const NEARBY_TIMETABLE_REFRESH_MS = 30 * 1000
const MAP_REQUEST_DEBOUNCE_MS = 350
const MAP_CACHE_ENTRY_TTL_MS = 30 * 60 * 1000
const MAP_CACHE_MIN_RADIUS_KM = 0.9
const MAP_CACHE_MAX_RADIUS_KM = 4
const MAP_CACHE_EDGE_BUFFER_KM = 0.15
const MAP_CACHE_RENDER_MARGIN_KM = 0.35
const MAP_CACHE_MAX_AREAS = 48
const MAP_CACHE_MAX_RENDER_AREAS = 6
const MAP_CENTER_CHANGE_EPSILON_KM = 0.01
const WEATHER_REFRESH_INTERVAL_MS = 60 * 1000
const WEATHER_COORDINATE_DECIMALS = 4
const WEATHER_LABELS = {
  sunny: 'Sunny',
  clear: 'Clear',
  cloudy: 'Cloudy',
  rain: 'Rain',
  storm: 'Storm',
  snow: 'Snow',
  snowstorm: 'Snowstorm',
  unknown: 'Unknown',
}

function normalizeText(value) {
  return (value ?? '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim()
}

function levenshteinDistance(firstText, secondText) {
  const first = normalizeText(firstText)
  const second = normalizeText(secondText)

  if (first === second) {
    return 0
  }
  if (!first.length) {
    return second.length
  }
  if (!second.length) {
    return first.length
  }

  const matrix = Array.from({ length: first.length + 1 }, () => Array(second.length + 1).fill(0))

  for (let row = 0; row <= first.length; row += 1) {
    matrix[row][0] = row
  }
  for (let column = 0; column <= second.length; column += 1) {
    matrix[0][column] = column
  }

  for (let row = 1; row <= first.length; row += 1) {
    for (let column = 1; column <= second.length; column += 1) {
      const substitutionCost = first[row - 1] === second[column - 1] ? 0 : 1
      matrix[row][column] = Math.min(
        matrix[row - 1][column] + 1,
        matrix[row][column - 1] + 1,
        matrix[row - 1][column - 1] + substitutionCost,
      )
    }
  }

  return matrix[first.length][second.length]
}

function fuzzyNameScore(query, stopName) {
  const normalizedQuery = normalizeText(query)
  const normalizedName = normalizeText(stopName)

  if (!normalizedQuery) {
    return 0
  }
  if (!normalizedName) {
    return Number.MAX_SAFE_INTEGER
  }
  if (normalizedName.includes(normalizedQuery)) {
    return 0
  }

  const nameWords = normalizedName.split(' ')
  const bestWordDistance = nameWords.reduce(
    (bestDistance, word) => Math.min(bestDistance, levenshteinDistance(normalizedQuery, word)),
    Number.MAX_SAFE_INTEGER,
  )

  const wholeNameDistance = levenshteinDistance(normalizedQuery, normalizedName)
  return Math.min(bestWordDistance, wholeNameDistance)
}

function getDistanceInKm(from, to) {
  const toRadians = (value) => (value * Math.PI) / 180
  const earthRadiusKm = 6371

  const latDelta = toRadians(to.lat - from.lat)
  const longDelta = toRadians(to.long - from.long)
  const fromLatRad = toRadians(from.lat)
  const toLatRad = toRadians(to.lat)

  const a =
    Math.sin(latDelta / 2) ** 2 +
    Math.cos(fromLatRad) * Math.cos(toLatRad) * Math.sin(longDelta / 2) ** 2

  return earthRadiusKm * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

function normalizeMapCenter(center) {
  const lat = Number(center?.lat)
  const long = Number(center?.long)

  if (!Number.isFinite(lat) || !Number.isFinite(long)) {
    return null
  }

  return { lat, long }
}

function normalizeNodeForCache(node) {
  if (!node || node.id === undefined || node.id === null) {
    return null
  }

  const lat = Number(node?.location?.lat)
  const long = Number(node?.location?.long)
  if (!Number.isFinite(lat) || !Number.isFinite(long)) {
    return null
  }

  return {
    ...node,
    location: {
      ...node.location,
      lat,
      long,
    },
  }
}

function mergeNodesById(previousNodes, nextNodes) {
  const mergedById = new Map()

  const mergeArray = (nodes) => {
    if (!Array.isArray(nodes)) {
      return
    }

    nodes.forEach((node) => {
      const normalized = normalizeNodeForCache(node)
      if (!normalized) {
        return
      }

      mergedById.set(String(normalized.id), normalized)
    })
  }

  mergeArray(previousNodes)
  mergeArray(nextNodes)
  return Array.from(mergedById.values())
}

function normalizeWeatherPayload(weatherPayload, fallbackLocation = null) {
  if (typeof weatherPayload === 'string') {
    const code = weatherPayload.toLowerCase().trim()
    return {
      weather: code || 'unknown',
      location: fallbackLocation ?? undefined,
    }
  }

  if (!weatherPayload || typeof weatherPayload !== 'object') {
    return null
  }

  const rawCode = typeof weatherPayload.weather === 'string'
    ? weatherPayload.weather
    : 'unknown'
  const weatherCode = rawCode.toLowerCase().trim() || 'unknown'

  const normalized = {
    ...weatherPayload,
    weather: weatherCode,
  }

  if (!normalized.location && fallbackLocation) {
    normalized.location = fallbackLocation
  }

  return normalized
}

function extractWeatherFromMapPayload(mapPayload, fallbackLocation = null) {
  const candidates = [
    mapPayload?.information?.weather,
    mapPayload?.weather,
    mapPayload?.meta?.information?.weather,
    mapPayload?.meta?.weather,
  ]

  for (const candidate of candidates) {
    const normalized = normalizeWeatherPayload(candidate, fallbackLocation)
    if (normalized) {
      return normalized
    }
  }

  return null
}

function getCurrentUserLocation() {
  if (typeof navigator === 'undefined' || !navigator.geolocation) {
    return Promise.resolve(null)
  }

  return new Promise((resolve) => {
    navigator.geolocation.getCurrentPosition(
      (position) => {
        resolve({
          lat: position.coords.latitude,
          long: position.coords.longitude,
        })
      },
      () => resolve(null),
      {
        enableHighAccuracy: true,
        timeout: 10000,
        maximumAge: 60000,
      },
    )
  })
}

function parseIsoDate(value) {
  if (!value) {
    return null
  }
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    return null
  }
  return parsed
}

function dedupeArrivalsByTimeAndService(arrivals) {
  if (!Array.isArray(arrivals)) {
    return []
  }

  const seen = new Set()
  const deduped = []

  arrivals.forEach((arrival) => {
    const arrivalTime = String(arrival?.time ?? '')
    const serviceName = String(arrival?.name ?? '')
    const key = `${arrivalTime}::${serviceName}`

    if (seen.has(key)) {
      return
    }

    seen.add(key)
    deduped.push(arrival)
  })

  return deduped
}

function getArrivalsWithin24Hours(arrivals, referenceDate = new Date()) {
  if (!Array.isArray(arrivals)) {
    return []
  }

  const start = referenceDate.getTime()
  const end = start + TIMETABLE_WINDOW_MS

  const arrivalsInWindow = arrivals
    .filter((arrival) => {
      const arrivalDate = parseIsoDate(arrival?.time)
      if (!arrivalDate) {
        return false
      }
      const arrivalMs = arrivalDate.getTime()
      return arrivalMs >= start && arrivalMs <= end
    })
    .sort((first, second) => new Date(first.time).getTime() - new Date(second.time).getTime())

  return dedupeArrivalsByTimeAndService(arrivalsInWindow)
}

function formatArrivalSummary(arrival) {
  if (!arrival?.time) {
    return 'No upcoming arrivals'
  }

  const when = parseIsoDate(arrival.time)
  if (!when) {
    return 'No upcoming arrivals'
  }

  const localTime = when.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  const serviceName = arrival.name ? ` ${arrival.name}` : ''
  return `${serviceName} at ${localTime}`.trim()
}

function toDateTimeLocalString(date) {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  return `${year}-${month}-${day}T${hours}:${minutes}`
}

function localDateTimeToUtcIso(value) {
  if (!value) {
    return now()
  }

  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    return now()
  }

  const year = parsed.getUTCFullYear()
  const month = String(parsed.getUTCMonth() + 1).padStart(2, '0')
  const day = String(parsed.getUTCDate()).padStart(2, '0')
  const hours = String(parsed.getUTCHours()).padStart(2, '0')
  const minutes = String(parsed.getUTCMinutes()).padStart(2, '0')
  const seconds = String(parsed.getUTCSeconds()).padStart(2, '0')
  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}Z`
}

async function savePdfDocument(doc, fileName) {
  const pdfBytes = doc.output('arraybuffer')

  if (typeof window !== 'undefined' && typeof window.showSaveFilePicker === 'function') {
    const fileHandle = await window.showSaveFilePicker({
      suggestedName: fileName,
      types: [
        {
          description: 'PDF Document',
          accept: { 'application/pdf': ['.pdf'] },
        },
      ],
    })

    const writable = await fileHandle.createWritable()
    await writable.write(pdfBytes)
    await writable.close()
    return
  }

  doc.save(fileName)
}

async function buildTimetablePdf(stop, timetable) {
  const { jsPDF } = await import('https://cdn.jsdelivr.net/npm/jspdf@2.5.1/+esm')
  const doc = new jsPDF({ unit: 'pt', format: 'a4' })

  const arrivals = timetable?.arrivals ?? []
  const createdAt = new Date().toLocaleString()
  const currentDate = new Date().toLocaleDateString()
  const stopTitle = stop?.name || `Stop ${stop?.id ?? ''}`
  const locationLine = stop?.location
    ? `Lat ${stop.location.lat.toFixed(5)}, Long ${stop.location.long.toFixed(5)}`
    : 'Location unavailable'

  doc.setFillColor(227, 242, 253)
  doc.rect(0, 0, 595, 110, 'F')
  doc.setFont('helvetica', 'bold')
  doc.setTextColor(31, 41, 55)
  doc.setFontSize(20)
  doc.text('Upcoming Arrivals', 40, 46)

  doc.setFont('helvetica', 'normal')
  doc.setFontSize(11)
  doc.text(stopTitle, 40, 70)
  doc.text(locationLine, 40, 88)
  doc.text(`Date: ${currentDate}`, 370, 70)
  doc.text(`Generated: ${createdAt}`, 370, 88)

  let cursorY = 138
  doc.setFont('helvetica', 'bold')
  doc.setFontSize(11)
  doc.text('Time', 40, cursorY)
  doc.text('Service', 120, cursorY)
  doc.text('Status', 210, cursorY)
  doc.text('Operator', 300, cursorY)
  cursorY += 14

  if (arrivals.length === 0) {
    doc.setFont('helvetica', 'normal')
    doc.setTextColor(107, 114, 128)
    doc.text('No arrivals in the next 24 hours.', 40, cursorY + 20)
    return doc
  }

  arrivals.slice(0, 16).forEach((arrival, index) => {
    if (cursorY > 760) {
      doc.addPage()
      cursorY = 60
    }

    if (index % 2 === 0) {
      doc.setFillColor(249, 250, 251)
      doc.rect(36, cursorY - 12, 523, 24, 'F')
    }

    const arrivalTime = parseIsoDate(arrival.time)
    const timeLabel = arrivalTime
      ? arrivalTime.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
      : 'Unknown'
    const statusLabel = (arrival.status ?? 'unknown').replace('-', ' ')

    doc.setFont('helvetica', 'bold')
    doc.setTextColor(17, 24, 39)
    doc.text(timeLabel, 40, cursorY + 4)
    doc.setTextColor(29, 78, 216)
    doc.text(arrival.name ?? 'Service', 120, cursorY + 4)

    doc.setFont('helvetica', 'normal')
    doc.setTextColor(55, 65, 81)
    doc.text(statusLabel, 210, cursorY + 4)
    doc.text(arrival.operator ?? 'Operator unavailable', 300, cursorY + 4)

    cursorY += 24
  })

  return doc
}

export default function TransportApp() {
  const [selectedStopId, setSelectedStopId] = useState(null)
  const [selectedStopSource, setSelectedStopSource] = useState(null)
  const [searchInput, setSearchInput] = useState('')
  const [mapNodes, setMapNodes] = useState([])
  const [mapWeather, setMapWeather] = useState(null)
  const [userLocation, setUserLocation] = useState(null)
  const [mapCenter, setMapCenter] = useState(LANCASTER_CENTER)
  const [centerSelectedStopRequest, setCenterSelectedStopRequest] = useState(0)
  const [centerUserLocationRequest, setCenterUserLocationRequest] = useState(0)
  const [timetablesByStopId, setTimetablesByStopId] = useState({})
  const [selectedStopTimetableLoading, setSelectedStopTimetableLoading] = useState(false)
  const [selectedStopTimetableSaving, setSelectedStopTimetableSaving] = useState(false)
  const [sidebarMode, setSidebarMode] = useState('stops')
  const [routeSourceId, setRouteSourceId] = useState(null)
  const [routeDestinationId, setRouteDestinationId] = useState(null)
  const [routeSourceQuery, setRouteSourceQuery] = useState('')
  const [routeDestinationQuery, setRouteDestinationQuery] = useState('')
  const [routeDateTime, setRouteDateTime] = useState(toDateTimeLocalString(new Date()))
  const [routeRequestedDateTime, setRouteRequestedDateTime] = useState(toDateTimeLocalString(new Date()))
  const [routePreferences, setRoutePreferences] = useState([])
  const [routeMapPickTarget, setRouteMapPickTarget] = useState(null)
  const [mapSelectionVersion, setMapSelectionVersion] = useState(0)
  const [selectedStopSelectionVersion, setSelectedStopSelectionVersion] = useState(0)
  const [routeResults, setRouteResults] = useState(null)
  const [selectedRouteIndex, setSelectedRouteIndex] = useState(0)
  const [routeRequestLoading, setRouteRequestLoading] = useState(false)
  const [routeRequestError, setRouteRequestError] = useState(null)
  const [initialCenterResolved, setInitialCenterResolved] = useState(false)
  const [servicesByNodeId, setServicesByNodeId] = useState({})
  const servicesByNodeIdRef = useRef({})
  const serviceRequestInFlightRef = useRef(new Set())
  const queuedServiceRefreshRef = useRef(new Set())
  const isComponentMountedRef = useRef(true)
  const mapAreaCacheRef = useRef([])
  const mapNodeIndexRef = useRef(new Map())
  const latestMapRequestIdRef = useRef(0)
  const latestMapCenterRef = useRef(LANCASTER_CENTER)
  const latestUserLocationRef = useRef(null)
  const weatherRequestInFlightRef = useRef(false)
  const routeInfoEntrySelectedStopIdRef = useRef(null)
  const routeInfoEntrySelectedStopSourceRef = useRef(null)

  useEffect(() => {
    servicesByNodeIdRef.current = servicesByNodeId
  }, [servicesByNodeId])

  useEffect(() => {
    latestMapCenterRef.current = mapCenter
  }, [mapCenter])

  useEffect(() => {
    latestUserLocationRef.current = userLocation
  }, [userLocation])

  useEffect(() => () => {
    isComponentMountedRef.current = false
  }, [])

  useEffect(() => {
    if (typeof navigator === 'undefined' || !navigator.geolocation) {
      return
    }

    const watchId = navigator.geolocation.watchPosition(
      (position) => {
        setUserLocation({
          lat: position.coords.latitude,
          long: position.coords.longitude,
        })
      },
      () => {
      },
      {
        enableHighAccuracy: true,
        timeout: 10000,
        maximumAge: 5000,
      },
    )

    return () => {
      navigator.geolocation.clearWatch(watchId)
    }
  }, [])

  const pruneMapAreaCache = useCallback(() => {
    const cutoffTime = Date.now() - MAP_CACHE_ENTRY_TTL_MS
    const nextAreas = mapAreaCacheRef.current
      .filter((entry) => entry && entry.fetchedAt >= cutoffTime)
      .sort((first, second) => first.fetchedAt - second.fetchedAt)

    if (nextAreas.length > MAP_CACHE_MAX_AREAS) {
      nextAreas.splice(0, nextAreas.length - MAP_CACHE_MAX_AREAS)
    }

    mapAreaCacheRef.current = nextAreas

    const referencedNodeIds = new Set()
    nextAreas.forEach((entry) => {
      const nodeIds = Array.isArray(entry.nodeIds) ? entry.nodeIds : []
      nodeIds.forEach((nodeId) => referencedNodeIds.add(nodeId))
    })

    const nodeIndex = mapNodeIndexRef.current
    Array.from(nodeIndex.keys()).forEach((nodeId) => {
      if (!referencedNodeIds.has(nodeId)) {
        nodeIndex.delete(nodeId)
      }
    })

    return nextAreas
  }, [])

  const isCenterCoveredByMapCache = useCallback((center) => {
    const normalizedCenter = normalizeMapCenter(center)
    if (!normalizedCenter) {
      return false
    }

    const areas = pruneMapAreaCache()
    return areas.some((entry) => {
      if (!entry?.center || !Number.isFinite(entry?.radiusKm)) {
        return false
      }

      const effectiveRadius = Math.max(0, entry.radiusKm - MAP_CACHE_EDGE_BUFFER_KM)
      return getDistanceInKm(normalizedCenter, entry.center) <= effectiveRadius
    })
  }, [pruneMapAreaCache])

  const getCachedWeatherForCenter = useCallback((center) => {
    const normalizedCenter = normalizeMapCenter(center)
    if (!normalizedCenter) {
      return null
    }

    const areas = pruneMapAreaCache()
    const weatherAreas = areas
      .filter(
        (entry) =>
          entry?.center &&
          entry.weather &&
          typeof entry.weather === 'object',
      )
      .sort(
        (first, second) =>
          getDistanceInKm(normalizedCenter, first.center) - getDistanceInKm(normalizedCenter, second.center),
      )

    return weatherAreas[0]?.weather ?? null
  }, [pruneMapAreaCache])

  const getRenderableNodesForCenter = useCallback((center) => {
    const normalizedCenter = normalizeMapCenter(center)
    if (!normalizedCenter) {
      return []
    }

    const areas = pruneMapAreaCache()
    const renderableAreas = areas
      .filter((entry) => {
        if (!entry?.center || !Number.isFinite(entry?.radiusKm)) {
          return false
        }

        const distanceKm = getDistanceInKm(normalizedCenter, entry.center)
        return distanceKm <= entry.radiusKm + MAP_CACHE_RENDER_MARGIN_KM
      })
      .sort(
        (first, second) =>
          getDistanceInKm(normalizedCenter, first.center) - getDistanceInKm(normalizedCenter, second.center),
      )
      .slice(0, MAP_CACHE_MAX_RENDER_AREAS)

    const renderNodeIds = new Set()
    renderableAreas.forEach((entry) => {
      const nodeIds = Array.isArray(entry.nodeIds) ? entry.nodeIds : []
      nodeIds.forEach((nodeId) => renderNodeIds.add(nodeId))
    })

    const nodes = []
    renderNodeIds.forEach((nodeId) => {
      const node = mapNodeIndexRef.current.get(nodeId)
      if (node) {
        nodes.push(node)
      }
    })

    return nodes
  }, [pruneMapAreaCache])

  const cacheMapResponseForCenter = useCallback((center, mapData) => {
    const normalizedCenter = normalizeMapCenter(center)
    if (!normalizedCenter) {
      return []
    }

    const responseNodes = Array.isArray(mapData?.nodes) ? mapData.nodes : []
    const normalizedNodes = responseNodes
      .map((node) => normalizeNodeForCache(node))
      .filter((node) => node !== null)

    const nodeIndex = mapNodeIndexRef.current
    const nodeIds = []
    normalizedNodes.forEach((node) => {
      const nodeKey = String(node.id)
      nodeIndex.set(nodeKey, node)
      nodeIds.push(nodeKey)
    })

    let maxDistanceKm = 0
    normalizedNodes.forEach((node) => {
      const distanceKm = getDistanceInKm(normalizedCenter, node.location)
      if (Number.isFinite(distanceKm)) {
        maxDistanceKm = Math.max(maxDistanceKm, distanceKm)
      }
    })

    const areaRadiusKm = Math.min(
      MAP_CACHE_MAX_RADIUS_KM,
      Math.max(MAP_CACHE_MIN_RADIUS_KM, maxDistanceKm),
    )

    const weatherInfo = extractWeatherFromMapPayload(mapData, normalizedCenter)
    const nextAreas = pruneMapAreaCache().slice()
    const mergeThresholdKm = Math.max(0.15, areaRadiusKm * 0.2)
    const existingIndex = nextAreas.findIndex(
      (entry) => getDistanceInKm(normalizedCenter, entry.center) <= mergeThresholdKm,
    )

    const existingWeather = existingIndex >= 0 ? nextAreas[existingIndex]?.weather : null
    const nextAreaEntry = {
      center: normalizedCenter,
      radiusKm: areaRadiusKm,
      fetchedAt: Date.now(),
      nodeIds: Array.from(new Set(nodeIds)),
      weather: weatherInfo ?? existingWeather ?? null,
    }

    if (existingIndex >= 0) {
      nextAreas[existingIndex] = nextAreaEntry
    } else {
      nextAreas.push(nextAreaEntry)
    }

    mapAreaCacheRef.current = nextAreas
    pruneMapAreaCache()

    return normalizedNodes
  }, [pruneMapAreaCache])

  const applyCachedMapDataForCenter = useCallback((center) => {
    const cachedNodes = getRenderableNodesForCenter(center)
    if (cachedNodes.length > 0 || isCenterCoveredByMapCache(center)) {
      setMapNodes((previousNodes) => mergeNodesById(previousNodes, cachedNodes))
    }

    const cachedWeather = getCachedWeatherForCenter(center)
    if (cachedWeather) {
      setMapWeather(cachedWeather)
    }
  }, [getCachedWeatherForCenter, getRenderableNodesForCenter, isCenterCoveredByMapCache])

  const fetchMapNodesForCenter = useCallback(async (center) => {
    const normalizedCenter = normalizeMapCenter(center)
    if (!normalizedCenter) {
      return
    }

    const requestId = latestMapRequestIdRef.current + 1
    latestMapRequestIdRef.current = requestId

    try {
      // Force fresh map nodes because api-request cache is not area-aware.
      const mapData = await requestMap(normalizedCenter.lat, normalizedCenter.long, { forceRefresh: true })

      if (!isComponentMountedRef.current || requestId !== latestMapRequestIdRef.current) {
        return
      }

      if (!mapData) {
        applyCachedMapDataForCenter(normalizedCenter)
        return
      }

      const weatherInfo = extractWeatherFromMapPayload(mapData, normalizedCenter)
      if (weatherInfo) {
        setMapWeather(weatherInfo)
      }

      const freshNodes = cacheMapResponseForCenter(normalizedCenter, mapData)
      const nodesForCenter = getRenderableNodesForCenter(normalizedCenter)
      if (nodesForCenter.length > 0) {
        setMapNodes((previousNodes) => mergeNodesById(previousNodes, nodesForCenter))
      } else {
        setMapNodes((previousNodes) => mergeNodesById(previousNodes, freshNodes))
      }
    } catch (error) {
      console.error('Failed to load map nodes:', error)
    }
  }, [applyCachedMapDataForCenter, cacheMapResponseForCenter, getRenderableNodesForCenter])

  useEffect(() => {
    let isMounted = true

    const resolveInitialCenter = async () => {
      const currentLocation = await getCurrentUserLocation()
      if (!isMounted) {
        return
      }

      if (currentLocation) {
        setUserLocation(currentLocation)
        setMapCenter(currentLocation)
      } else {
        setMapCenter(LANCASTER_CENTER)
      }

      setInitialCenterResolved(true)
    }

    resolveInitialCenter()

    return () => {
      isMounted = false
    }
  }, [])

  const refreshWeatherForCurrentLocation = useCallback(async () => {
    if (weatherRequestInFlightRef.current) {
      return
    }

    const sourceCoordinates =
      latestUserLocationRef.current ??
      latestMapCenterRef.current ??
      LANCASTER_CENTER
    const normalizedCoordinates = normalizeMapCenter(sourceCoordinates)
    if (!normalizedCoordinates) {
      return
    }

    const lat = Number(normalizedCoordinates.lat.toFixed(WEATHER_COORDINATE_DECIMALS))
    const long = Number(normalizedCoordinates.long.toFixed(WEATHER_COORDINATE_DECIMALS))

    weatherRequestInFlightRef.current = true
    try {
      const weatherData = await requestWeather(lat, long)
      const normalizedWeather = normalizeWeatherPayload(weatherData, normalizedCoordinates)
      if (!isComponentMountedRef.current || !normalizedWeather) {
        return
      }

      setMapWeather((previousWeather) => {
        const previousCode = String(previousWeather?.weather ?? '').toLowerCase().trim()
        const nextCode = String(normalizedWeather?.weather ?? '').toLowerCase().trim()
        const previousTemperature = Number.isFinite(Number(previousWeather?.temperature))
          ? Number(previousWeather.temperature)
          : null
        const nextTemperature = Number.isFinite(Number(normalizedWeather?.temperature))
          ? Number(normalizedWeather.temperature)
          : null

        if (previousCode === nextCode && previousTemperature === nextTemperature) {
          return previousWeather
        }

        return normalizedWeather
      })
    } catch (error) {
      console.error('Failed to refresh weather data:', error)
    } finally {
      weatherRequestInFlightRef.current = false
    }
  }, [])

  useEffect(() => {
    if (!initialCenterResolved) {
      return
    }

    refreshWeatherForCurrentLocation()
    const intervalId = setInterval(refreshWeatherForCurrentLocation, WEATHER_REFRESH_INTERVAL_MS)

    return () => {
      clearInterval(intervalId)
    }
  }, [initialCenterResolved, refreshWeatherForCurrentLocation])

  useEffect(() => {
    if (!initialCenterResolved) {
      return
    }

    const normalizedCenter = normalizeMapCenter(mapCenter)
    if (!normalizedCenter) {
      return
    }

    if (isCenterCoveredByMapCache(normalizedCenter)) {
      applyCachedMapDataForCenter(normalizedCenter)
      return
    }

    const timeoutId = setTimeout(() => {
      fetchMapNodesForCenter(normalizedCenter)
    }, MAP_REQUEST_DEBOUNCE_MS)

    return () => {
      clearTimeout(timeoutId)
    }
  }, [
    applyCachedMapDataForCenter,
    fetchMapNodesForCenter,
    initialCenterResolved,
    isCenterCoveredByMapCache,
    mapCenter,
  ])

  const nearbyStops = useMemo(() => {
    if (!Array.isArray(mapNodes) || mapNodes.length === 0) {
      return []
    }

    const origin = userLocation ?? mapCenter ?? LANCASTER_CENTER

    return mapNodes
      .filter((node) => node?.location?.lat !== undefined && node?.location?.long !== undefined)
      .map((node) => {
        const distanceKm = getDistanceInKm(origin, {
          lat: node.location.lat,
          long: node.location.long,
        })

        return {
          id: node.id,
          name: node.name || `Stop ${node.id}`,
          distanceLabel: `${distanceKm.toFixed(2)} km away`,
          location: node.location,
          distanceKm,
        }
      })
      .sort((first, second) => first.distanceKm - second.distanceKm)
      .slice(0, MAX_CLOSEST_STOPS)
  }, [mapNodes, mapCenter, userLocation])

  const displayedStops = useMemo(() => {
    const trimmedSearch = searchInput.trim()
    if (!trimmedSearch) {
      return nearbyStops
    }

    const scoredStops = mapNodes
      .filter((node) => node?.location?.lat !== undefined && node?.location?.long !== undefined)
      .map((node) => {
        const origin = userLocation ?? mapCenter ?? LANCASTER_CENTER
        const distanceKm = getDistanceInKm(origin, {
          lat: node.location.lat,
          long: node.location.long,
        })

        return {
          id: node.id,
          name: node.name || `Stop ${node.id}`,
          distanceLabel: `${distanceKm.toFixed(2)} km away`,
          location: node.location,
          distanceKm,
          nameScore: fuzzyNameScore(trimmedSearch, node.name || ''),
        }
      })
      .sort((first, second) => {
        if (first.nameScore !== second.nameScore) {
          return first.nameScore - second.nameScore
        }
        return first.distanceKm - second.distanceKm
      })

    return scoredStops.slice(0, MAX_CLOSEST_STOPS)
  }, [mapCenter, mapNodes, nearbyStops, searchInput, userLocation])

  const searchSuggestions = useMemo(() => {
    const trimmedSearch = searchInput.trim()
    if (!trimmedSearch) {
      return []
    }

    return displayedStops
      .slice(0, 5)
      .map((stop) => ({ id: stop.id, name: stop.name }))
  }, [displayedStops, searchInput])

  const nearbyStopPollingNodeIdsKey = useMemo(() => {
    if (!Array.isArray(displayedStops) || displayedStops.length === 0) {
      return ''
    }

    const uniqueNodeIds = new Set(displayedStops.map((stop) => String(stop.id)))
    return Array.from(uniqueNodeIds).sort().join(',')
  }, [displayedStops])

  const isNearbyStopsListVisible =
    sidebarMode === 'stops' && (selectedStopId === null || selectedStopId === undefined)

  useEffect(() => {
    if (!isNearbyStopsListVisible || !nearbyStopPollingNodeIdsKey) {
      return
    }

    const nodeIds = nearbyStopPollingNodeIdsKey.split(',').filter(Boolean)
    if (nodeIds.length === 0) {
      return
    }

    let isMounted = true
    let isRequestInFlight = false

    const loadNearbyTimetables = async () => {
      if (isRequestInFlight) {
        return
      }

      isRequestInFlight = true
      try {
        const timetableData = await requestTimetablesForNodes(now(), nodeIds)
        if (!isMounted || !timetableData) {
          return
        }

        setTimetablesByStopId((previous) => ({ ...previous, ...timetableData }))
      } finally {
        isRequestInFlight = false
      }
    }

    loadNearbyTimetables()
    const intervalId = setInterval(loadNearbyTimetables, NEARBY_TIMETABLE_REFRESH_MS)

    return () => {
      isMounted = false
      clearInterval(intervalId)
    }
  }, [isNearbyStopsListVisible, nearbyStopPollingNodeIdsKey])

  useEffect(() => {
    if (selectedStopId === null) {
      return
    }

    const selectedExists = mapNodes.some((node) => node.id === selectedStopId)
    if (!selectedExists) {
      setSelectedStopId(null)
      setSelectedStopSource(null)
    }
  }, [mapNodes, selectedStopId])

  const selectedStop = useMemo(
    () => mapNodes.find((node) => node.id === selectedStopId) ?? null,
    [mapNodes, selectedStopId],
  )
  
  const routeSourceStop = useMemo(
    () => mapNodes.find((node) => node.id === routeSourceId) ?? null,
    [mapNodes, routeSourceId],
  )
  
  const routeDestinationStop = useMemo(
    () => mapNodes.find((node) => node.id === routeDestinationId) ?? null,
    [mapNodes, routeDestinationId],
  )

  const selectedRouteForMap = useMemo(() => {
    if (!Array.isArray(routeResults) || routeResults.length === 0) {
      return null
    }

    if (!Number.isInteger(selectedRouteIndex) || selectedRouteIndex < 0) {
      return routeResults[0]
    }

    return routeResults[Math.min(selectedRouteIndex, routeResults.length - 1)]
  }, [routeResults, selectedRouteIndex])

  const servicePathNodeId = useMemo(() => {
    if (sidebarMode === 'stops') {
      if (selectedStopId === null || selectedStopId === undefined) {
        return null
      }
      return String(selectedStopId)
    }

    if (
      (sidebarMode === 'route-selection' || sidebarMode === 'route-information') &&
      routeSourceId !== null &&
      routeSourceId !== undefined
    ) {
      return String(routeSourceId)
    }

    return null
  }, [routeSourceId, selectedStopId, sidebarMode])

  const servicePathsForMap = useMemo(() => {
    if (!servicePathNodeId) {
      return []
    }

    const services = servicesByNodeId[servicePathNodeId]
    return Array.isArray(services) ? services : []
  }, [servicePathNodeId, servicesByNodeId])
  
  const routeSourceSuggestions = useMemo(() => {
    const trimmedQuery = routeSourceQuery.trim()
    if (!trimmedQuery) {
      return []
    }
  
    const origin = userLocation ?? mapCenter ?? LANCASTER_CENTER
  
    return mapNodes
      .filter((node) => node?.name)
      .map((node) => ({
        id: node.id,
        name: node.name,
        distanceKm: getDistanceInKm(origin, node.location),
        score: fuzzyNameScore(trimmedQuery, node.name),
      }))
      .sort((first, second) => {
        if (first.score !== second.score) {
          return first.score - second.score
        }
        return first.distanceKm - second.distanceKm
      })
      .slice(0, 5)
      .map(({ id, name }) => ({ id, name }))
  }, [mapCenter, mapNodes, routeSourceQuery, userLocation])
  
  const routeDestinationSuggestions = useMemo(() => {
    const trimmedQuery = routeDestinationQuery.trim()
    if (!trimmedQuery) {
      return []
    }
  
    const origin = userLocation ?? mapCenter ?? LANCASTER_CENTER
  
    return mapNodes
      .filter((node) => node?.name)
      .map((node) => ({
        id: node.id,
        name: node.name,
        distanceKm: getDistanceInKm(origin, node.location),
        score: fuzzyNameScore(trimmedQuery, node.name),
      }))
      .sort((first, second) => {
        if (first.score !== second.score) {
          return first.score - second.score
        }
        return first.distanceKm - second.distanceKm
      })
      .slice(0, 5)
      .map(({ id, name }) => ({ id, name }))
  }, [mapCenter, mapNodes, routeDestinationQuery, userLocation])

  const refreshTimetableForStop = useCallback(async (stopId) => {
    if (stopId === null || stopId === undefined) {
      return null
    }

    const stopKey = String(stopId)
    setSelectedStopTimetableLoading(true)

    try {
      const timetable = await requestTimetable(now(), stopKey)
      if (timetable) {
        setTimetablesByStopId((previous) => ({
          ...previous,
          [stopKey]: timetable,
        }))
      }
      return timetable
    } finally {
      setSelectedStopTimetableLoading(false)
    }
  }, [])

  useEffect(() => {
    if (selectedStopId === null || selectedStopId === undefined) {
      setSelectedStopTimetableLoading(false)
      return
    }

    refreshTimetableForStop(selectedStopId)
  }, [refreshTimetableForStop, selectedStopId, selectedStopSelectionVersion])

  const requestServicesForNode = useCallback(async (nodeId, forceRefresh = false) => {
    if (!nodeId) {
      return
    }

    const nodeKey = String(nodeId)
    const hasCachedValue = Object.prototype.hasOwnProperty.call(servicesByNodeIdRef.current, nodeKey)

    if (!forceRefresh && hasCachedValue) {
      return
    }

    if (serviceRequestInFlightRef.current.has(nodeKey)) {
      if (forceRefresh) {
        queuedServiceRefreshRef.current.add(nodeKey)
      }
      return
    }

    serviceRequestInFlightRef.current.add(nodeKey)

    try {
      const services = await requestServicesThroughNode(nodeKey)
      if (!isComponentMountedRef.current) {
        return
      }

      setServicesByNodeId((previous) => ({
        ...previous,
        [nodeKey]: Array.isArray(services) ? services : [],
      }))
    } catch (error) {
      console.error('Failed to load services for stop:', nodeKey, error)
      if (!isComponentMountedRef.current) {
        return
      }

      setServicesByNodeId((previous) => ({
        ...previous,
        [nodeKey]: [],
      }))
    } finally {
      serviceRequestInFlightRef.current.delete(nodeKey)

      if (queuedServiceRefreshRef.current.has(nodeKey) && isComponentMountedRef.current) {
        queuedServiceRefreshRef.current.delete(nodeKey)
        requestServicesForNode(nodeKey, true)
      }
    }
  }, [])

  useEffect(() => {
    if (!servicePathNodeId) {
      return
    }

    const isSelectedStopServicePath =
      sidebarMode === 'stops' &&
      selectedStopId !== null &&
      selectedStopId !== undefined &&
      String(selectedStopId) === servicePathNodeId

    if (
      !isSelectedStopServicePath &&
      Object.prototype.hasOwnProperty.call(servicesByNodeIdRef.current, servicePathNodeId)
    ) {
      return
    }

    requestServicesForNode(servicePathNodeId, isSelectedStopServicePath)
  }, [requestServicesForNode, servicePathNodeId, selectedStopId, selectedStopSelectionVersion, sidebarMode])

  const nearbyStopsWithTimetables = useMemo(
    () => displayedStops.map((stop) => {
      const timetable = timetablesByStopId[String(stop.id)]
      const upcoming = getArrivalsWithin24Hours(timetable?.arrivals)
      return {
        ...stop,
        nextArrivalSummary: formatArrivalSummary(upcoming[0]),
      }
    }),
    [displayedStops, timetablesByStopId],
  )

  const selectedStopTimetable = useMemo(() => {
    if (selectedStopId === null || selectedStopId === undefined) {
      return null
    }

    const timetable = timetablesByStopId[String(selectedStopId)]
    if (!timetable) {
      return null
    }

    return {
      ...timetable,
      arrivals: getArrivalsWithin24Hours(timetable.arrivals),
    }
  }, [selectedStopId, timetablesByStopId])

  const mapWeatherOverlay = useMemo(() => {
    if (!mapWeather || typeof mapWeather !== 'object') {
      return null
    }

    const rawCode = typeof mapWeather.weather === 'string' ? mapWeather.weather : 'unknown'
    const weatherCode = rawCode.toLowerCase().trim()
    const fallbackLabel = weatherCode.replace(/[-_]+/g, ' ').trim()
    const weatherLabel = WEATHER_LABELS[weatherCode] ?? (fallbackLabel || WEATHER_LABELS.unknown)

    return {
      emoji: weatherEmoji(weatherCode),
      summary: weatherCode === 'unknown'
        ? 'Current weather unavailable'
        : `Current weather: ${weatherLabel}`,
    }
  }, [mapWeather])

  const handleMapCenterChange = useCallback((nextCenter) => {
    const normalizedCenter = normalizeMapCenter(nextCenter)
    if (!normalizedCenter) {
      return
    }

    setMapCenter((previousCenter) => {
      const previous = normalizeMapCenter(previousCenter)
      if (!previous) {
        return normalizedCenter
      }

      const movedDistanceKm = getDistanceInKm(previous, normalizedCenter)
      if (movedDistanceKm < MAP_CENTER_CHANGE_EPSILON_KM) {
        return previousCenter
      }

      return normalizedCenter
    })
  }, [])

  const handleCenterSelectedStop = () => {
    setCenterSelectedStopRequest((count) => count + 1)
  }

  const centerMapOnUserLocationIfAvailable = useCallback(async () => {
    const location = userLocation ?? await getCurrentUserLocation()
    if (!location) {
      return
    }

    setUserLocation(location)
    setMapCenter(location)
    setCenterUserLocationRequest((count) => count + 1)
  }, [userLocation])

  const handleSelectStopFromSidebar = (stopId) => {
    setRouteSourceId(null)
    setRouteDestinationId(null)
    setRouteSourceQuery('')
    setRouteDestinationQuery('')
    setRouteMapPickTarget(null)
    setSelectedStopId(stopId)
    setSelectedStopSource('sidebar')
    if (stopId !== null && stopId !== undefined) {
      setSelectedStopSelectionVersion((value) => value + 1)
    }
  }

  const handleSelectStopFromMap = (stopId) => {
    setMapSelectionVersion((value) => value + 1)

    if (
      stopId !== null &&
      stopId !== undefined &&
      (sidebarMode === 'route-selection' || sidebarMode === 'route-information') &&
      !routeMapPickTarget
    ) {
      setSidebarMode('stops')
      setRouteMapPickTarget(null)
    }

    if (!routeMapPickTarget) {
      setRouteSourceId(null)
      setRouteDestinationId(null)
      setRouteSourceQuery('')
      setRouteDestinationQuery('')
      setRouteMapPickTarget(null)
    }

    setSelectedStopId(stopId)
    setSelectedStopSource(stopId === null ? null : 'map')
    if (stopId !== null && stopId !== undefined) {
      setSelectedStopSelectionVersion((value) => value + 1)
    }
  }

  const handleCloseStopDetails = () => {
    setSelectedStopId(null)
    setSelectedStopSource(null)
    centerMapOnUserLocationIfAvailable()
  }
  
  const openRouteSelectionWithDefaults = () => {
    routeInfoEntrySelectedStopIdRef.current = selectedStopId
    routeInfoEntrySelectedStopSourceRef.current = selectedStopSource
    setSidebarMode('route-selection')
    setRouteDateTime(toDateTimeLocalString(new Date()))
    setRouteMapPickTarget(null)
    setRouteRequestError(null)
  }
  
  const handleRouteToFromStop = (stop) => {
    if (!stop) {
      return
    }
  
    openRouteSelectionWithDefaults()
    setRouteDestinationId(stop.id)
    setRouteDestinationQuery(stop.name || `Stop ${stop.id}`)
  }
  
  const handleRouteFromStop = (stop) => {
    if (!stop) {
      return
    }
  
    openRouteSelectionWithDefaults()
    setRouteSourceId(stop.id)
    setRouteSourceQuery(stop.name || `Stop ${stop.id}`)
  }
  
  const handleOpenStopSidebar = () => {
    setSidebarMode('stops')
    setRouteMapPickTarget(null)
  }
  
  const handleRouteSourceQueryChange = (value) => {
    setRouteSourceQuery(value)
    if (!value.trim()) {
      setRouteSourceId(null)
    }
  }
  
  const handleRouteDestinationQueryChange = (value) => {
    setRouteDestinationQuery(value)
    if (!value.trim()) {
      setRouteDestinationId(null)
    }
  }
  
  const handleSelectRouteSourceSuggestion = (suggestion) => {
    setRouteSourceId(suggestion.id)
    setRouteSourceQuery(suggestion.name)
  }
  
  const handleSelectRouteDestinationSuggestion = (suggestion) => {
    setRouteDestinationId(suggestion.id)
    setRouteDestinationQuery(suggestion.name)
  }

  const handleFindRoutes = async () => {
    if (!routeSourceId || !routeDestinationId) {
      setRouteRequestError('Please set both source and destination.')
      return
    }

    setRouteRequestLoading(true)
    setRouteRequestError(null)
    routeInfoEntrySelectedStopIdRef.current = selectedStopId
    routeInfoEntrySelectedStopSourceRef.current = selectedStopSource

    try {
      const requestedDateTime = routeDateTime || toDateTimeLocalString(new Date())
      setRouteRequestedDateTime(requestedDateTime)
      const departureIsoTime = localDateTimeToUtcIso(requestedDateTime)
      const routes = await requestRoutes(String(routeSourceId), String(routeDestinationId), departureIsoTime)
      setSelectedRouteIndex(0)
      if (!routes) {
        setRouteResults([])
        setRouteRequestError('No routes returned for this selection.')
      } else {
        setRouteResults(rankRoutes(routes, routePreferences))
      }
      setSidebarMode('route-information')
    } catch (error) {
      console.error('Failed to load routes:', error)
      setRouteResults([])
      setRouteRequestError('Failed to load routes.')
      setSidebarMode('route-information')
    } finally {
      setRouteRequestLoading(false)
    }
  }

  const handleCloseRouteInformationToStops = () => {
    const previousSelectedStopId = routeInfoEntrySelectedStopIdRef.current
    const previousSelectedStopSource = routeInfoEntrySelectedStopSourceRef.current

    setRouteSourceId(null)
    setRouteDestinationId(null)
    setRouteSourceQuery('')
    setRouteDestinationQuery('')
    setRouteMapPickTarget(null)
    setSelectedRouteIndex(0)
    setSidebarMode('stops')

    if (previousSelectedStopId === null || previousSelectedStopId === undefined) {
      setSelectedStopId(null)
      setSelectedStopSource(null)
      return
    }

    setSelectedStopId(previousSelectedStopId)
    setSelectedStopSource(previousSelectedStopSource ?? 'sidebar')
  }

  const handleToggleRoutePreference = (preferenceKey, enabled) => {
    setRoutePreferences((previous) => {
      if (enabled) {
        if (previous.includes(preferenceKey)) {
          return previous
        }
        return [...previous, preferenceKey]
      }

      return previous.filter((preference) => preference !== preferenceKey)
    })
  }
  
  const handleMapPickSelection = (stopId) => {
    if (!routeMapPickTarget) {
      return
    }
  
    const pickedStop = mapNodes.find((node) => node.id === stopId)
    if (!pickedStop) {
      return
    }
  
    if (routeMapPickTarget === 'source') {
      setRouteSourceId(pickedStop.id)
      setRouteSourceQuery(pickedStop.name || `Stop ${pickedStop.id}`)
    } else if (routeMapPickTarget === 'destination') {
      setRouteDestinationId(pickedStop.id)
      setRouteDestinationQuery(pickedStop.name || `Stop ${pickedStop.id}`)
    }
  
    setRouteMapPickTarget(null)
    setMapSelectionVersion((value) => value + 1)
  }

  const handleRefreshSelectedStopTimetable = async () => {
    if (selectedStopId === null || selectedStopId === undefined) {
      return
    }

    await refreshTimetableForStop(selectedStopId)
  }

  const handleSaveSelectedStopTimetable = async () => {
    if (!selectedStop) {
      return
    }

    setSelectedStopTimetableSaving(true)

    try {
      const refreshed = await refreshTimetableForStop(selectedStop.id)
      const fallbackTimetable = timetablesByStopId[String(selectedStop.id)]
      const sourceTimetable = refreshed ?? fallbackTimetable ?? { arrivals: [] }
      const timetableForPdf = {
        ...sourceTimetable,
        arrivals: getArrivalsWithin24Hours(sourceTimetable.arrivals),
      }

      const doc = await buildTimetablePdf(selectedStop, timetableForPdf)
      const safeName = (selectedStop.name || selectedStop.id || 'stop')
        .toString()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/(^-|-$)/g, '')

      await savePdfDocument(doc, `${safeName || 'stop'}-timetable.pdf`)
    } catch (error) {
      if (error?.name !== 'AbortError') {
        console.error('Failed to save timetable PDF:', error)
      }
    } finally {
      setSelectedStopTimetableSaving(false)
    }
  }

  const handleSelectSearchSuggestion = (suggestion) => {
    setSearchInput(suggestion.name)
    handleSelectStopFromSidebar(suggestion.id)
  }

  const sidebarModeTitle =
    sidebarMode === 'stops'
      ? (selectedStop ? 'Stop Details' : 'Nearby Stops')
      : sidebarMode === 'route-selection'
        ? 'Route Selection'
        : 'Route Information'

  const sidebarModeSubtitle =
    sidebarMode === 'stops'
      ? (selectedStop ? 'Live arrivals and route actions' : 'Explore nearby stops and live arrivals')
      : sidebarMode === 'route-selection'
        ? 'Set source, destination, and preferences'
        : `${routeSourceStop?.name ?? 'Unknown source'} to ${routeDestinationStop?.name ?? 'Unknown destination'}`

  return (
    <div className="app-container">
      <SearchBar
        value={searchInput}
        onChange={setSearchInput}
        suggestions={searchSuggestions}
        onSelectSuggestion={handleSelectSearchSuggestion}
      />
      <div className="main-content">
        <aside className="desktop-sidebar" aria-label="Sidebar controls and information">
          <div className="desktop-sidebar-header">
            <div className="desktop-sidebar-header-main">
              <div className="desktop-sidebar-title">{sidebarModeTitle}</div>
              <div className="desktop-sidebar-subtitle">{sidebarModeSubtitle}</div>
            </div>
            {sidebarMode !== 'stops' && (
              <button
                type="button"
                className="desktop-sidebar-reset"
                onClick={handleCloseRouteInformationToStops}
              >
                Stops
              </button>
            )}
          </div>
          <div className="desktop-sidebar-flow" aria-hidden="true">
            <span className={`desktop-sidebar-step ${sidebarMode === 'stops' ? 'active' : ''}`}>Stops</span>
            <span className={`desktop-sidebar-step ${sidebarMode === 'route-selection' ? 'active' : ''}`}>Route Setup</span>
            <span className={`desktop-sidebar-step ${sidebarMode === 'route-information' ? 'active' : ''}`}>Routes</span>
          </div>
          <div className="desktop-sidebar-content">
            {sidebarMode === 'stops' ? (
              <StopInformation
                nearbyStops={nearbyStopsWithTimetables}
                selectedStop={selectedStop}
                selectedStopTimetable={selectedStopTimetable}
                selectedStopTimetableLoading={selectedStopTimetableLoading}
                selectedStopTimetableSaving={selectedStopTimetableSaving}
                onSelectStop={handleSelectStopFromSidebar}
                onCloseStopDetails={handleCloseStopDetails}
                onCenterStop={handleCenterSelectedStop}
                onRefreshTimetable={handleRefreshSelectedStopTimetable}
                onSaveTimetable={handleSaveSelectedStopTimetable}
                onRouteToFromStop={() => handleRouteToFromStop(selectedStop)}
                onRouteFromStop={() => handleRouteFromStop(selectedStop)}
              />
            ) : sidebarMode === 'route-selection' ? (
              <RouteSelection
                sourceStop={routeSourceStop}
                destinationStop={routeDestinationStop}
                sourceQuery={routeSourceQuery}
                destinationQuery={routeDestinationQuery}
                sourceSuggestions={routeSourceSuggestions}
                destinationSuggestions={routeDestinationSuggestions}
                departureTime={routeDateTime}
                preferences={routePreferences}
                mapPickTarget={routeMapPickTarget}
                mapSelectionVersion={mapSelectionVersion}
                onSourceQueryChange={handleRouteSourceQueryChange}
                onDestinationQueryChange={handleRouteDestinationQueryChange}
                onSelectSourceSuggestion={handleSelectRouteSourceSuggestion}
                onSelectDestinationSuggestion={handleSelectRouteDestinationSuggestion}
                onSetDepartureTime={setRouteDateTime}
                onTogglePreference={handleToggleRoutePreference}
                onStartMapPickSource={() => setRouteMapPickTarget('source')}
                onStartMapPickDestination={() => setRouteMapPickTarget('destination')}
                onCancelMapPick={() => setRouteMapPickTarget(null)}
                onFindRoutes={handleFindRoutes}
                isFindingRoutes={routeRequestLoading}
                routeError={routeRequestError}
              />
            ) : (
              <RouteInformation
                sourceStop={routeSourceStop}
                destinationStop={routeDestinationStop}
                requestedDateTime={routeRequestedDateTime}
                routes={routeResults}
                nodes={mapNodes}
                selectedRouteIndex={selectedRouteIndex}
                onSelectRouteIndex={setSelectedRouteIndex}
                routeError={routeRequestError}
                onBackToSelection={() => setSidebarMode('route-selection')}
              />
            )}
          </div>
        </aside>
        <MapDisplay
          selectedStopId={selectedStopId}
          selectedStopSource={selectedStopSource}
          routeSourceId={routeSourceId}
          routeDestinationId={routeDestinationId}
          selectedRoute={sidebarMode === 'route-information' ? selectedRouteForMap : null}
          servicePaths={servicePathsForMap}
          nodes={mapNodes}
          userLocation={userLocation}
          weatherOverlay={mapWeatherOverlay}
          onSelectStop={handleSelectStopFromMap}
          onMapPickStop={handleMapPickSelection}
          mapPickTarget={routeMapPickTarget}
          onMapCenterChange={handleMapCenterChange}
          centerSelectedStopRequest={centerSelectedStopRequest}
          centerUserLocationRequest={centerUserLocationRequest}
        />
      </div>
    </div>
  )
}