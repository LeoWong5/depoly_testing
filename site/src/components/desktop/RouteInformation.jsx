import React, { useMemo } from 'react'
import './RouteInformation.css'

const ROUTE_TYPE_LABELS = {
  fastest: 'Fastest',
  'fewest-changes': 'Fewest Changes',
  'least-walking': 'Least Walking',
}

const TRANSPORT_TYPE_LABELS = {
  bus: 'Bus',
  train: 'Train',
  tram: 'Tram',
  walking: 'Walk',
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

function isSameDay(firstDate, secondDate) {
  return (
    firstDate.getFullYear() === secondDate.getFullYear() &&
    firstDate.getMonth() === secondDate.getMonth() &&
    firstDate.getDate() === secondDate.getDate()
  )
}

function formatRequestedDateLabel(value) {
  const requestedDate = parseIsoDate(value)
  if (!requestedDate) {
    return 'Date unknown'
  }

  const today = new Date()
  const tomorrow = new Date(today)
  tomorrow.setDate(today.getDate() + 1)

  if (isSameDay(requestedDate, today)) {
    return 'Today'
  }

  if (isSameDay(requestedDate, tomorrow)) {
    return 'Tomorrow'
  }

  return requestedDate.toLocaleDateString([], {
    weekday: 'short',
    day: 'numeric',
    month: 'short',
  })
}

function formatClock(value) {
  const parsed = parseIsoDate(value)
  if (!parsed) {
    return 'Unknown'
  }
  return parsed.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function getRouteTypeLabels(route) {
  if (!Array.isArray(route?.type)) {
    return []
  }
  return route.type.map((typeKey) => ROUTE_TYPE_LABELS[typeKey] ?? typeKey)
}

function getLegArrivalIso(leg) {
  const departure = parseIsoDate(leg?.departure)
  if (!departure) {
    return null
  }

  const durationMinutes = Number(leg?.duration)
  if (!Number.isFinite(durationMinutes)) {
    return departure.toISOString()
  }

  return new Date(departure.getTime() + durationMinutes * 60 * 1000).toISOString()
}

function getRouteWindow(route) {
  const travel = Array.isArray(route?.travel) ? route.travel : []
  if (travel.length === 0) {
    return { start: 'Unknown', end: 'Unknown' }
  }

  const firstLeg = travel[0]
  const lastLeg = travel[travel.length - 1]
  return {
    start: formatClock(firstLeg?.departure),
    end: formatClock(getLegArrivalIso(lastLeg)),
  }
}

function normalizeTransportType(type) {
  if (typeof type !== 'string') {
    return 'unknown'
  }
  return type.toLowerCase()
}

function transportLabel(type) {
  const transportType = normalizeTransportType(type)
  return TRANSPORT_TYPE_LABELS[transportType] ?? transportType
}

function serviceLabel(leg, isWalking) {
  if (isWalking) {
    return 'Walking segment'
  }

  const service = leg?.service
  if (typeof service === 'string' && service.trim().length > 0) {
    return `Service ${service.trim()}`
  }

  return 'Service not provided'
}

function routeServicesSummary(route) {
  const travel = Array.isArray(route?.travel) ? route.travel : []
  if (travel.length === 0) {
    return 'via no services'
  }

  const includesWalking = travel.some((leg) => normalizeTransportType(leg?.type) === 'walking')
  const serviceNames = []

  travel.forEach((leg) => {
    const service = typeof leg?.service === 'string' ? leg.service.trim() : ''
    if (!service || serviceNames.includes(service)) {
      return
    }
    serviceNames.push(service)
  })

  if (serviceNames.length === 0) {
    return includesWalking ? 'via walking' : 'via service unknown'
  }

  return includesWalking
    ? `via ${serviceNames.join(', ')} + walk`
    : `via ${serviceNames.join(', ')}`
}

export default function RouteInformation({
  sourceStop,
  destinationStop,
  requestedDateTime,
  routes,
  nodes,
  selectedRouteIndex,
  onSelectRouteIndex,
  routeError,
  onBackToSelection,
}) {
  const availableRoutes = Array.isArray(routes) ? routes : []

  const nodeNameById = useMemo(() => {
    const lookup = {}
    if (!Array.isArray(nodes)) {
      return lookup
    }

    nodes.forEach((node) => {
      if (node?.id !== undefined && node?.id !== null) {
        lookup[String(node.id)] = node.name || `Stop ${node.id}`
      }
    })

    return lookup
  }, [nodes])

  const activeRouteIndex = Number.isInteger(selectedRouteIndex) ? selectedRouteIndex : 0

  const selectedRoute =
    availableRoutes.length > 0
      ? availableRoutes[Math.min(activeRouteIndex, availableRoutes.length - 1)]
      : null

  const sourceLabel = sourceStop?.name ?? 'Unknown source'
  const destinationLabel = destinationStop?.name ?? 'Unknown destination'
  const requestedDateLabel = formatRequestedDateLabel(requestedDateTime)

  const getNodeName = (nodeId) => {
    if (nodeId === undefined || nodeId === null) {
      return 'Unknown stop'
    }
    return nodeNameById[String(nodeId)] ?? `Stop ${nodeId}`
  }

  const getLegDestinationId = (route, legIndex) => {
    const pathSegment = Array.isArray(route?.path) ? route.path[legIndex] : null
    if (pathSegment?.to !== undefined && pathSegment?.to !== null) {
      return pathSegment.to
    }
    if (legIndex === (route?.travel?.length ?? 0) - 1) {
      return route?.destination
    }
    return null
  }

  return (
    <div className="route-info-container">
      <div className="route-info-header">
        <h2>Route Information</h2>
      </div>
      <div className="route-info-subtitle">
        {sourceLabel} → {destinationLabel} · {requestedDateLabel}
      </div>
      {routeError && <div className="route-info-error">{routeError}</div>}

      {availableRoutes.length === 0 ? (
        <div className="route-info-empty">No routes available for this journey.</div>
      ) : (
        <>
          <div className="route-options-list">
            {availableRoutes.map((route, routeIndex) => {
              const routeWindow = getRouteWindow(route)
              const routeTypes = getRouteTypeLabels(route)
              const legCount = Array.isArray(route?.travel) ? route.travel.length : 0
              const changes = Math.max(0, legCount - 1)
              const routeServicesText = routeServicesSummary(route)

              return (
                <button
                  key={`route-${routeIndex}`}
                  type="button"
                  className={`route-option-card ${activeRouteIndex === routeIndex ? 'selected' : ''}`}
                  onClick={() => onSelectRouteIndex?.(routeIndex)}
                >
                  <div className="route-option-times">{routeWindow.start} → {routeWindow.end}</div>
                  <div className="route-option-summary">
                    {route?.duration ?? 'Unknown'} min · {changes} change{changes === 1 ? '' : 's'}
                  </div>
                  <div className="route-option-services">{routeServicesText}</div>
                  {routeTypes.length > 0 && (
                    <div className="route-option-tags">
                      {routeTypes.map((typeLabel) => (
                        <span key={typeLabel} className="route-option-tag">{typeLabel}</span>
                      ))}
                    </div>
                  )}
                </button>
              )
            })}
          </div>

          {selectedRoute && (
            <div className="route-journey-details">
              <div className="route-journey-title">Journey steps</div>
              <ol className="route-journey-list">
                {(selectedRoute.travel ?? []).map((leg, legIndex) => {
                  const fromName = getNodeName(leg?.node)
                  const destinationId = getLegDestinationId(selectedRoute, legIndex)
                  const toName = getNodeName(destinationId)
                  const mode = transportLabel(leg?.type)
                  const departure = formatClock(leg?.departure)
                  const arrival = formatClock(getLegArrivalIso(leg))
                  const isWalking = normalizeTransportType(leg?.type) === 'walking'
                  const actionLabel = legIndex === 0 ? `Board ${mode}` : `Change to ${mode}`
                  const legServiceLabel = serviceLabel(leg, isWalking)

                  return (
                    <li key={`leg-${legIndex}-${leg?.node ?? 'unknown'}`} className="route-journey-item">
                      <div className="route-journey-time">{departure}</div>
                      <div className="route-journey-main">{isWalking ? `Walk from ${fromName}` : `${actionLabel} at ${fromName}`}</div>
                      <div className="route-journey-sub">To {toName} · Arrive {arrival}</div>
                      <div className="route-journey-meta">
                        <span className="route-journey-chip">{mode}</span>
                        <span className={`route-journey-chip ${isWalking ? 'walking' : 'service'}`}>{legServiceLabel}</span>
                        <span className="route-journey-chip">{leg?.duration ?? 'Unknown'} min</span>
                      </div>
                    </li>
                  )
                })}
              </ol>
            </div>
          )}
        </>
      )}

      <button type="button" className="route-info-selection" onClick={onBackToSelection}>
        Edit Route Criteria
      </button>
    </div>
  )
}
