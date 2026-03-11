import React, { useEffect, useState } from 'react'
import './StopInformation.css'

function formatArrivalTime(value) {
  const arrivalTime = new Date(value)
  return Number.isNaN(arrivalTime.getTime())
    ? 'Unknown time'
    : arrivalTime.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function formatStatus(value) {
  if (!value) {
    return 'unknown'
  }
  return value.replace('-', ' ')
}

function formatStopLocationTooltip(location) {
  const lat = Number(location?.lat)
  const long = Number(location?.long)

  if (!Number.isFinite(lat) || !Number.isFinite(long)) {
    return 'Location unavailable'
  }

  return `Lat ${lat.toFixed(5)}, Long ${long.toFixed(5)}`
}

export default function StopInformation({
  nearbyStops,
  selectedStop,
  selectedStopTimetable,
  selectedStopTimetableLoading,
  selectedStopTimetableSaving,
  onSelectStop,
  onCloseStopDetails,
  onCenterStop,
  onRefreshTimetable,
  onSaveTimetable,
  onRouteToFromStop,
  onRouteFromStop,
}) {
  const [isRefreshCoolingDown, setIsRefreshCoolingDown] = useState(false)

  useEffect(() => {
    setIsRefreshCoolingDown(false)
  }, [selectedStop?.id])

  const handleRefreshClick = async () => {
    if (isRefreshCoolingDown || selectedStopTimetableLoading) {
      return
    }

    setIsRefreshCoolingDown(true)
    try {
      await onRefreshTimetable?.()
    } finally {
      setTimeout(() => {
        setIsRefreshCoolingDown(false)
      }, 5000)
    }
  }

  if (selectedStop) {
    const arrivals = selectedStopTimetable?.arrivals ?? []

    return (
      <div className="stop-info-container">
        <div className="stop-header-row">
          <h2>Stop Information</h2>
          <button className="stop-close-button" type="button" onClick={onCloseStopDetails} aria-label="Back to nearby stops">
            Nearby Stops
          </button>
        </div>

        <div className="stop-detail-card">
          <div
            className="stop-detail-title"
            title={formatStopLocationTooltip(selectedStop.location)}
          >
            {selectedStop.name || `Stop ${selectedStop.id}`}
          </div>
        </div>

        <div className="stop-timetable-section">
          <div className="stop-timetable-header">
            <div className="stop-timetable-title">Upcoming Arrivals (24h)</div>
            <div className="stop-timetable-actions">
              <button
                className="timetable-refresh-button"
                type="button"
                aria-label="Refresh timetable"
                title="Refresh timetable"
                onClick={handleRefreshClick}
                disabled={selectedStopTimetableLoading || isRefreshCoolingDown}
              >
                ↻
              </button>
              <button
                className="timetable-save-button"
                type="button"
                onClick={() => onSaveTimetable?.()}
                disabled={selectedStopTimetableSaving || selectedStopTimetableLoading}
              >
                💾 Save
              </button>
            </div>
          </div>
          {selectedStopTimetableLoading ? (
            <div className="stop-timetable-placeholder">Loading timetable...</div>
          ) : arrivals.length > 0 ? (
            <div className="arrival-list">
              {arrivals.slice(0, 8).map((arrival) => (
                <div key={`${arrival.time}-${arrival.name}-${arrival.operator}`} className="arrival-row">
                  <div className="arrival-time">{formatArrivalTime(arrival.time)}</div>
                  <div className="arrival-details">
                    <div className="arrival-main-row">
                      <span className="arrival-service">{arrival.name ?? 'Service'}</span>
                      <span className="arrival-status">({formatStatus(arrival.status)})</span>
                    </div>
                    <div className="arrival-operator">{arrival.operator ?? 'Operator unavailable'}</div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="stop-timetable-placeholder">No upcoming arrivals in the next 24 hours.</div>
          )}
        </div>

        <div className="stop-action-row">
          <button className="route-to-button" type="button" onClick={onCenterStop}>
            Center on Map
          </button>
          <button className="route-to-button" type="button" onClick={() => onRouteFromStop?.()}>
            Set as Start
          </button>
          <button className="route-to-button" type="button" onClick={() => onRouteToFromStop?.()}>
            Set as End
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="stop-info-container">
      <h2>Nearby Stops</h2>
      <div className="stop-list-helper">Choose a stop to see arrivals, save timetables, and begin route planning.</div>
      <div className="stops-list">
        {nearbyStops.map((stop) => (
          <button
            type="button"
            key={stop.id}
            className="stop-item"
            onClick={() => onSelectStop(stop.id)}
          >
            <div className="stop-icon">🚌</div>
            <div className="stop-details">
              <div className="stop-name" title={formatStopLocationTooltip(stop.location)}>{stop.name}</div>
              <div className="stop-distance">{stop.distanceLabel}</div>
              <div className="stop-next-arrival">{stop.nextArrivalSummary}</div>
            </div>
          </button>
        ))}
        {nearbyStops.length === 0 && (
          <div className="stop-empty-state">No nearby stops were found for this area.</div>
        )}
      </div>
    </div>
  )
}