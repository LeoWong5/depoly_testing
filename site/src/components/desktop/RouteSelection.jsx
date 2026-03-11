import React, { useEffect, useState } from 'react'
import './RouteSelection.css'

export default function RouteSelection({
  sourceStop,
  destinationStop,
  sourceQuery,
  destinationQuery,
  sourceSuggestions,
  destinationSuggestions,
  departureTime,
  preferences,
  mapPickTarget,
  mapSelectionVersion,
  onSourceQueryChange,
  onDestinationQueryChange,
  onSelectSourceSuggestion,
  onSelectDestinationSuggestion,
  onSetDepartureTime,
  onTogglePreference,
  onStartMapPickSource,
  onStartMapPickDestination,
  onCancelMapPick,
  onFindRoutes,
  isFindingRoutes,
  routeError,
}) {
  const [isSourceDropdownOpen, setIsSourceDropdownOpen] = useState(false)
  const [isDestinationDropdownOpen, setIsDestinationDropdownOpen] = useState(false)

  useEffect(() => {
    setIsSourceDropdownOpen(false)
    setIsDestinationDropdownOpen(false)
  }, [mapSelectionVersion])

  const currentSourceLabel = sourceStop?.name ?? 'Not set'
  const currentDestinationLabel = destinationStop?.name ?? 'Not set'

  const sourceMapButtonLabel = sourceStop ? '🗺 Change on map' : '🗺 Set on map'
  const destinationMapButtonLabel = destinationStop ? '🗺 Change on map' : '🗺 Set on map'

  return (
    <div className="route-selection-container">
      <div className="route-selection-header">
        <h2>Route Selection</h2>
      </div>

      <div className="route-current-summary">
        <div className="route-current-row"><span>Source:</span><strong className="route-current-value">{currentSourceLabel}</strong></div>
        <div className="route-current-row"><span>Destination:</span><strong className="route-current-value">{currentDestinationLabel}</strong></div>
        <div className="route-current-row"><span>Time:</span><strong>{departureTime || 'Not set'}</strong></div>
      </div>
      <div className="route-selection-hint">Search by name or pick directly from the map for quick setup.</div>

      <div className="route-selection-section">
        <label htmlFor="route-source" className="route-selection-label">Source</label>
        <div className="route-search-wrapper">
          {sourceQuery && (
            <button
              type="button"
              className="route-search-clear"
              onClick={() => onSourceQueryChange('')}
              aria-label="Clear source"
            >
              ✕
            </button>
          )}
          <input
            id="route-source"
            type="text"
            className="route-selection-input route-selection-search-input"
            placeholder="Search source stop"
            value={sourceQuery}
            onFocus={() => setIsSourceDropdownOpen(true)}
            onChange={(event) => {
              onSourceQueryChange(event.target.value)
              setIsSourceDropdownOpen(true)
            }}
          />
          {sourceQuery && sourceSuggestions.length > 0 && isSourceDropdownOpen && (
            <div className="route-search-suggestions">
              {sourceSuggestions.map((suggestion) => (
                <div
                  key={suggestion.id}
                  className="route-search-suggestion-item"
                  onClick={() => {
                    onSelectSourceSuggestion(suggestion)
                    setIsSourceDropdownOpen(false)
                  }}
                >
                  {suggestion.name}
                </div>
              ))}
            </div>
          )}
        </div>
        <button
          type="button"
          className={`route-map-pick-button ${mapPickTarget === 'source' ? 'active' : ''}`}
          onClick={mapPickTarget === 'source' ? onCancelMapPick : onStartMapPickSource}
        >
          {mapPickTarget === 'source' ? '✕ Cancel map pick' : sourceMapButtonLabel}
        </button>
      </div>

      <div className="route-selection-section">
        <label htmlFor="route-destination" className="route-selection-label">Destination</label>
        <div className="route-search-wrapper">
          {destinationQuery && (
            <button
              type="button"
              className="route-search-clear"
              onClick={() => onDestinationQueryChange('')}
              aria-label="Clear destination"
            >
              ✕
            </button>
          )}
          <input
            id="route-destination"
            type="text"
            className="route-selection-input route-selection-search-input"
            placeholder="Search destination stop"
            value={destinationQuery}
            onFocus={() => setIsDestinationDropdownOpen(true)}
            onChange={(event) => {
              onDestinationQueryChange(event.target.value)
              setIsDestinationDropdownOpen(true)
            }}
          />
          {destinationQuery && destinationSuggestions.length > 0 && isDestinationDropdownOpen && (
            <div className="route-search-suggestions">
              {destinationSuggestions.map((suggestion) => (
                <div
                  key={suggestion.id}
                  className="route-search-suggestion-item"
                  onClick={() => {
                    onSelectDestinationSuggestion(suggestion)
                    setIsDestinationDropdownOpen(false)
                  }}
                >
                  {suggestion.name}
                </div>
              ))}
            </div>
          )}
        </div>
        <button
          type="button"
          className={`route-map-pick-button ${mapPickTarget === 'destination' ? 'active' : ''}`}
          onClick={mapPickTarget === 'destination' ? onCancelMapPick : onStartMapPickDestination}
        >
          {mapPickTarget === 'destination' ? '✕ Cancel map pick' : destinationMapButtonLabel}
        </button>
      </div>

      <div className="route-selection-section">
        <label htmlFor="route-time" className="route-selection-label">Departure Time</label>
        <input
          id="route-time"
          type="datetime-local"
          className="route-selection-input"
          value={departureTime}
          onChange={(event) => onSetDepartureTime(event.target.value)}
        />
      </div>

      <div className="route-selection-section route-preferences-section">
        <div className="route-selection-label">Preferences</div>
        <label className="route-preference-item">
          <input
            type="checkbox"
            checked={preferences.includes('fastest')}
            onChange={(event) => onTogglePreference('fastest', event.target.checked)}
          />
          Fastest
        </label>
        <label className="route-preference-item">
          <input
            type="checkbox"
            checked={preferences.includes('fewest-changes')}
            onChange={(event) => onTogglePreference('fewest-changes', event.target.checked)}
          />
          Fewest Changes
        </label>
        <label className="route-preference-item">
          <input
            type="checkbox"
            checked={preferences.includes('least-walking')}
            onChange={(event) => onTogglePreference('least-walking', event.target.checked)}
          />
          Least Walking
        </label>
      </div>

      {routeError && <div className="route-selection-error">{routeError}</div>}
      <button
        type="button"
        className="route-selection-button"
        onClick={onFindRoutes}
        disabled={isFindingRoutes}
      >
        {isFindingRoutes ? 'Finding…' : 'Find Routes'}
      </button>
    </div>
  )
}
