import React, { useState } from 'react'
import './BusScheduleScreen.css'

export default function BusScheduleScreen({
  fromLocation,
  toLocation,
  selectedRoute,
  onBack,
  onChangeLocations,
}) {
  const [searchInput, setSearchInput] = useState('')

  const suggestions = [
    'Main Street Station',
    'Central Park',
    'Downtown Terminal',
    'Airport Station',
    'University Ave',
  ]

  const filteredSuggestions = suggestions.filter((s) =>
    s.toLowerCase().includes(searchInput.toLowerCase())
  )

  const schedules = [
    { time: '9:15 AM', bus: 'Bus scheduled at 9:15 AM' },
    { time: '9:45 AM', bus: 'Bus scheduled at 9:45 AM' },
    { time: '10:15 AM', bus: 'Bus scheduled at 10:15 AM' },
    { time: '10:45 AM', bus: 'Bus scheduled at 10:45 AM' },
  ]

  return (
    <div className="bus-schedule-screen">
      <button className="back-button" onClick={onBack} title="Back">
        ←
      </button>

      <div className="schedule-map-area">
        <div className="map-placeholder">
          <p>Route Map</p>
        </div>
      </div>

      <div className="search-bar-container">
        <input
          type="text"
          placeholder="Search stops..."
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          className="search-input"
        />
        {searchInput && filteredSuggestions.length > 0 && (
          <div className="suggestions">
            {filteredSuggestions.map((suggestion, index) => (
              <div
                key={index}
                className="suggestion-item"
                onClick={() => setSearchInput(suggestion)}
              >
                {suggestion}
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="location-editor">
        <button className="edit-location-button" onClick={onChangeLocations}>
          📍 {fromLocation} → {toLocation}
        </button>
      </div>

      <div className="schedules-list">
        <h3>Next Buses</h3>
        {schedules.map((schedule, index) => (
          <div key={index} className="schedule-item">
            <span className="schedule-time">{schedule.time}</span>
            <span className="schedule-bus">{schedule.bus}</span>
          </div>
        ))}
      </div>
    </div>
  )
}