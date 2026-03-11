import React from 'react'
import './SearchLocationScreen.css'

export default function SearchLocationScreen({
  fromLocation,
  toLocation,
  preferences,
  onFromChange,
  onToChange,
  onPreferencesChange,
  onGoClick,
}) {
  const handlePreferenceChange = (pref) => {
    onPreferencesChange({
      fewestChanges: pref === 'fewestChanges',
      shortestTime: pref === 'shortestTime',
      shortestDistance: pref === 'shortestDistance',
    })
  }

  return (
    <div className="search-location-screen">
      <div className="screen-content">
        <div className="location-inputs">
          <div className="input-group">
            <div className="location-icon from-icon">📍</div>
            <input
              type="text"
              placeholder="From..."
              value={fromLocation}
              onChange={(e) => onFromChange(e.target.value)}
              className="location-input"
            />
          </div>

          <div className="input-group">
            <div className="location-icon to-icon">📍</div>
            <input
              type="text"
              placeholder="To..."
              value={toLocation}
              onChange={(e) => onToChange(e.target.value)}
              className="location-input"
            />
          </div>
        </div>

        <div className="preferences">
          <label className="preference-item">
            <input
              type="checkbox"
              checked={preferences.fewestChanges}
              onChange={() => handlePreferenceChange('fewestChanges')}
            />
            <span>Fewest changes</span>
          </label>

          <label className="preference-item">
            <input
              type="checkbox"
              checked={preferences.shortestTime}
              onChange={() => handlePreferenceChange('shortestTime')}
            />
            <span>Shortest Time</span>
          </label>

          <label className="preference-item">
            <input
              type="checkbox"
              checked={preferences.shortestDistance}
              onChange={() => handlePreferenceChange('shortestDistance')}
            />
            <span>Shortest Distance</span>
          </label>
        </div>

        <button
          className="go-button"
          onClick={onGoClick}
          disabled={!fromLocation || !toLocation}
        >
          Go
        </button>
      </div>
    </div>
  )
}