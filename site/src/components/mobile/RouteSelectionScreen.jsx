import React, { useState } from 'react'
import './RouteSelectionScreen.css'

export default function RouteSelectionScreen({
  fromLocation,
  toLocation,
  onRouteSelect,
  onBack,
}) {
  // Mock route data - replace with real data from backend
  const [routes] = useState([
    {
      id: 1,
      time: '30 min',
      type: 'primary', // solid blue
      stops: [
        { name: '1 Fleet Road', time: '9:15 AM' },
        { name: 'Malvern Avenue', time: '9:45 AM' },
      ],
    },
    {
      id: 2,
      time: '47 min',
      type: 'alternative', // dotted blue
      stops: [
        { name: 'Downtown Terminal', time: '9:20 AM' },
        { name: 'Malvern Avenue', time: '9:47 AM' },
      ],
    },
    {
      id: 3,
      time: '52 min',
      type: 'alternative', // dotted blue
      stops: [
        { name: 'Central Station', time: '9:25 AM' },
        { name: 'Malvern Avenue', time: '9:52 AM' },
      ],
    },
  ])

  return (
    <div className="route-selection-screen">
      <button className="back-button" onClick={onBack} title="Back">
        ←
      </button>

      <div className="map-area">
        <div className="map-placeholder">
          <p>Map with routes</p>
        </div>
      </div>

      <div className="location-display">
        <span className="location-name">{fromLocation}</span>
        <span className="arrow">↓</span>
        <span className="location-name">{toLocation}</span>
      </div>

      <div className="route-options">
        {routes.map((route) => (
          <button
            key={route.id}
            className={`route-button ${route.type}`}
            onClick={() => onRouteSelect(route)}
          >
            <span className="route-time">{route.time}</span>
          </button>
        ))}
      </div>
    </div>
  )
}