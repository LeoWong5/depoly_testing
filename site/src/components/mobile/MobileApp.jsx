import React, { useState } from 'react'
import './MobileApp.css'
import SearchLocationScreen from './SearchLocationScreen'
import RouteSelectionScreen from './RouteSelectionScreen'
import BusScheduleScreen from './BusScheduleScreen'

export default function MobileApp() {
  const [currentScreen, setCurrentScreen] = useState('search') // 'search', 'routes', 'schedule'
  const [fromLocation, setFromLocation] = useState('')
  const [toLocation, setToLocation] = useState('')
  const [preferences, setPreferences] = useState({
    fewestChanges: true,
    shortestTime: false,
    shortestDistance: false,
  })
  const [selectedRoute, setSelectedRoute] = useState(null)

  const handleGoClick = () => {
    if (fromLocation && toLocation) {
      setCurrentScreen('routes')
    }
  }

  const handleRouteSelect = (route) => {
    setSelectedRoute(route)
    setCurrentScreen('schedule')
  }

  const handleBackToRoutes = () => {
    setCurrentScreen('routes')
    setSelectedRoute(null)
  }

  const handleBackToSearch = () => {
    setCurrentScreen('search')
    setSelectedRoute(null)
  }

  return (
    <div className="mobile-app">
      {currentScreen === 'search' && (
        <SearchLocationScreen
          fromLocation={fromLocation}
          toLocation={toLocation}
          preferences={preferences}
          onFromChange={setFromLocation}
          onToChange={setToLocation}
          onPreferencesChange={setPreferences}
          onGoClick={handleGoClick}
        />
      )}

      {currentScreen === 'routes' && (
        <RouteSelectionScreen
          fromLocation={fromLocation}
          toLocation={toLocation}
          onRouteSelect={handleRouteSelect}
          onBack={handleBackToSearch}
        />
      )}

      {currentScreen === 'schedule' && (
        <BusScheduleScreen
          fromLocation={fromLocation}
          toLocation={toLocation}
          selectedRoute={selectedRoute}
          onBack={handleBackToRoutes}
          onChangeLocations={handleBackToSearch}
        />
      )}
    </div>
  )
}