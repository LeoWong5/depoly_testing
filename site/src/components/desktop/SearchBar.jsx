import React, { useState } from 'react'
import './SearchBar.css'

export default function SearchBar({ value, onChange, suggestions = [], onSelectSuggestion }) {
  const [isDropdownOpen, setIsDropdownOpen] = useState(true)

  const handleInputChange = (event) => {
    onChange(event.target.value)
    setIsDropdownOpen(true)
  }

  const handleClearInput = () => {
    onChange('')
    setIsDropdownOpen(false)
  }

  const handleSuggestionSelect = (suggestion) => {
    onSelectSuggestion?.(suggestion)
    setIsDropdownOpen(false)
  }

  return (
    <div className="search-bar-container">
      <div className={`search-input-wrapper ${value ? 'has-clear-button' : ''}`}>
        {value && (
          <button className="search-clear-button" type="button" onClick={handleClearInput} aria-label="Clear search">
            ✕
          </button>
        )}
        <input
          type="text"
          placeholder="Search stops..."
          value={value}
          onChange={handleInputChange}
          onFocus={() => setIsDropdownOpen(true)}
          className="search-input"
        />
      </div>
      {value && suggestions.length > 0 && isDropdownOpen && (
        <div className="suggestions">
          {suggestions.map((suggestion) => (
            <div
              key={suggestion.id}
              className="suggestion-item"
              onClick={() => handleSuggestionSelect(suggestion)}
            >
              {suggestion.name}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}