"""
weather_service.py
==================
Weather data fetching module for the Transport Application.

Fetches real-time weather information from Lancaster University's weather service.
Provides temperature, conditions, wind speed, and other meteorological data.

Usage:
    from weather_service import WeatherService
    
    weather_service = WeatherService()
    weather = weather_service.get_weather(latitude=54.05, longitude=-2.80)
"""

import requests
import json
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, TypedDict
import logging


class GeoLocation(TypedDict):
    lat: float
    long: float


class WeatherObject(TypedDict):
    """
    Weather object as defined in the API specification.

    'weather' is one of:
        "unknown", "sunny", "clear", "cloudy",
        "rain", "storm", "snow", "snowstorm"
    'temperature' is degrees Celsius (omitted when weather is "unknown").
    'location' is a Geo-Location object.
    """
    weather: str
    temperature: Optional[float]
    location: GeoLocation


# Maps raw condition strings from the upstream API to the spec enum values.
_CONDITION_TO_SPEC: Dict[str, str] = {
    "clear":        "clear",
    "sunny":        "sunny",
    "clouds":       "cloudy",
    "mist":         "cloudy",
    "haze":         "cloudy",
    "fog":          "cloudy",
    "smoke":        "cloudy",
    "dust":         "cloudy",
    "sand":         "cloudy",
    "ash":          "cloudy",
    "squall":       "cloudy",
    "drizzle":      "rain",
    "rain":         "rain",
    "thunderstorm": "storm",
    "tornado":      "storm",
    "snow":         "snow",
    "sleet":        "snow",
    "blizzard":     "snowstorm",
}

# logger = logging.getLogger(__name__)


class Weather_Service:
    """
    Fetches dynamic weather information from Lancaster University's weather service.
    
    Provides methods to retrieve current weather conditions for specific coordinates.
    Weather data is fetched in real-time on demand for user-requested locations.
    """
    
    # Weather service endpoint
    BASE_URL = "https://transport.scc.lancs.ac.uk/weather"

    # Maximum number of unique locations to keep in cache
    _MAX_CACHE_SIZE = 1 << 8

    def __init__(self, base_url: str = "", timeout: int = 5, cache_duration_minutes: int = 5):
        """
        Initialize the WeatherService.

        Args:
            base_url: Override default weather service URL
            timeout: Request timeout in seconds
            cache_duration_minutes: How long to cache weather data
        """
        # Use instance attribute so multiple instances don't share the same URL
        self._base_url = base_url if base_url else self.BASE_URL
        self._timeout = timeout
        # OrderedDict gives O(1) LRU eviction (move_to_end + popitem)
        self._cache: OrderedDict = OrderedDict()
        self._cache_duration = timedelta(minutes=cache_duration_minutes)
        
    def get_weather(self, latitude: float, longitude: float, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        Fetch real-time weather information for given coordinates.
        
        Args:
            latitude: Latitude of the location
            longitude: Longitude of the location
            use_cache: Whether to use cached data if available
            
        Returns:
            Dictionary containing weather information:
            {
                "temperature": float (°C),
                "condition": str (e.g., "Clear", "Rainy"),
                "wind_speed": float (km/h),
                "humidity": int (%),
                "timestamp": str (ISO format),
                "latitude": float,
                "longitude": float,
                "cached": bool
            }
            
            Returns None if the request fails.
        """
        return None

        
        try:
            cache_key = f"{latitude}_{longitude}"

            # Return cached entry if still fresh, promoting it to most-recent
            if use_cache and cache_key in self._cache:
                cached_data, cached_time = self._cache[cache_key]
                if datetime.now() - cached_time < self._cache_duration:
                    self._cache.move_to_end(cache_key)
                    return {**cached_data, "cached": True}

            # logger.info(f"Fetching weather for coordinates ({latitude}, {longitude})")
            response = requests.get(
                self._base_url,
                params={"lat": latitude, "lon": longitude},
                timeout=self._timeout,
            )
            response.raise_for_status()

            standardized_data = self._standardize_response(response.json(), latitude, longitude)

            # Evict oldest entry when cache is full
            if len(self._cache) >= self._MAX_CACHE_SIZE:
                self._cache.popitem(last=False)

            self._cache[cache_key] = (standardized_data.copy(), datetime.now())
            self._cache.move_to_end(cache_key)

            # logger.info(f"Successfully fetched weather for ({latitude}, {longitude})")
            return {**standardized_data, "cached": False}

        except requests.exceptions.Timeout:
            # logger.error(f"Weather service timed out for ({latitude}, {longitude})")
            return None
        except requests.exceptions.RequestException as e:
            # logger.error(f"Weather service request failed for ({latitude}, {longitude}): {e}")
            return None
        except (json.JSONDecodeError, ValueError) as e:
            # logger.error(f"Failed to parse weather response: {e}")
            return None
    
    def _standardize_response(self, data: Dict[str, Any], latitude: float, longitude: float) -> Dict[str, Any]:
        """
        Standardize weather API response to common format.
        
        Args:
            data: Raw response from weather API
            latitude: Location latitude
            longitude: Location longitude
            
        Returns:
            Standardized weather data dictionary
        """
        weather_root = data.get("weather", data)
        main_data = weather_root.get("main", {}) if isinstance(weather_root, dict) else {}
        wind_data = weather_root.get("wind", {}) if isinstance(weather_root, dict) else {}
        weather_items = weather_root.get("weather", []) if isinstance(weather_root, dict) else []

        condition = "Unknown"
        if isinstance(weather_items, list) and weather_items:
            first = weather_items[0]
            if isinstance(first, dict):
                condition = first.get("main") or first.get("description") or "Unknown"

        standardized = {
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": datetime.now().isoformat(),
            "temperature": main_data.get("temp", data.get("temperature")),
            "condition": condition,
            "wind_speed": wind_data.get("speed", data.get("wind_speed")),
            "humidity": main_data.get("humidity", data.get("humidity")),
            "pressure": main_data.get("pressure", data.get("pressure")),
            "precipitation": data.get("precipitation"),
        }
        return standardized
    
    def clear_cache(self):
        """Clear all cached weather data."""
        self._cache.clear()
        # logger.info("Weather cache cleared")
    
    def get_weather_obj(self, lon: float, lat: float, use_cache: bool = True) -> WeatherObject:
        """
        Return weather data in the API spec WeatherObject format.

        Calls get_weather() internally and converts:
          - 'condition'   → 'weather'     (mapped to spec enum)
          - 'temperature' → 'temperature' (degrees Celsius)
          - lat/lon       → 'location'    ({lat, long})

        If the upstream fetch fails, returns a minimal object with
        weather="unknown" and no temperature.
        """
        raw = self.get_weather(lat, lon, use_cache=use_cache)

        if raw is None:
            return WeatherObject(
                weather="unknown",
                temperature=None,
                location=GeoLocation(lat=lat, long=lon),
            )

        condition_raw = (raw.get("condition") or "").strip().lower()
        weather_str = _CONDITION_TO_SPEC.get(condition_raw, "unknown")

        obj = WeatherObject(
            weather=weather_str,
            temperature=raw.get("temperature"),
            location=GeoLocation(lat=lat, long=lon),
        )
        # Omit temperature when weather is unknown, as per spec
        if weather_str == "unknown":
            obj.pop("temperature", None)
        return obj

    def get_multiple_locations(self, locations: list) -> list:
        """
        Fetch weather for multiple locations.
        
        Args:
            locations: List of tuples [(lat1, lon1), (lat2, lon2), ...]
            
        Returns:
            List of weather data dictionaries for each location
        """
        results = []
        for lat, lon in locations:
            weather = self.get_weather(lat, lon)
            if weather:
                results.append(weather)
            # else:
                # logger.warning(f"Failed to fetch weather for location ({lat}, {lon})")
        return results
