/**
 * @author Sam / walkersi
 * @description API data structure helper functions based on specification in API documentation
 * @ai Copilot assisted
 */

// constants for route type strings that the API returns
// these should match the user preference selections
export const FASTEST = "fastest";
export const FEWEST_CHANGES = "fewest-changes";
export const LEAST_WALKING = "least-walking";

function toRouteTypes(route) {
    if (!Array.isArray(route?.type)) {
        return [];
    }
    return route.type;
}

function routePreferenceMatchCount(route, preferences) {
    const routeTypes = toRouteTypes(route);
    return preferences.reduce((count, preference) => {
        return count + (routeTypes.includes(preference) ? 1 : 0);
    }, 0);
}

function routeChangeCount(route) {
    if (!Array.isArray(route?.travel)) {
        return Number.MAX_SAFE_INTEGER;
    }
    return Math.max(0, route.travel.length - 1);
}

function routeWalkingMinutes(route) {
    if (!Array.isArray(route?.travel)) {
        return Number.MAX_SAFE_INTEGER;
    }
    return route.travel.reduce((total, leg) => {
        if (leg?.type === "walking") {
            return total + (Number(leg?.duration) || 0);
        }
        return total;
    }, 0);
}

function routeDurationMinutes(route) {
    const duration = Number(route?.duration);
    if (Number.isFinite(duration) && duration >= 0) {
        return duration;
    }
    return Number.MAX_SAFE_INTEGER;
}

function preferenceComparator(preference, firstRoute, secondRoute) {
    if (preference === FASTEST) {
        return routeDurationMinutes(firstRoute) - routeDurationMinutes(secondRoute);
    }
    if (preference === FEWEST_CHANGES) {
        return routeChangeCount(firstRoute) - routeChangeCount(secondRoute);
    }
    if (preference === LEAST_WALKING) {
        return routeWalkingMinutes(firstRoute) - routeWalkingMinutes(secondRoute);
    }
    return 0;
}

/**
 * Returns a sorted list of routes based on user preferences.
 * Index 0 is the best match for the user's preferences, and should be the default selection.
 * @param {Array of Route objects} routes selection of possible routes as returned by api-request requestRoutes(...)
 * @param {Array of String} preferences that the user has ticked, from ["fastest", "fewest-changes", "least-walking"]
 */
export function rankRoutes(routes, preferences) {
    if (!Array.isArray(routes)) {
        return [];
    }

    const selectedPreferences = Array.isArray(preferences) ? preferences : [];

    return [...routes].sort((firstRoute, secondRoute) => {
        const firstMatchCount = routePreferenceMatchCount(firstRoute, selectedPreferences);
        const secondMatchCount = routePreferenceMatchCount(secondRoute, selectedPreferences);

        if (firstMatchCount !== secondMatchCount) {
            return secondMatchCount - firstMatchCount;
        }

        for (const preference of selectedPreferences) {
            const preferenceDifference = preferenceComparator(preference, firstRoute, secondRoute);
            if (preferenceDifference !== 0) {
                return preferenceDifference;
            }
        }

        const durationDifference = routeDurationMinutes(firstRoute) - routeDurationMinutes(secondRoute);
        if (durationDifference !== 0) {
            return durationDifference;
        }

        const changeDifference = routeChangeCount(firstRoute) - routeChangeCount(secondRoute);
        if (changeDifference !== 0) {
            return changeDifference;
        }

        return routeWalkingMinutes(firstRoute) - routeWalkingMinutes(secondRoute);
    });
}


/**
 * Converts weather string (see API spec) to an emoji for UI display
 * @param {String} weather 
 * @return {String} emoji
 */
export function weatherEmoji(weather) {
    switch (weather) {
        case "clear":
        case "sunny":
            return "☀️";
        case "cloudy":
            return "☁️";
        case "rain":
            return "🌧️"
        case "snow":
            return "❄️";
        case "storm":
            return "⛈️";
        case "snowstorm":
            return "🌨️";
        default:
            return "❓";
    };
}