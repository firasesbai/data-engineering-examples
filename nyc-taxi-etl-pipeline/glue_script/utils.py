from datetime import datetime
import hashlib
import logging
from typing import Optional, Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_coordinates(lat: float, lon: float) -> bool:
    """
    Validate latitude and longitude coordinates for NYC area
    """
    try:
        # NYC approximate bounds
        nyc_lat_min, nyc_lat_max = 40.4, 41.0
        nyc_lon_min, nyc_lon_max = -74.3, -73.7
        
        return (nyc_lat_min <= lat <= nyc_lat_max and 
                nyc_lon_min <= lon <= nyc_lon_max)
    except (TypeError, ValueError):
        return False

def validate_monetary_amount(amount: Union[float, str]) -> bool:
    """
    Validate monetary amounts
    """
    try:
        amount_float = float(amount)
        return amount_float >= 0 and amount_float <= 10000  # Reasonable upper limit
    except (TypeError, ValueError):
        return False

def calculate_trip_duration(pickup_datetime: str, dropoff_datetime: str) -> Optional[float]:
    """
    Calculate trip duration in minutes between pickup and dropoff times
    
    Args:
        pickup_datetime: Pickup datetime string in format "YYYY-MM-DD HH:MM:SS"
        dropoff_datetime: Dropoff datetime string in format "YYYY-MM-DD HH:MM:SS"
    
    Returns:
        Duration in minutes as float, or None if calculation fails
    """
    try:
        if pickup_datetime is None or dropoff_datetime is None:
            logger.warning("Pickup or dropoff datetime is None")
            return None
        
        # Parse datetime strings
        pickup_dt = datetime.strptime(pickup_datetime, "%Y-%m-%d %H:%M:%S")
        dropoff_dt = datetime.strptime(dropoff_datetime, "%Y-%m-%d %H:%M:%S")
        
        # Validate that dropoff is after pickup
        if dropoff_dt <= pickup_dt:
            logger.warning(f"Dropoff time {dropoff_datetime} is not after pickup time {pickup_datetime}")
            return None
        
        # Calculate duration
        duration = dropoff_dt - pickup_dt
        duration_minutes = duration.total_seconds() / 60.0
        
        # Validate reasonable duration (0.1 to 24 hours)
        if duration_minutes < 0.1 or duration_minutes > 1440:
            logger.warning(f"Trip duration {duration_minutes} minutes is outside reasonable range")
            return None
        
        return round(duration_minutes, 2)
    
    except ValueError as e:
        logger.error(f"Invalid datetime format: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error calculating trip duration: {str(e)}")
        return None

def get_season(pickup_datetime: str) -> Optional[str]:
    """
    Determine season based on pickup date
    
    Args:
        pickup_datetime: Pickup datetime string in format "YYYY-MM-DD HH:MM:SS"
    
    Returns:
        Season name as string, or None if determination fails
    """
    try:
        if pickup_datetime is None:
            logger.warning("Pickup datetime is None")
            return None
        
        # Parse datetime string
        pickup_dt = datetime.strptime(pickup_datetime, "%Y-%m-%d %H:%M:%S")
        month = pickup_dt.month
        
        if month in [12, 1, 2]:
            return "Winter"
        elif month in [3, 4, 5]:
            return "Spring"
        elif month in [6, 7, 8]:
            return "Summer"
        elif month in [9, 10, 11]:
            return "Fall"
        else:
            logger.error(f"Invalid month: {month}")
            return "Unknown"
    
    except ValueError as e:
        logger.error(f"Invalid datetime format: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error determining season: {str(e)}")
        return None

def generate_trip_id(pickup_datetime: str, dropoff_datetime: str) -> Optional[str]:
    """
    Generate a unique trip ID based on pickup and dropoff times
    
    Args:
        pickup_datetime: Pickup datetime string in format "YYYY-MM-DD HH:MM:SS"
        dropoff_datetime: Dropoff datetime string in format "YYYY-MM-DD HH:MM:SS"
    
    Returns:
        Unique trip ID as string, or None if generation fails
    """
    try:
        if pickup_datetime is None or dropoff_datetime is None:
            logger.warning("Pickup or dropoff datetime is None")
            return None
        
        # Create a unique string from pickup and dropoff times
        unique_string = f"{pickup_datetime}_{dropoff_datetime}"
        
        # Generate hash
        trip_hash = hashlib.md5(unique_string.encode()).hexdigest()[:8]
        
        return f"trip_{trip_hash}"
    
    except Exception as e:
        logger.error(f"Error generating trip ID: {str(e)}")
        return None 