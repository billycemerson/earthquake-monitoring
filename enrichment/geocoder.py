"""
Reverse geocoder utility for province enrichment.

Provides batch geocoding functionality to convert coordinates to province
names using the reverse_geocoder library.
"""

import logging
from typing import List, Tuple, Optional, Dict

try:
    import reverse_geocoder as rg
except ImportError:
    raise ImportError(
        "reverse_geocoder not installed. "
        "Install with: pip install reverse-geocoder"
    )

log = logging.getLogger(__name__)


class ProvinceGeocoder:
    """
    Handles reverse geocoding of coordinates to province names.
    
    Uses the reverse_geocoder library to batch process coordinates
    and extract province (admin1) information.
    """

    @staticmethod
    def geocode_batch(coordinates: List[Tuple[float, float]]) -> List[str]:
        """
        Geocode a batch of coordinates to province names.
        
        Args:
            coordinates: List of (latitude, longitude) tuples
            
        Returns:
            List of province names corresponding to each coordinate
            
        Raises:
            ValueError: If coordinates list is empty or invalid
        """
        if not coordinates:
            raise ValueError("Coordinates list cannot be empty")

        try:
            results = rg.search(coordinates)
            provinces = [
                result[0].get('admin1', 'Unknown') 
                for result in results
            ]
            return provinces
        except Exception as e:
            log.error(f"Error during geocoding: {e}")
            raise

    @staticmethod
    def geocode_single(latitude: float, longitude: float) -> str:
        """
        Geocode a single coordinate to province name.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            
        Returns:
            Province name
        """
        try:
            result = rg.search([(latitude, longitude)])
            return result[0].get('admin1', 'Unknown')
        except Exception as e:
            log.error(f"Error geocoding ({latitude}, {longitude}): {e}")
            return 'Unknown'

    @staticmethod
    def validate_coordinates(
        latitude: Optional[float], 
        longitude: Optional[float]
    ) -> bool:
        """
        Validate coordinate values.
        
        Args:
            latitude: Latitude value
            longitude: Longitude value
            
        Returns:
            True if coordinates are valid, False otherwise
        """
        if latitude is None or longitude is None:
            return False
        
        try:
            lat = float(latitude)
            lon = float(longitude)
            return -90 <= lat <= 90 and -180 <= lon <= 180
        except (ValueError, TypeError):
            return False