"""
Reverse geocoder utility for province enrichment.

Provides batch geocoding functionality to convert coordinates to province
names using the reverse_geocoder library.
"""

import logging
from typing import List, Tuple, Optional, Dict
import pandas as pd

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
    
    Safely handles NA/NaN values and different result structures.
    """

    @staticmethod
    def _safe_str(value) -> str:
        """
        Safely convert any value to string, handling NaN/None/NA types.
        
        Returns:
            Clean string or 'Unknown' if value is null-like
        """
        if value is None:
            return 'Unknown'
        
        # Handle pandas NA/NaN types
        if pd.isna(value):
            return 'Unknown'
        
        # Convert to string and clean
        str_val = str(value).strip()
        
        # Filter out common null representations
        if str_val.lower() in ('nan', 'none', 'null', ''):
            return 'Unknown'
        
        return str_val

    @staticmethod
    def geocode_batch(coordinates: List[Tuple[float, float]]) -> List[str]:
        """
        Geocode a batch of coordinates to province names.
        
        Args:
            coordinates: List of (latitude, longitude) tuples
            
        Returns:
            List of province names corresponding to each coordinate
            
        Raises:
            ValueError: If coordinates list is empty
        """
        if not coordinates:
            raise ValueError("Coordinates list cannot be empty")

        try:
            # Call reverse_geocoder
            results = rg.search(coordinates)
            
            provinces = []
            for result in results:
                province = ProvinceGeocoder._extract_province(result)
                provinces.append(province)
            
            log.info(f"Successfully geocoded {len(provinces)} coordinates")
            return provinces
            
        except Exception as e:
            log.error(f"Error during geocoding: {type(e).__name__}: {e}")
            raise

    @staticmethod
    def _extract_province(result) -> str:
        """
        Extract province from reverse_geocoder result.
        
        Handles different possible return formats and NA values.
        
        Args:
            result: A single result from rg.search()
            
        Returns:
            Province name as string, or 'Unknown' if not found
        """
        if not isinstance(result, (list, tuple)):
            return ProvinceGeocoder._safe_str(result)
        
        if len(result) == 0:
            return 'Unknown'
        
        # Try common indices for admin1_name (province)
        # reverse_geocoder typically: (lat, lon, admin1_code, admin1_name, country_code, country_name)
        candidates = []
        
        # Index 3: Most common for admin1_name
        if len(result) > 3:
            candidates.append(result[3])
        
        # Index 1: Sometimes province is here
        if len(result) > 1:
            candidates.append(result[1])
        
        # Index 2: Try admin1_code as fallback
        if len(result) > 2:
            candidates.append(result[2])
        
        # Try any other string elements
        for item in result:
            if isinstance(item, str):
                candidates.append(item)
        
        # Return first non-null candidate
        for candidate in candidates:
            province = ProvinceGeocoder._safe_str(candidate)
            if province != 'Unknown':
                return province
        
        return 'Unknown'

    @staticmethod
    def geocode_single(latitude: float, longitude: float) -> str:
        """
        Geocode a single coordinate to province name.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            
        Returns:
            Province name (or 'Unknown' if geocoding fails)
        """
        try:
            result = rg.search([(latitude, longitude)])
            if result and len(result) > 0:
                province = ProvinceGeocoder._extract_province(result[0])
                return province
            return 'Unknown'
        except Exception as e:
            log.error(f"Error geocoding ({latitude}, {longitude}): {type(e).__name__}: {e}")
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
            # Handle pandas NA
            if pd.isna(latitude) or pd.isna(longitude):
                return False
            
            lat = float(latitude)
            lon = float(longitude)
            return -90 <= lat <= 90 and -180 <= lon <= 180
        except (ValueError, TypeError):
            return False
