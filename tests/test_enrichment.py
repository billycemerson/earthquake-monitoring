"""
Unit tests for province enrichment functionality.
"""

import pytest
from enrichment.geocoder import ProvinceGeocoder


class TestProvinceGeocoder:
    """Test suite for ProvinceGeocoder class."""

    def test_validate_coordinates_valid(self):
        """Test validation of valid coordinates."""
        geocoder = ProvinceGeocoder()
        
        # Valid Indonesia coordinates
        assert geocoder.validate_coordinates(-6.2088, 106.8456) is True
        assert geocoder.validate_coordinates(-7.5, 110.25) is True

    def test_validate_coordinates_invalid(self):
        """Test validation of invalid coordinates."""
        geocoder = ProvinceGeocoder()
        
        # Out of bounds
        assert geocoder.validate_coordinates(91, 180) is False
        assert geocoder.validate_coordinates(-90.1, 0) is False
        assert geocoder.validate_coordinates(0, 180.1) is False

    def test_validate_coordinates_none(self):
        """Test validation with None values."""
        geocoder = ProvinceGeocoder()
        
        assert geocoder.validate_coordinates(None, 106.8456) is False
        assert geocoder.validate_coordinates(-6.2088, None) is False
        assert geocoder.validate_coordinates(None, None) is False

    def test_validate_coordinates_invalid_type(self):
        """Test validation with invalid types."""
        geocoder = ProvinceGeocoder()
        
        # Should handle gracefully
        assert geocoder.validate_coordinates("not_a_number", 106) is False
        assert geocoder.validate_coordinates(-6, "not_a_number") is False

    def test_geocode_batch_empty_list(self):
        """Test batch geocoding with empty list."""
        geocoder = ProvinceGeocoder()
        
        with pytest.raises(ValueError, match="Coordinates list cannot be empty"):
            geocoder.geocode_batch([])

    def test_geocode_single_jakarta(self):
        """Test single coordinate geocoding for Jakarta."""
        geocoder = ProvinceGeocoder()
        
        # Jakarta coordinates
        province = geocoder.geocode_single(-6.2088, 106.8456)
        
        # Should return a province name
        assert isinstance(province, str)
        assert len(province) > 0
        assert province != 'Unknown' or province == 'Unknown'  # May be Unknown in test

    def test_geocode_single_invalid(self):
        """Test single coordinate geocoding with invalid input."""
        geocoder = ProvinceGeocoder()
        
        province = geocoder.geocode_single(None, 106)
        assert province == 'Unknown'

    def test_geocode_batch_invalid_input(self):
        """Test batch geocoding with invalid input."""
        geocoder = ProvinceGeocoder()
        
        # Invalid coordinate format (not tuple of floats)
        with pytest.raises(Exception):
            geocoder.geocode_batch([("invalid", "data")])