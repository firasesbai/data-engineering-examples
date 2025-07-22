from glue_script.utils import (
    calculate_trip_duration, get_season, generate_trip_id,
    validate_coordinates, validate_monetary_amount
)

class TestTaxiTransformations:
    
    def test_calculate_trip_duration_normal(self):
        """Test normal trip duration calculation"""
        pickup = "2024-01-15 08:30:00"
        dropoff = "2024-01-15 08:45:00"
        
        duration = calculate_trip_duration(pickup, dropoff)
        
        assert duration == 15.0
    
    def test_calculate_trip_duration_long_trip(self):
        """Test longer trip duration calculation"""
        pickup = "2024-01-15 10:00:00"
        dropoff = "2024-01-15 10:25:00"
        
        duration = calculate_trip_duration(pickup, dropoff)
        
        assert duration == 25.0
    
    def test_calculate_trip_duration_none_values(self):
        """Test trip duration with None values"""
        duration = calculate_trip_duration(None, "2024-01-15 08:45:00")
        assert duration is None
        
        duration = calculate_trip_duration("2024-01-15 08:30:00", None)
        assert duration is None
    
    def test_calculate_trip_duration_invalid_format(self):
        """Test trip duration with invalid datetime format"""
        duration = calculate_trip_duration("invalid", "2024-01-15 08:45:00")
        assert duration is None
    
    def test_calculate_trip_duration_invalid_order(self):
        """Test trip duration when dropoff is before pickup"""
        pickup = "2024-01-15 08:45:00"
        dropoff = "2024-01-15 08:30:00"
        
        duration = calculate_trip_duration(pickup, dropoff)
        assert duration is None
    
    def test_calculate_trip_duration_too_short(self):
        """Test trip duration that's too short (less than 0.1 minutes)"""
        pickup = "2024-01-15 08:30:00"
        dropoff = "2024-01-15 08:30:05"  # 5 seconds
        
        duration = calculate_trip_duration(pickup, dropoff)
        assert duration is None
    
    def test_calculate_trip_duration_too_long(self):
        """Test trip duration that's too long (more than 24 hours)"""
        pickup = "2024-01-15 08:30:00"
        dropoff = "2024-01-16 09:30:00"  # 25 hours
        
        duration = calculate_trip_duration(pickup, dropoff)
        assert duration is None
    
    def test_get_season_winter(self):
        """Test season classification for winter months"""
        assert get_season("2024-12-15 08:30:00") == "Winter"
        assert get_season("2024-01-15 08:30:00") == "Winter"
        assert get_season("2024-02-15 08:30:00") == "Winter"
    
    def test_get_season_spring(self):
        """Test season classification for spring months"""
        assert get_season("2024-03-15 08:30:00") == "Spring"
        assert get_season("2024-04-15 08:30:00") == "Spring"
        assert get_season("2024-05-15 08:30:00") == "Spring"
    
    def test_get_season_summer(self):
        """Test season classification for summer months"""
        assert get_season("2024-06-15 08:30:00") == "Summer"
        assert get_season("2024-07-15 08:30:00") == "Summer"
        assert get_season("2024-08-15 08:30:00") == "Summer"
    
    def test_get_season_fall(self):
        """Test season classification for fall months"""
        assert get_season("2024-09-15 08:30:00") == "Fall"
        assert get_season("2024-10-15 08:30:00") == "Fall"
        assert get_season("2024-11-15 08:30:00") == "Fall"
    
    def test_get_season_none_value(self):
        """Test season classification with None value"""
        assert get_season(None) is None
    
    def test_get_season_invalid_format(self):
        """Test season classification with invalid datetime format"""
        assert get_season("invalid") is None
    
    def test_generate_trip_id_normal(self):
        """Test trip ID generation for normal case"""
        pickup = "2024-01-15 08:30:00"
        dropoff = "2024-01-15 08:45:00"
        
        trip_id = generate_trip_id(pickup, dropoff)
        
        assert trip_id.startswith("trip_")
        assert len(trip_id) == 13  # "trip_" + 8 character hash
    
    def test_generate_trip_id_consistent(self):
        """Test that same input produces same trip ID"""
        pickup = "2024-01-15 08:30:00"
        dropoff = "2024-01-15 08:45:00"
        
        trip_id1 = generate_trip_id(pickup, dropoff)
        trip_id2 = generate_trip_id(pickup, dropoff)
        
        assert trip_id1 == trip_id2
    
    def test_generate_trip_id_different(self):
        """Test that different inputs produce different trip IDs"""
        pickup1 = "2024-01-15 08:30:00"
        dropoff1 = "2024-01-15 08:45:00"
        
        pickup2 = "2024-01-15 08:30:00"
        dropoff2 = "2024-01-15 08:50:00"
        
        trip_id1 = generate_trip_id(pickup1, dropoff1)
        trip_id2 = generate_trip_id(pickup2, dropoff2)
        
        assert trip_id1 != trip_id2
    
    def test_generate_trip_id_none_values(self):
        """Test trip ID generation with None values"""
        assert generate_trip_id(None, "2024-01-15 08:45:00") is None
        assert generate_trip_id("2024-01-15 08:30:00", None) is None

class TestDataValidation:
    
    def test_validate_coordinates_valid_nyc(self):
        """Test coordinate validation for valid NYC coordinates"""
        # Times Square area
        assert validate_coordinates(40.7580, -73.9855) == True
        # Central Park area
        assert validate_coordinates(40.7829, -73.9654) == True
        # Brooklyn Bridge area
        assert validate_coordinates(40.7061, -73.9969) == True
    
    def test_validate_coordinates_invalid_outside_nyc(self):
        """Test coordinate validation for coordinates outside NYC"""
        # San Francisco coordinates
        assert validate_coordinates(37.7749, -122.4194) == False
        # London coordinates
        assert validate_coordinates(51.5074, -0.1278) == False
    
    def test_validate_coordinates_invalid_values(self):
        """Test coordinate validation with invalid values"""
        assert validate_coordinates(None, -73.9855) == False
        assert validate_coordinates(40.7580, None) == False
        assert validate_coordinates("invalid", -73.9855) == False
    
    def test_validate_monetary_amount_valid(self):
        """Test monetary amount validation for valid amounts"""
        assert validate_monetary_amount(12.50) == True
        assert validate_monetary_amount(0.0) == True
        assert validate_monetary_amount(100.00) == True
        assert validate_monetary_amount("25.75") == True
    
    def test_validate_monetary_amount_invalid(self):
        """Test monetary amount validation for invalid amounts"""
        assert validate_monetary_amount(-5.00) == False  # Negative
        assert validate_monetary_amount(15000.00) == False  # Too high
        assert validate_monetary_amount("invalid") == False
        assert validate_monetary_amount(None) == False 