CREATE TABLE IF NOT EXISTS Weather (
    zipcode INTEGER,
    ride_date DATE,
    hour INTEGER,
    temperature DOUBLE PRECISION,
    weather_description TEXT
);

TRUNCATE TABLE Weather;

SELECT
        v.vehicleid AS vehicle_id,
        bt.biketypedescription AS type
    FROM vehicles v
    JOIN bikelots bl ON v.bikelotid = bl.bikelotid
    JOIN bike_types bt ON bl.biketypeid = bt.biketypeid