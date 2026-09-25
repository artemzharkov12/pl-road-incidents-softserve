basic_expectations = {
    "accident_id_not_null": "accident_id IS NOT NULL",
    "icon_category_not_null": "icon_category IS NOT NULL",
    "impact_jams_not_null": "impact_jams IS NOT NULL",
    "start_time_not_null": "start_time IS NOT NULL",
    "description_not_null": "description IS NOT NULL",
    "start_longitude_not_null" : "start_longitude IS NOT NULL",
    "start_latitude_not_null" : "start_latitude IS NOT NULL"
}

business_expectations = {
    "coordinates_inside_poland": "st_contains(st_geomfromtext('POLYGON ((14.2 53.9, 15.0 54.0, 17.5 54.8, 19.0 54.3, 19.6 54.4, 22.8 54.3, 23.9 54.0, 23.6 52.7, 24.1 51.5, 24.1 50.5, 22.8 49.0, 20.0 49.2, 18.8 49.5, 17.0 50.1, 16.2 50.6, 14.8 51.0, 14.7 51.9, 14.1 52.8, 14.2 53.9))'), st_point(start_longitude, start_latitude))",
    "icon_category_bigger_than_0": "icon_category >= 0"
}