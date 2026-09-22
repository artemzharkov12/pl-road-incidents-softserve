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
    "start_longitude_is_poland" : "start_longitude BETWEEN 14.1 AND 24.2",
    "start_latitude_is_poland" : "start_latitude >= 49.0 AND start_latitude <= 54.9",
    "icon_category_bigger_than_0": "icon_category >= 0"
}