from pyspark.sql.functions import *
from pyspark.sql.types import *

road_schema = StructType([
    StructField("count_point_id", StringType()),   # cast to Int explicitly downstream
    StructField("year", StringType()),
    StructField("region_id", StringType()),
    StructField("region_name", StringType()),
    StructField("region_ons_code", StringType()),
    StructField("local_authority_id", StringType()),
    StructField("local_authority_name", StringType()),
    StructField("local_authority_code", StringType()),
    StructField("road_name", StringType()),
    StructField("road_category", StringType()),
    StructField("road_type", StringType()),
    StructField("start_junction_road_name", StringType()),
    StructField("end_junction_road_name", StringType()),
    StructField("easting", StringType()),
    StructField("northing", StringType()),
    StructField("latitude", StringType()),
    StructField("longitude", StringType()),
    StructField("link_length_km", StringType()),
    StructField("link_length_miles", StringType())
])

traffic_schema = StructType([
    StructField("count_point_id", StringType()),
    StructField("year", StringType()),
    StructField("region_id", StringType()),
    StructField("region_name", StringType()),
    StructField("region_ons_code", StringType()),
    StructField("local_authority_id", StringType()),
    StructField("local_authority_name", StringType()),
    StructField("local_authority_code", StringType()),
    StructField("road_name", StringType()),
    StructField("road_category", StringType()),
    StructField("road_type", StringType()),
    StructField("start_junction_road_name", StringType()),
    StructField("end_junction_road_name", StringType()),
    StructField("easting", StringType()),
    StructField("northing", StringType()),
    StructField("latitude", StringType()),
    StructField("longitude", StringType()),
    StructField("estimation_method", StringType()),
    StructField("estimation_method_detailed", StringType()),
    StructField("direction_of_travel", StringType()),
    StructField("pedal_cycles", StringType()),
    StructField("two_wheeled_motor_vehicles", StringType()),
    StructField("cars_and_taxis", StringType()),
    StructField("buses_and_coaches", StringType()),
    StructField("LGVs", StringType()),
    StructField("HGVs_2_rigid_axle", StringType()),
    StructField("HGVs_3_rigid_axle", StringType()),
    StructField("HGVs_4_or_more_rigid_axle", StringType()),
    StructField("HGVs_3_or_4_articulated_axle", StringType()),
    StructField("HGVs_5_articulated_axle", StringType()),
    StructField("HGVs_6_articulated_axle", StringType()),
    StructField("all_HGVs", StringType()),
    StructField("all_motor_vehicles", StringType()),
    StructField("source", StringType()),
    StructField("link_length_km", StringType()),
    StructField("link_length_miles", StringType()),
])
import os

print(os.getcwd())