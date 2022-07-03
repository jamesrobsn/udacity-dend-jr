# DROP TABLES

staging_eia_table_drop = "drop table if exists staging_eia"
staging_temperature_table_drop = "drop table if exists staging_temperature"
staging_demographics_table_drop = "drop table if exists staging_demographics"

date_table_drop = "drop table if exists dim_date"
location_table_drop = "drop table if exists dim_location"
plant_table_drop = "drop table if exists dim_plant"

temperature_table_drop = "drop table if exists fact_temperature"
demographics_table_drop = "drop table if exists fact_demographics"
electricity_table_drop = "drop table if exists fact_electricity"

dim_aer_fuel_type_table_drop = "drop table if exists dim_aer_fuel_type"
dim_balancing_authority_table_drop = "drop table if exists dim_balancing_authority"
dim_census_region_table_drop = "drop table if exists dim_census_region"
dim_eia_sector_table_drop = "drop table if exists dim_eia_sector"
dim_nerc_region_table_drop = "drop table if exists dim_nerc_region"
dim_reported_fuel_type_table_drop = "drop table if exists dim_reported_fuel_type"
dim_reported_prime_mover_table_drop = "drop table if exists dim_reported_prime_mover"


# QUERY LISTS

drop_table_queries = [staging_eia_table_drop, staging_temperature_table_drop, staging_demographics_table_drop, date_table_drop, location_table_drop, plant_table_drop, temperature_table_drop, demographics_table_drop, electricity_table_drop, dim_aer_fuel_type_table_drop, dim_balancing_authority_table_drop, dim_census_region_table_drop, dim_eia_sector_table_drop, dim_nerc_region_table_drop, dim_reported_fuel_type_table_drop, dim_reported_prime_mover_table_drop]

#REDUCED LIST TO AVOID LOADING STAGING TABLES
# drop_table_queries = [date_table_drop, location_table_drop, plant_table_drop, temperature_table_drop, demographics_table_drop, electricity_table_drop, dim_aer_fuel_type_table_drop, dim_balancing_authority_table_drop, dim_census_region_table_drop, dim_eia_sector_table_drop, dim_nerc_region_table_drop, dim_reported_fuel_type_table_drop, dim_reported_prime_mover_table_drop]