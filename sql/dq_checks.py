# DATA QUALTIY CHECKS

date_table_check = ("""
select count(*) from public.dim_date where date_key is null
""")

location_table_check = ("""
select count(*) from public.dim_location where location_key is null
""")

plant_table_check = ("""
select count(*) from public.dim_plant where plant_key is null
""")

operator_table_check = ("""
select count(*) from public.dim_operator where operator_key is null
""")

dim_aer_fuel_type_table_check = ("""
select count(*) from public.dim_aer_fuel_type where aer_fuel_type_key is null
""")

dim_balancing_authority_table_check = ("""
select count(*) from public.dim_balancing_authority where balancing_authority_key is null
""")

dim_census_region_table_check = ("""
select count(*) from public.dim_census_region where census_region_key is null
""")

dim_eia_sector_table_check = ("""
select count(*) from public.dim_eia_sector where eia_sector_key is null
""")

dim_nerc_region_table_check = ("""
select count(*) from public.dim_nerc_region where nerc_region_key is null
""")

dim_reported_fuel_type_table_check = ("""
select count(*) from public.dim_reported_fuel_type where reported_fuel_type_key is null
""")

dim_reported_prime_mover_table_check = ("""
select count(*) from public.dim_reported_prime_mover where reported_prime_mover_key is null
""")


# QUERY LISTS

data_quality_check_queries = [date_table_check, location_table_check, operator_table_check, plant_table_check, dim_aer_fuel_type_table_check, dim_balancing_authority_table_check, dim_census_region_table_check, dim_eia_sector_table_check, dim_nerc_region_table_check, dim_reported_fuel_type_table_check, dim_reported_prime_mover_table_check]