date_table_insert = ("""
insert into dim_date (
    date_key
    ,date
    ,year
    ,month
    ,day
    ,weekday
)
select distinct
    cast(to_char(date, 'YYYYMMDD') as integer) as date_key
    ,date
    ,extract(year from date) as year
    ,extract(month from date) as month
    ,extract(day from date) as day
    ,extract(weekday from date) as weekday
from (
    select Dt as "date" from staging_temperature
)
""")

location_table_insert = ("""
insert into dim_location (
    country_code
    ,country_name
    ,state_code
    ,state_name
)
select distinct
    'US' as country_code
    ,'United States' as country_name
    ,State_Code as state_code
    ,State as state_name
from staging_demographics 
""")

eia_date_table_insert = ("""
insert into dim_date (
    date_key
    ,date
    ,year
    ,month
    ,day
    ,weekday
)
select distinct
    cast(to_char(date, 'YYYYMMDD') as integer) as date_key
    ,date
    ,extract(year from date) as year
    ,extract(month from date) as month
    ,extract(day from date) as day
    ,extract(weekday from date) as weekday
from (
    select to_date(cast(year as varchar) || lpad(cast(month_number as varchar), 2, '0') || '01', 'YYYYMMDD', TRUE) as "date" from staging_eia
)
where date not in (select distinct date from dim_date)
""")

eia_plant_table_insert = ("""
insert into dim_plant (
    plant_id
    ,plant_name
    ,operator_id
    ,operator_name
)
select distinct
    plant_id as plant_id
    ,plant_name as plant_name
    ,operator_id as operator_id
    ,operator_name as operator_name
from staging_eia
where plant_id || operator_id not in (select distinct plant_id || operator_id from dim_plant)
""")

temperature_table_insert = ("""
insert into fact_temperature (
    date_key
    ,location_key
    ,average_temperature
    ,average_temperature_uncertainty
)
select distinct
    cast(to_char(t.Dt, 'YYYYMMDD') as int) as date_key
    ,dl.location_key
    ,t.AverageTemperature
    ,t.AverageTemperatureUncertainty
from staging_temperature t
left join dim_location dl
    on t.Country = dl.country_name
    and t.State = dl.state_name
""")

demographics_table_insert = ("""
insert into fact_demographics (
    location_key
    ,race
    ,median_age
    ,male_population
    ,female_population
    ,total_population
    ,number_of_veterans
    ,foreign_born
    ,average_household_size
    ,count
)
select distinct
    dl.location_key
    ,d.race
    ,avg(d.median_age) as median_age
    ,sum(d.male_population) as male_population
    ,sum(d.female_population) as female_population
    ,sum(d.total_population) as total_population
    ,sum(d.number_of_veterans) as number_of_veterans
    ,sum(d.foreign_born) as foreign_born
    ,avg(d.average_household_size) as average_household_size
    ,sum(count) as count
from staging_demographics d
left join dim_location dl
    on dl.country_code = 'US'
    and d.State_Code = dl.state_code
group by 1,2
""")

electricity_table_insert = ("""
insert into fact_electricity (
    date_key
    ,location_key
    ,plant_key
    ,aer_fuel_type_key
    ,balancing_authority_key
    ,census_region_key
    ,eia_sector_key
    ,nerc_region_key
    ,reported_fuel_type_key
    ,reported_prime_mover_key
    ,combined_heat_and_power_plant
    ,nuclear_unit_id
    ,naics_code
    ,physical_unit_label
    ,quantity
    ,elec_quantity
    ,mmbtu_per_unit
    ,tot_mmbtu
    ,elec_mmbtu
    ,netgen
    ,elec_fuel_consumption_mmbtu
    ,net_generation_megawatthours
    ,total_fuel_consumption_quantity
    ,electric_fuel_consumption_quantity
    ,total_fuel_consumption_mmbtu
)
select distinct
    cast(cast(e.year as varchar) || lpad(cast(e.month_number as varchar), 2, '0') || '01' as integer) as date_key
    ,dl.location_key
    ,p.plant_key
    ,aer.aer_fuel_type_key
    ,b.balancing_authority_key
    ,c.census_region_key
    ,eia.eia_sector_key
    ,n.nerc_region_key
    ,ft.reported_fuel_type_key
    ,pm.reported_prime_mover_key
    ,e.combined_heat_and_power_plant
    ,e.nuclear_unit_id
    ,cast(e.naics_code as integer) as naics_code
    ,e.physical_unit_label
    ,e.quantity
    ,e.elec_quantity
    ,e.mmbtu_per_unit
    ,e.tot_mmbtu
    ,e.elec_mmbtu
    ,e.netgen
    ,e.elec_fuel_consumption_mmbtu
    ,e.net_generation_megawatthours
    ,e.total_fuel_consumption_quantity
    ,e.electric_fuel_consumption_quantity
    ,e.total_fuel_consumption_mmbtu
from staging_eia e
left join dim_location dl
    on dl.country_code = 'US'
    and e.Plant_State = dl.state_code
left join dim_plant p
    on e.plant_id = p.plant_id
    and e.operator_id = p.operator_id
left join dim_aer_fuel_type aer
    on e.aer_fuel_type_code = aer.aer_fuel_type_code
left join dim_balancing_authority b
    on e.balancing_authority_code = b.balancing_authority_code
left join dim_census_region c
    on e.census_region = c.census_region_code   
left join dim_eia_sector eia
    on e.eia_sector_number = eia.eia_sector_code
left join dim_nerc_region n
    on e.nerc_region = n.nerc_region_code
left join dim_reported_fuel_type ft
    on e.reported_fuel_type_code = ft.reported_fuel_type_code
left join dim_reported_prime_mover pm
    on e.reported_prime_mover = pm.reported_prime_mover_code
""")


# QUERY LISTS

eia_insert_table_queries = [eia_date_table_insert, eia_plant_table_insert, electricity_table_insert]

insert_table_queries = [date_table_insert, location_table_insert, temperature_table_insert, demographics_table_insert]