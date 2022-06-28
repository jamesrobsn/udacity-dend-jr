import configparser

'''
This file contains all queries to drop and create tables, and to transform and load data first into staging tables and finallly into modeled tables.
'''

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_eia_table_drop = "drop table if exists staging_eia"
staging_temperature_table_drop = "drop table if exists staging_temperature"
staging_demographics_table_drop = "drop table if exists staging_demographics"

date_table_drop = "drop table if exists dim_date"
location_table_drop = "drop table if exists dim_location"
operator_table_drop = "drop table if exists dim_operator"
plant_table_drop = "drop table if exists dim_plant"

temperature_table_drop = "drop table if exists fact_temperature"
demographics_table_drop = "drop table if exists fact_demographics"
electricity_table_drop = "drop table if exists fact_electricity"

aer_fuel_type_ref_table_drop = "drop table if exists aer_fuel_type_ref"
balancing_authority_ref_table_drop = "drop table if exists balancing_authority_ref"
census_region_ref_table_drop = "drop table if exists census_region_ref"
eia_sector_ref_table_drop = "drop table if exists eia_sector_ref"
nerc_region_ref_table_drop = "drop table if exists nerc_region_ref"
reported_fuel_type_ref_table_drop = "drop table if exists reported_fuel_type_ref"
reported_prime_mover_ref_table_drop = "drop table if exists reported_prime_mover_ref"

'aer_fuel_type_ref.csv', 'balancing_authority_ref.csv', 'census_region_ref.csv', 'eia_sector_ref.csv', 'nerc_region_ref.csv', 'reported_fuel_type_ref.csv', 'reported_prime_mover_ref.csv'

# CREATE TABLES

staging_eia_table_create= ("""
create table if not exists staging_eia
(Plant_Id integer
,Combined_Heat_And_Power_Plant varchar
,Nuclear_Unit_Id varchar
,Plant_Name varchar
,Operator_Name varchar
,Operator_Id decimal
,Plant_State varchar
,Census_Region varchar
,NERC_Region varchar
,NAICS_Code integer
,EIA_Sector_Number integer
,Sector_Name varchar
,Reported_Prime_Mover varchar
,Reported_Fuel_Type_Code varchar
,AER_Fuel_Type_Code varchar
,Balancing_Authority_Code varchar
,Physical_Unit_Label varchar
,YEAR integer
,Month varchar
,Elec_Fuel_Consumption_MMBtu decimal
,Net_Generation_Megawatthours decimal
,Total_Fuel_Consumption_Quantity decimal
,Electric_Fuel_Consumption_Quantity decimal
,Total_Fuel_Consumption_MMBtu decimal
,Quantity decimal
,Elec_Quantity decimal
,MMBtuPer_Unit decimal
,Tot_MMBtu decimal
,Elec_MMBtu decimal
,Netgen decimal
,Month_number integer
)
""")

staging_temperature_table_create= ("""
create table if not exists staging_temperature
(dt date
,AverageTemperature decimal
,AverageTemperatureUncertainty decimal
,State varchar
,Country varchar
)
""")

staging_demographics_table_create= ("""
create table if not exists staging_demographics
(City varchar
,State varchar
,Median_Age decimal
,Male_Population decimal
,Female_Population decimal
,Total_Population decimal
,Number_of_Veterans decimal
,Foreign_born decimal
,Average_Household_Size decimal
,State_Code varchar
,Race varchar
,Count decimal
)
""")

date_table_create = ("""
create table if not exists dim_date
(date_key integer primary key
,date date
,year integer
,month integer
,day integer
,weekday integer
)
""")

location_table_create = ("""
create table if not exists dim_location
(location_key integer identity(1,1) primary key
,country_code varchar
,country_name varchar
,state_code varchar
,state_name varchar
)
""")

operator_table_create = ("""
create table if not exists dim_operator
(operator_key integer identity(1,1) primary key
,operator_id varchar
,operator_name varchar
)
""")

plant_table_create = ("""
create table if not exists dim_plant
(plant_key integer identity(1,1) primary key
,plant_id varchar
,plant_name varchar
)
""")

temperature_table_create = ("""
create table if not exists fact_temperature
(date_key integer primary key
,location_key integer
,average_temperature decimal
,average_temperature_uncertainty decimal
)
""")

demographics_table_create = ("""
create table if not exists fact_demographics
(location_key integer primary key
,race varchar
,median_age decimal
,male_population integer
,female_population integer
,total_population integer
,number_of_veterans integer
,foreign_born integer
,average_household_size decimal
,count integer
)
""")

electricity_table_create = ("""
create table if not exists fact_electricity
(date_key integer primary key
,location_key integer
,plant_key integer
,operator_key integer
,aer_fuel_type_key integer
,balancing_authority_key integer
,census_region_key integer
,eia_sector_key integer
,nerc_region_key integer
,reported_fuel_type_key integer
,reported_prime_mover_key integer
,combined_heat_and_power_plant varchar
,nuclear_unit_id varchar
,naics_code integer
,sector_name varchar
,physical_unit_label varchar
,quantity decimal
,elec_quantity decimal
,mmbtuper_unit decimal
,tot_mmbtu decimal
,elec_mmbtu decimal
,netgen decimal
,elec_fuel_consumption_mmbtu decimal
,net_generation_megawatthours decimal
,total_fuel_consumption_quantity decimal
,electric_fuel_consumption_quantity decimal
,total_fuel_consumption_mmbtu decimal
)
""")



aer_fuel_type_ref_table_create = ("""
create table if not exists aer_fuel_type_ref
(aer_fuel_type_key integer identity(1,1) primary key
,aer_fuel_type_code varchar
,aer_fuel_type_desc varchar
)
""")

balancing_authority_ref_table_create = ("""
create table if not exists balancing_authority_ref
(balancing_authority_key integer identity(1,1) primary key
,balancing_authority_code varchar
,balancing_authority_desc varchar
)
""")

census_region_ref_table_create = ("""
create table if not exists census_region_ref
(census_region_key integer identity(1,1) primary key
,census_region_code varchar
,census_region_desc varchar
)
""")

eia_sector_ref_table_create = ("""
create table if not exists eia_sector_ref
(eia_sector_key integer identity(1,1) primary key
,eia_sector_code varchar
,eia_sector_desc varchar
)
""")

nerc_region_ref_table_create = ("""
create table if not exists nerc_region_ref
(nerc_region_key integer identity(1,1) primary key
,nerc_region_code varchar
,nerc_region_desc varchar
)
""")

reported_fuel_type_ref_table_create = ("""
create table if not exists reported_fuel_type_ref
(reported_fuel_type_key integer identity(1,1) primary key
,reported_fuel_type_code varchar
,reported_fuel_type_desc varchar
)
""")

reported_prime_mover_ref_table_create = ("""
create table if not exists reported_prime_mover_ref
(reported_prime_mover_key integer identity(1,1) primary key
,reported_prime_mover_code varchar
,reported_prime_mover_desc varchar
)
""")

# FINAL TABLES

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
    union all
    select to_date(cast(YEAR as varchar) || lpad(cast(Month_Number as varchar), 2, '0') || '01', 'YYYYMMDD', TRUE) as "date" from staging_eia
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

operator_table_insert = ("""
insert into dim_operator (
    operator_id
    ,operator_name
)
select distinct
    Operator_Id as operator_id
    ,Operator_Name as operator_name
from staging_eia
""")

plant_table_insert = ("""
insert into dim_plant (
    plant_id
    ,plant_name
)
select distinct
    Plant_Id as plant_id
    ,Plant_Name as plant_name
from staging_eia
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
    ,operator_key
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
    ,sector_name
    ,physical_unit_label
    ,quantity
    ,elec_quantity
    ,mmbtuper_unit
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
    cast(cast(e.YEAR as varchar) || lpad(cast(e.Month_Number as varchar), 2, '0') || '01' as integer) as date_key
    ,dl.location_key
    ,p.plant_key
    ,o.operator_key
    ,aer.aer_fuel_type_key
    ,b.balancing_authority_key
    ,c.census_region_key
    ,eia.eia_sector_key
    ,n.nerc_region_key
    ,ft.reported_fuel_type_key
    ,pm.reported_prime_mover_key
    ,e.combined_heat_and_power_plant
    ,e.nuclear_unit_id
    ,e.naics_code
    ,e.sector_name
    ,e.physical_unit_label
    ,e.quantity
    ,e.elec_quantity
    ,e.mmbtuper_unit
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
left join dim_operator o
    on e.Operator_id = o.operator_id
left join dim_plant p
    on e.Plant_Id = p.plant_id
left join aer_fuel_type_ref aer
    on e.aer_fuel_type_code = aer.aer_fuel_type_code
left join balancing_authority_ref b
    on e.balancing_authority_code = b.balancing_authority_code
left join census_region_ref c
    on e.census_region = c.census_region_code   
left join eia_sector_ref eia
    on e.eia_sector_number = eia.eia_sector_code
left join nerc_region_ref n
    on e.nerc_region = n.nerc_region_code
left join reported_fuel_type_ref ft
    on e.reported_fuel_type_code = ft.reported_fuel_type_code
left join reported_prime_mover_ref pm
    on e.reported_prime_mover = pm.reported_prime_mover_code
""")


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

aer_fuel_type_ref_table_check = ("""
select count(*) from public.aer_fuel_type_ref where aer_fuel_type_key is null
""")

balancing_authority_ref_table_check = ("""
select count(*) from public.balancing_authority_ref where balancing_authority_key is null
""")

census_region_ref_table_check = ("""
select count(*) from public.census_region_ref where census_region_key is null
""")

eia_sector_ref_table_check = ("""
select count(*) from public.eia_sector_ref where eia_sector_key is null
""")

nerc_region_ref_table_check = ("""
select count(*) from public.nerc_region_ref where nerc_region_key is null
""")

reported_fuel_type_ref_table_check = ("""
select count(*) from public.reported_fuel_type_ref where reported_fuel_type_key is null
""")

reported_prime_mover_ref_table_check = ("""
select count(*) from public.reported_prime_mover_ref where reported_prime_mover_key is null
""")


# QUERY LISTS

create_table_queries = [staging_eia_table_create, staging_temperature_table_create, staging_demographics_table_create, date_table_create, location_table_create, operator_table_create, plant_table_create, temperature_table_create, demographics_table_create, electricity_table_create, aer_fuel_type_ref_table_create, balancing_authority_ref_table_create, census_region_ref_table_create, eia_sector_ref_table_create, nerc_region_ref_table_create, reported_fuel_type_ref_table_create, reported_prime_mover_ref_table_create]

#REDUCED LIST TO AVOID LOADING STAGING TABLES
# create_table_queries = [date_table_create, location_table_create, operator_table_create, plant_table_create, temperature_table_create, demographics_table_create, electricity_table_create, aer_fuel_type_ref_table_create, balancing_authority_ref_table_create, census_region_ref_table_create, eia_sector_ref_table_create, nerc_region_ref_table_create, reported_fuel_type_ref_table_create, reported_prime_mover_ref_table_create]

drop_table_queries = [staging_eia_table_drop, staging_temperature_table_drop, staging_demographics_table_drop, date_table_drop, location_table_drop, operator_table_drop, plant_table_drop, temperature_table_drop, demographics_table_drop, electricity_table_drop, aer_fuel_type_ref_table_drop, balancing_authority_ref_table_drop, census_region_ref_table_drop, eia_sector_ref_table_drop, nerc_region_ref_table_drop, reported_fuel_type_ref_table_drop, reported_prime_mover_ref_table_drop]

#REDUCED LIST TO AVOID LOADING STAGING TABLES
# drop_table_queries = [date_table_drop, location_table_drop, operator_table_drop, plant_table_drop, temperature_table_drop, demographics_table_drop, electricity_table_drop, aer_fuel_type_ref_table_drop, balancing_authority_ref_table_drop, census_region_ref_table_drop, eia_sector_ref_table_drop, nerc_region_ref_table_drop, reported_fuel_type_ref_table_drop, reported_prime_mover_ref_table_drop]

insert_table_queries = [date_table_insert, location_table_insert, operator_table_insert, plant_table_insert, temperature_table_insert, demographics_table_insert, electricity_table_insert]

data_quality_check_queries = [date_table_check, location_table_check, operator_table_check, plant_table_check, aer_fuel_type_ref_table_check, balancing_authority_ref_table_check, census_region_ref_table_check, eia_sector_ref_table_check, nerc_region_ref_table_check, reported_fuel_type_ref_table_check, reported_prime_mover_ref_table_check]