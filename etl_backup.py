import configparser
import psycopg2
import pandas as pd
import boto3
import datetime
import numpy as np
# from sql_queries import insert_table_queries, data_quality_check_queries
from sql.insert_queries import insert_table_queries
from sql.dq_checks import data_quality_check_queries

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
EIA_DATA = config.get('S3', 'EIA_DATA')
TEMPERATURE_DATA = config.get('S3', 'TEMPERATURE_DATA')
DEMOGRAPHICS_DATA = config.get('S3', 'DEMOGRAPHICS_DATA')
REFERENCE_DATA = config.get('S3', 'REFERENCE_DATA')
BUCKET = config.get('S3', 'BUCKET')
ACCESS_KEY = config.get('KEYS', 'AWS_ACCESS_KEY_ID')
SECRET_KEY = config.get('KEYS', 'AWS_SECRET_ACCESS_KEY')

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def copy_s3_to_redshift(cur, conn, dest_table, s3_folder):
    sql = """
    COPY {} FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    CSV
    IGNOREHEADER 1
    REGION 'us-east-2';
    """.format(dest_table, s3_folder, ARN)
    
    try:
        cur.execute(sql)
#         conn.commit()
        cur.execute("commit;")
        print('Data has been copied to redshift')
    except Exception as e:
        print('There was an error copying data from S3 to redshift')
        print(e)

def source_dest_check(cur, conn, total_rows, dest_table):
    sql = 'select count(*) from {}'.format(dest_table)

    try:
        cur.execute(sql)
        results = cur.fetchall()
    except Exception as e:
        print('Could not query staging table')
        print(e)
    
    if total_rows == results[0][0]:
        print('Source and destination rows match!')
    else:
        print('Source and destination rows are not matching. You should probably check that out...')
        print('Total Source Rows = {}'.format(total_rows))
        print('Total Destination Rows = {}'.format(results[0][0]))    

def column_configuration(column_config):
    '''
    These mappings are defined to facilitate the etl process. The EIA excel files change slightly over time
    which necessitated some way to match everything up accross the years.
    '''
    if column_config == 1:
        return {'Plant ID': 'Plant Id', 
                'Combined Heat & Power Plant': 'Combined Heat And\nPower Plant', 
                'Nuclear Unit I.D.': 'Nuclear Unit Id', 
                'Plant Name': 'Plant Name', 
                'Operator Name': 'Operator Name', 
                'Operator ID': 'Operator Id', 
                'State': 'Plant State', 
                'Census Region': 'Census Region', 
                'NERC Region': 'NERC Region', 
                'Reserved ': 'Reserved', 
                'NAICS Code': 'NAICS Code', 
                'EIA Sector Number': 'EIA Sector Number', 
                'Sector Name': 'Sector Name', 
                'Reported Prime Mover': 'Reported\nPrime Mover', 
                'Reported Fuel Type Code': 'Reported\nFuel Type Code', 
                'AER Fuel Type Code': 'AER\nFuel Type Code', 
                'Reserved .1': 'Balancing\nAuthority Code', 
                'Reserved .2': 'Respondent\nFrequency', 
                'Physical Unit Label': 'Physical\nUnit Label', 
                'QUANTITY_JAN': 'Quantity\nJanuary', 
                'QUANTITY_FEB': 'Quantity\nFebruary', 
                'QUANTITY_MAR': 'Quantity\nMarch', 
                'QUANTITY_APR': 'Quantity\nApril', 
                'QUANTITY_MAY': 'Quantity\nMay', 
                'QUANTITY_JUN': 'Quantity\nJune', 
                'QUANTITY_JUL': 'Quantity\nJuly', 
                'QUANTITY_AUG': 'Quantity\nAugust', 
                'QUANTITY_SEP': 'Quantity\nSeptember', 
                'QUANTITY_OCT': 'Quantity\nOctober', 
                'QUANTITY_NOV': 'Quantity\nNovember', 
                'QUANTITY_DEC': 'Quantity\nDecember', 
                'ELEC_QUANTITY_JAN': 'Elec_Quantity\nJanuary', 
                'ELEC_QUANTITY_FEB': 'Elec_Quantity\nFebruary', 
                'ELEC_QUANTITY_MAR': 'Elec_Quantity\nMarch', 
                'ELEC_QUANTITY_APR': 'Elec_Quantity\nApril', 
                'ELEC_QUANTITY_MAY': 'Elec_Quantity\nMay', 
                'ELEC_QUANTITY_JUN': 'Elec_Quantity\nJune', 
                'ELEC_QUANTITY_JUL': 'Elec_Quantity\nJuly', 
                'ELEC_QUANTITY_AUG': 'Elec_Quantity\nAugust', 
                'ELEC_QUANTITY_SEP': 'Elec_Quantity\nSeptember', 
                'ELEC_QUANTITY_OCT': 'Elec_Quantity\nOctober', 
                'ELEC_QUANTITY_NOV': 'Elec_Quantity\nNovember', 
                'ELEC_QUANTITY_DEC': 'Elec_Quantity\nDecember', 
                'MMBTU_PER_UNIT_JAN': 'MMBtuPer_Unit\nJanuary', 
                'MMBTU_PER_UNIT_FEB': 'MMBtuPer_Unit\nFebruary', 
                'MMBTU_PER_UNIT_MAR': 'MMBtuPer_Unit\nMarch', 
                'MMBTU_PER_UNIT_APR': 'MMBtuPer_Unit\nApril', 
                'MMBTU_PER_UNIT_MAY': 'MMBtuPer_Unit\nMay', 
                'MMBTU_PER_UNIT_JUN': 'MMBtuPer_Unit\nJune', 
                'MMBTU_PER_UNIT_JUL': 'MMBtuPer_Unit\nJuly', 
                'MMBTU_PER_UNIT_AUG': 'MMBtuPer_Unit\nAugust', 
                'MMBTU_PER_UNIT_SEP': 'MMBtuPer_Unit\nSeptember', 
                'MMBTU_PER_UNIT_OCT': 'MMBtuPer_Unit\nOctober', 
                'MMBTU_PER_UNIT_NOV': 'MMBtuPer_Unit\nNovember', 
                'MMBTU_PER_UNIT_DEC': 'MMBtuPer_Unit\nDecember', 
                'TOT_MMBTU_JAN': 'Tot_MMBtu\nJanuary', 
                'TOT_MMBTU_FEB': 'Tot_MMBtu\nFebruary', 
                'TOT_MMBTU_MAR': 'Tot_MMBtu\nMarch', 
                'TOT_MMBTU_APR': 'Tot_MMBtu\nApril', 
                'TOT_MMBTU_MAY': 'Tot_MMBtu\nMay', 
                'TOT_MMBTU_JUN': 'Tot_MMBtu\nJune', 
                'TOT_MMBTU_JUL': 'Tot_MMBtu\nJuly', 
                'TOT_MMBTU_AUG': 'Tot_MMBtu\nAugust', 
                'TOT_MMBTU_SEP': 'Tot_MMBtu\nSeptember', 
                'TOT_MMBTU_OCT': 'Tot_MMBtu\nOctober', 
                'TOT_MMBTU_NOV': 'Tot_MMBtu\nNovember', 
                'TOT_MMBTU_DEC': 'Tot_MMBtu\nDecember', 
                'ELEC_MMBTUS_JAN': 'Elec_MMBtu\nJanuary', 
                'ELEC_MMBTUS_FEB': 'Elec_MMBtu\nFebruary', 
                'ELEC_MMBTUS_MAR': 'Elec_MMBtu\nMarch', 
                'ELEC_MMBTUS_APR': 'Elec_MMBtu\nApril', 
                'ELEC_MMBTUS_MAY': 'Elec_MMBtu\nMay', 
                'ELEC_MMBTUS_JUN': 'Elec_MMBtu\nJune', 
                'ELEC_MMBTUS_JUL': 'Elec_MMBtu\nJuly', 
                'ELEC_MMBTUS_AUG': 'Elec_MMBtu\nAugust', 
                'ELEC_MMBTUS_SEP': 'Elec_MMBtu\nSeptember', 
                'ELEC_MMBTUS_OCT': 'Elec_MMBtu\nOctober', 
                'ELEC_MMBTUS_NOV': 'Elec_MMBtu\nNovember', 
                'ELEC_MMBTUS_DEC': 'Elec_MMBtu\nDecember', 
                'NETGEN_JAN': 'Netgen\nJanuary', 
                'NETGEN_FEB': 'Netgen\nFebruary', 
                'NETGEN_MAR': 'Netgen\nMarch', 
                'NETGEN_APR': 'Netgen\nApril', 
                'NETGEN_MAY': 'Netgen\nMay', 
                'NETGEN_JUN': 'Netgen\nJune', 
                'NETGEN_JUL': 'Netgen\nJuly', 
                'NETGEN_AUG': 'Netgen\nAugust', 
                'NETGEN_SEP': 'Netgen\nSeptember', 
                'NETGEN_OCT': 'Netgen\nOctober', 
                'NETGEN_NOV': 'Netgen\nNovember', 
                'NETGEN_DEC': 'Netgen\nDecember', 
                'TOTAL FUEL CONSUMPTION QUANTITY': 'Total Fuel Consumption\nQuantity', 
                'ELECTRIC FUEL CONSUMPTION QUANTITY': 'Electric Fuel Consumption\nQuantity', 
                'TOTAL FUEL CONSUMPTION MMBTUS': 'Total Fuel Consumption\nMMBtu', 
                'ELEC FUEL CONSUMPTION MMBTUS': 'Elec Fuel Consumption\nMMBtu', 
                'NET GENERATION (megawatthours)': 'Net Generation\n(Megawatthours)', 
                'Year': 'YEAR'
                }
    elif column_config == 2:
        return {'Plant Id': 'Plant Id', 
                'Combined Heat & Power Plant': 'Combined Heat And\nPower Plant', 
                'Nuclear Unit Id': 'Nuclear Unit Id', 
                'Plant Name': 'Plant Name', 
                'Operator Name': 'Operator Name', 
                'Operator Id': 'Operator Id', 
                'State': 'Plant State', 
                'Census Region': 'Census Region', 
                'NERC Region': 'NERC Region', 
                'Reserved': 'Reserved', 
                'NAICS Code': 'NAICS Code', 
                'EIA Sector Number': 'EIA Sector Number', 
                'Sector Name': 'Sector Name', 
                'Reported Prime Mover': 'Reported\nPrime Mover', 
                'Reported Fuel Type Code': 'Reported\nFuel Type Code', 
                'AER Fuel Type Code': 'AER\nFuel Type Code', 
                'Reserved.1': 'Balancing\nAuthority Code', 
                'Reserved.2': 'Respondent\nFrequency', 
                'Physical Unit Label': 'Physical\nUnit Label', 
                'Quantity_Jan': 'Quantity\nJanuary', 
                'Quantity_Feb': 'Quantity\nFebruary', 
                'Quantity_Mar': 'Quantity\nMarch', 
                'Quantity_Apr': 'Quantity\nApril', 
                'Quantity_May': 'Quantity\nMay', 
                'Quantity_Jun': 'Quantity\nJune', 
                'Quantity_Jul': 'Quantity\nJuly', 
                'Quantity_Aug': 'Quantity\nAugust', 
                'Quantity_Sep': 'Quantity\nSeptember', 
                'Quantity_Oct': 'Quantity\nOctober', 
                'Quantity_Nov': 'Quantity\nNovember', 
                'Quantity_Dec': 'Quantity\nDecember', 
                'Elec_Quantity_Jan': 'Elec_Quantity\nJanuary', 
                'Elec_Quantity_Feb': 'Elec_Quantity\nFebruary', 
                'Elec_Quantity_Mar': 'Elec_Quantity\nMarch', 
                'Elec_Quantity_Apr': 'Elec_Quantity\nApril', 
                'Elec_Quantity_May': 'Elec_Quantity\nMay', 
                'Elec_Quantity_Jun': 'Elec_Quantity\nJune', 
                'Elec_Quantity_Jul': 'Elec_Quantity\nJuly', 
                'Elec_Quantity_Aug': 'Elec_Quantity\nAugust', 
                'Elec_Quantity_Sep': 'Elec_Quantity\nSeptember', 
                'Elec_Quantity_Oct': 'Elec_Quantity\nOctober', 
                'Elec_Quantity_Nov': 'Elec_Quantity\nNovember', 
                'Elec_Quantity_Dec': 'Elec_Quantity\nDecember', 
                'MMBtuPer_Unit_Jan': 'MMBtuPer_Unit\nJanuary', 
                'MMBtuPer_Unit_Feb': 'MMBtuPer_Unit\nFebruary', 
                'MMBtuPer_Unit_Mar': 'MMBtuPer_Unit\nMarch', 
                'MMBtuPer_Unit_Apr': 'MMBtuPer_Unit\nApril', 
                'MMBtuPer_Unit_May': 'MMBtuPer_Unit\nMay', 
                'MMBtuPer_Unit_Jun': 'MMBtuPer_Unit\nJune', 
                'MMBtuPer_Unit_Jul': 'MMBtuPer_Unit\nJuly', 
                'MMBtuPer_Unit_Aug': 'MMBtuPer_Unit\nAugust', 
                'MMBtuPer_Unit_Sep': 'MMBtuPer_Unit\nSeptember', 
                'MMBtuPer_Unit_Oct': 'MMBtuPer_Unit\nOctober', 
                'MMBtuPer_Unit_Nov': 'MMBtuPer_Unit\nNovember', 
                'MMBtuPer_Unit_Dec': 'MMBtuPer_Unit\nDecember', 
                'Tot_MMBtuJan': 'Tot_MMBtu\nJanuary', 
                'Tot_MMBtuFeb': 'Tot_MMBtu\nFebruary', 
                'Tot_MMBtuMar': 'Tot_MMBtu\nMarch', 
                'Tot_MMBtuApr': 'Tot_MMBtu\nApril', 
                'Tot_MMBtuMay': 'Tot_MMBtu\nMay', 
                'Tot_MMBtuJun': 'Tot_MMBtu\nJune', 
                'Tot_MMBtuJul': 'Tot_MMBtu\nJuly', 
                'Tot_MMBtuAug': 'Tot_MMBtu\nAugust', 
                'Tot_MMBtuSep': 'Tot_MMBtu\nSeptember', 
                'Tot_MMBtuOct': 'Tot_MMBtu\nOctober', 
                'Tot_MMBtuNov': 'Tot_MMBtu\nNovember', 
                'Tot_MMBtuDec': 'Tot_MMBtu\nDecember', 
                'Elec_MMBtuJan': 'Elec_MMBtu\nJanuary', 
                'Elec_MMBtuFeb': 'Elec_MMBtu\nFebruary', 
                'Elec_MMBtuMar': 'Elec_MMBtu\nMarch', 
                'Elec_MMBtuApr': 'Elec_MMBtu\nApril', 
                'Elec_MMBtuMay': 'Elec_MMBtu\nMay', 
                'Elec_MMBtuJun': 'Elec_MMBtu\nJune', 
                'Elec_MMBtuJul': 'Elec_MMBtu\nJuly', 
                'Elec_MMBtuAug': 'Elec_MMBtu\nAugust', 
                'Elec_MMBtuSep': 'Elec_MMBtu\nSeptember', 
                'Elec_MMBtuOct': 'Elec_MMBtu\nOctober', 
                'Elec_MMBtuNov': 'Elec_MMBtu\nNovember', 
                'Elec_MMBtuDec': 'Elec_MMBtu\nDecember', 
                'Netgen_Jan': 'Netgen\nJanuary', 
                'Netgen_Feb': 'Netgen\nFebruary', 
                'Netgen_Mar': 'Netgen\nMarch', 
                'Netgen_Apr': 'Netgen\nApril', 
                'Netgen_May': 'Netgen\nMay', 
                'Netgen_Jun': 'Netgen\nJune', 
                'Netgen_Jul': 'Netgen\nJuly', 
                'Netgen_Aug': 'Netgen\nAugust', 
                'Netgen_Sep': 'Netgen\nSeptember', 
                'Netgen_Oct': 'Netgen\nOctober', 
                'Netgen_Nov': 'Netgen\nNovember', 
                'Netgen_Dec': 'Netgen\nDecember', 
                'Total Fuel Consumption Quantity': 'Total Fuel Consumption\nQuantity', 
                'Electric Fuel Consumption Quantity': 'Electric Fuel Consumption\nQuantity', 
                'Total Fuel Consumption MMBtu': 'Total Fuel Consumption\nMMBtu', 
                'Elec Fuel Consumption MMBtu': 'Elec Fuel Consumption\nMMBtu', 
                'Net Generation (Megawatthours)': 'Net Generation\n(Megawatthours)', 
                'YEAR': 'YEAR'
                }
    else:
        return {'Plant Id': 'Plant Id', 
                'Combined Heat And\nPower Plant': 'Combined Heat And\nPower Plant', 
                'Nuclear Unit Id': 'Nuclear Unit Id', 
                'Plant Name': 'Plant Name', 
                'Operator Name': 'Operator Name', 
                'Operator Id': 'Operator Id', 
                'Plant State': 'Plant State', 
                'Census Region': 'Census Region', 
                'NERC Region': 'NERC Region', 
                'Reserved': 'Reserved', 
                'NAICS Code': 'NAICS Code', 
                'EIA Sector Number': 'EIA Sector Number', 
                'Sector Name': 'Sector Name', 
                'Reported\nPrime Mover': 'Reported\nPrime Mover', 
                'Reported\nFuel Type Code': 'Reported\nFuel Type Code', 
                'AER\nFuel Type Code': 'AER\nFuel Type Code', 
                'Reserved.1': 'Balancing\nAuthority Code', 
                'Reserved.2': 'Respondent\nFrequency', 
                'Physical\nUnit Label': 'Physical\nUnit Label', 
                'Quantity\nJanuary': 'Quantity\nJanuary', 
                'Quantity\nFebruary': 'Quantity\nFebruary', 
                'Quantity\nMarch': 'Quantity\nMarch', 
                'Quantity\nApril': 'Quantity\nApril', 
                'Quantity\nMay': 'Quantity\nMay', 
                'Quantity\nJune': 'Quantity\nJune', 
                'Quantity\nJuly': 'Quantity\nJuly', 
                'Quantity\nAugust': 'Quantity\nAugust', 
                'Quantity\nSeptember': 'Quantity\nSeptember', 
                'Quantity\nOctober': 'Quantity\nOctober', 
                'Quantity\nNovember': 'Quantity\nNovember', 
                'Quantity\nDecember': 'Quantity\nDecember', 
                'Elec_Quantity\nJanuary': 'Elec_Quantity\nJanuary', 
                'Elec_Quantity\nFebruary': 'Elec_Quantity\nFebruary', 
                'Elec_Quantity\nMarch': 'Elec_Quantity\nMarch', 
                'Elec_Quantity\nApril': 'Elec_Quantity\nApril', 
                'Elec_Quantity\nMay': 'Elec_Quantity\nMay', 
                'Elec_Quantity\nJune': 'Elec_Quantity\nJune', 
                'Elec_Quantity\nJuly': 'Elec_Quantity\nJuly', 
                'Elec_Quantity\nAugust': 'Elec_Quantity\nAugust', 
                'Elec_Quantity\nSeptember': 'Elec_Quantity\nSeptember', 
                'Elec_Quantity\nOctober': 'Elec_Quantity\nOctober', 
                'Elec_Quantity\nNovember': 'Elec_Quantity\nNovember', 
                'Elec_Quantity\nDecember': 'Elec_Quantity\nDecember', 
                'MMBtuPer_Unit\nJanuary': 'MMBtuPer_Unit\nJanuary', 
                'MMBtuPer_Unit\nFebruary': 'MMBtuPer_Unit\nFebruary', 
                'MMBtuPer_Unit\nMarch': 'MMBtuPer_Unit\nMarch', 
                'MMBtuPer_Unit\nApril': 'MMBtuPer_Unit\nApril', 
                'MMBtuPer_Unit\nMay': 'MMBtuPer_Unit\nMay', 
                'MMBtuPer_Unit\nJune': 'MMBtuPer_Unit\nJune', 
                'MMBtuPer_Unit\nJuly': 'MMBtuPer_Unit\nJuly', 
                'MMBtuPer_Unit\nAugust': 'MMBtuPer_Unit\nAugust', 
                'MMBtuPer_Unit\nSeptember': 'MMBtuPer_Unit\nSeptember', 
                'MMBtuPer_Unit\nOctober': 'MMBtuPer_Unit\nOctober', 
                'MMBtuPer_Unit\nNovember': 'MMBtuPer_Unit\nNovember', 
                'MMBtuPer_Unit\nDecember': 'MMBtuPer_Unit\nDecember', 
                'Tot_MMBtu\nJanuary': 'Tot_MMBtu\nJanuary', 
                'Tot_MMBtu\nFebruary': 'Tot_MMBtu\nFebruary', 
                'Tot_MMBtu\nMarch': 'Tot_MMBtu\nMarch', 
                'Tot_MMBtu\nApril': 'Tot_MMBtu\nApril', 
                'Tot_MMBtu\nMay': 'Tot_MMBtu\nMay', 
                'Tot_MMBtu\nJune': 'Tot_MMBtu\nJune', 
                'Tot_MMBtu\nJuly': 'Tot_MMBtu\nJuly', 
                'Tot_MMBtu\nAugust': 'Tot_MMBtu\nAugust', 
                'Tot_MMBtu\nSeptember': 'Tot_MMBtu\nSeptember', 
                'Tot_MMBtu\nOctober': 'Tot_MMBtu\nOctober', 
                'Tot_MMBtu\nNovember': 'Tot_MMBtu\nNovember', 
                'Tot_MMBtu\nDecember': 'Tot_MMBtu\nDecember', 
                'Elec_MMBtu\nJanuary': 'Elec_MMBtu\nJanuary', 
                'Elec_MMBtu\nFebruary': 'Elec_MMBtu\nFebruary', 
                'Elec_MMBtu\nMarch': 'Elec_MMBtu\nMarch', 
                'Elec_MMBtu\nApril': 'Elec_MMBtu\nApril', 
                'Elec_MMBtu\nMay': 'Elec_MMBtu\nMay', 
                'Elec_MMBtu\nJune': 'Elec_MMBtu\nJune', 
                'Elec_MMBtu\nJuly': 'Elec_MMBtu\nJuly', 
                'Elec_MMBtu\nAugust': 'Elec_MMBtu\nAugust', 
                'Elec_MMBtu\nSeptember': 'Elec_MMBtu\nSeptember', 
                'Elec_MMBtu\nOctober': 'Elec_MMBtu\nOctober', 
                'Elec_MMBtu\nNovember': 'Elec_MMBtu\nNovember', 
                'Elec_MMBtu\nDecember': 'Elec_MMBtu\nDecember', 
                'Netgen\nJanuary': 'Netgen\nJanuary', 
                'Netgen\nFebruary': 'Netgen\nFebruary', 
                'Netgen\nMarch': 'Netgen\nMarch', 
                'Netgen\nApril': 'Netgen\nApril', 
                'Netgen\nMay': 'Netgen\nMay', 
                'Netgen\nJune': 'Netgen\nJune', 
                'Netgen\nJuly': 'Netgen\nJuly', 
                'Netgen\nAugust': 'Netgen\nAugust', 
                'Netgen\nSeptember': 'Netgen\nSeptember', 
                'Netgen\nOctober': 'Netgen\nOctober', 
                'Netgen\nNovember': 'Netgen\nNovember', 
                'Netgen\nDecember': 'Netgen\nDecember', 
                'Total Fuel Consumption\nQuantity': 'Total Fuel Consumption\nQuantity', 
                'Electric Fuel Consumption\nQuantity': 'Electric Fuel Consumption\nQuantity', 
                'Total Fuel Consumption\nMMBtu': 'Total Fuel Consumption\nMMBtu', 
                'Elec Fuel Consumption\nMMBtu': 'Elec Fuel Consumption\nMMBtu', 
                'Net Generation\n(Megawatthours)': 'Net Generation\n(Megawatthours)', 
                'YEAR': 'YEAR'
                }

def load_eia_staging(cur, conn, dest_table, s3_folder):
    '''
    This code loads each excel file in the repo, transforms it, exports it as a csv, loads it to an S3 bucket,
    then copies all csvs in S3 to redshift at the end, then checks that source and destination rows match.
    '''
    
    total_rows = 0
    files = ['eia923December2008.xls','EIA923 SCHEDULES 2_3_4_5 M Final 2009 REVISED 05252011.XLS','EIA923 SCHEDULES 2_3_4_5 Final 2010.xls','EIA923_Schedules_2_3_4_5_2011_Final_Revision.xlsx','EIA923_Schedules_2_3_4_5_M_12_2012_Final_Revision.xlsx','EIA923_Schedules_2_3_4_5_2013_Final_Revision.xlsx','EIA923_Schedules_2_3_4_5_M_12_2014_Final_Revision.xlsx','EIA923_Schedules_2_3_4_5_M_12_2015_Final_Revision.xlsx','EIA923_Schedules_2_3_4_5_M_12_2016_Final_Revision.xlsx','EIA923_Schedules_2_3_4_5_M_12_2017_Final_Revision.xlsx','EIA923_Schedules_2_3_4_5_M_12_2018_Final_Revision.xlsx','EIA923_Schedules_2_3_4_5_M_12_2019_Final_Revision.xlsx','EIA923_Schedules_2_3_4_5_M_12_2020_Final_Revision.xlsx','EIA923_Schedules_2_3_4_5_M_12_2021_18FEB2022.xlsx']
    headers = [7,7,7,5,5,5,5,5,5,5,5,5,5,5]
    files_column_config = [1,1,1,2,2,2,3,3,3,3,3,3,0,0]

    for i, file in enumerate(files):
        filepath = 'eia_data/' + file
        
        df = pd.read_excel(filepath, sheet_name=0, header=headers[i])
        
        df = df.replace(np.nan,0)
        
        if files_column_config[i] != 0:  
            column_config = column_configuration(files_column_config[i])
            df = df.rename(columns=column_config)
        
        df = df[['Plant Id', 'Combined Heat And\nPower Plant', 'Nuclear Unit Id', 'Plant Name', 'Operator Name', 'Operator Id', 'Plant State', 'Census Region', 'NERC Region', 'NAICS Code', 'EIA Sector Number', 'Sector Name', 'Reported\nPrime Mover', 'Reported\nFuel Type Code', 'AER\nFuel Type Code', 'Balancing\nAuthority Code', 'Physical\nUnit Label', 'Quantity\nJanuary', 'Quantity\nFebruary', 'Quantity\nMarch', 'Quantity\nApril', 'Quantity\nMay', 'Quantity\nJune', 'Quantity\nJuly', 'Quantity\nAugust', 'Quantity\nSeptember', 'Quantity\nOctober', 'Quantity\nNovember', 'Quantity\nDecember', 'Elec_Quantity\nJanuary', 'Elec_Quantity\nFebruary', 'Elec_Quantity\nMarch', 'Elec_Quantity\nApril', 'Elec_Quantity\nMay', 'Elec_Quantity\nJune', 'Elec_Quantity\nJuly', 'Elec_Quantity\nAugust', 'Elec_Quantity\nSeptember', 'Elec_Quantity\nOctober', 'Elec_Quantity\nNovember', 'Elec_Quantity\nDecember', 'MMBtuPer_Unit\nJanuary', 'MMBtuPer_Unit\nFebruary', 'MMBtuPer_Unit\nMarch', 'MMBtuPer_Unit\nApril', 'MMBtuPer_Unit\nMay', 'MMBtuPer_Unit\nJune', 'MMBtuPer_Unit\nJuly', 'MMBtuPer_Unit\nAugust', 'MMBtuPer_Unit\nSeptember', 'MMBtuPer_Unit\nOctober', 'MMBtuPer_Unit\nNovember', 'MMBtuPer_Unit\nDecember', 'Tot_MMBtu\nJanuary', 'Tot_MMBtu\nFebruary', 'Tot_MMBtu\nMarch', 'Tot_MMBtu\nApril', 'Tot_MMBtu\nMay', 'Tot_MMBtu\nJune', 'Tot_MMBtu\nJuly', 'Tot_MMBtu\nAugust', 'Tot_MMBtu\nSeptember', 'Tot_MMBtu\nOctober', 'Tot_MMBtu\nNovember', 'Tot_MMBtu\nDecember', 'Elec_MMBtu\nJanuary', 'Elec_MMBtu\nFebruary', 'Elec_MMBtu\nMarch', 'Elec_MMBtu\nApril', 'Elec_MMBtu\nMay', 'Elec_MMBtu\nJune', 'Elec_MMBtu\nJuly', 'Elec_MMBtu\nAugust', 'Elec_MMBtu\nSeptember', 'Elec_MMBtu\nOctober', 'Elec_MMBtu\nNovember', 'Elec_MMBtu\nDecember', 'Netgen\nJanuary', 'Netgen\nFebruary', 'Netgen\nMarch', 'Netgen\nApril', 'Netgen\nMay', 'Netgen\nJune', 'Netgen\nJuly', 'Netgen\nAugust', 'Netgen\nSeptember', 'Netgen\nOctober', 'Netgen\nNovember', 'Netgen\nDecember', 'Total Fuel Consumption\nQuantity', 'Electric Fuel Consumption\nQuantity', 'Total Fuel Consumption\nMMBtu', 'Elec Fuel Consumption\nMMBtu', 'Net Generation\n(Megawatthours)', 'YEAR']]
        
        df = df.astype(str) #converting columns objects to avoid issues in wide_to_long
        
        df.columns = df.columns.str.replace("\n", " ")
        df.columns = df.columns.str.replace(" ", "_")
        df.columns = df.columns.str.replace("(", "")
        df.columns = df.columns.str.replace(")", "")
        df = df.replace('.', None)

        df = pd.wide_to_long(df, 
                            stubnames=['Quantity','Elec_Quantity','MMBtuPer_Unit','Tot_MMBtu','Elec_MMBtu','Netgen'], 
                            i=['Plant_Id', 'Combined_Heat_And_Power_Plant', 'Nuclear_Unit_Id', 'Plant_Name', 'Operator_Name', 'Operator_Id', 'Plant_State', 'Census_Region', 'NERC_Region', 'NAICS_Code', 'EIA_Sector_Number', 'Sector_Name', 'Reported_Prime_Mover', 'Reported_Fuel_Type_Code', 'AER_Fuel_Type_Code', 'Balancing_Authority_Code', 'Physical_Unit_Label','YEAR'],
                            j='Month', sep='_', suffix=r'\D+').reset_index()
        
        print(df.shape[0])
        total_rows = total_rows + (df.shape[0])
        
        df['Month_Number'] = df['Month'].apply(lambda x: datetime.datetime.strptime(x, "%B").month)
        
        if file.find('xlsx') > 0:
            csv_file = file.lower().replace('.xlsx','.csv')
        else:
            csv_file = file.lower().replace('.xls','.csv')        
        
        df.to_csv('eia_data_clean/' + csv_file, index=False, na_rep='0')

        uploaded = upload_to_aws('eia_data_clean/' + csv_file, BUCKET, 'eia/' + csv_file)
        print(csv_file + ' has been copied to S3')
        
        df = None
    
    copy_s3_to_redshift(cur, conn, dest_table, s3_folder)
    source_dest_check(cur, conn, total_rows, dest_table)

def load_temperature_staging(cur, conn, dest_table, s3_folder):
    '''
    This code reads the temperature csv for total rows, copies it to S3, 
    loads it to redshift then checks that source and destination rows match.
    '''
    
    total_rows = 0
    file = 'GlobalLandTemperaturesByState.csv'

    filepath = 'temperature_data/' + file

    df = pd.read_csv(filepath)
    total_rows = total_rows + (df.shape[0])

    uploaded = upload_to_aws('temperature_data/' + file, BUCKET, 'temperature/' + file)
    print(file + ' has been copied to S3')

    df = None

    copy_s3_to_redshift(cur, conn, dest_table, s3_folder)
    source_dest_check(cur, conn, total_rows, dest_table)

def load_demographics_staging(cur, conn, dest_table, s3_folder):
    '''
    This code loads the demographics csv file in the repo, transforms it, exports it as a csv, loads it to an S3 bucket,
    then copies it from S3 to redshift at the end, then checks that source and destination rows match.
    '''
    
    total_rows = 0
    file = 'us-cities-demographics.csv'

    filepath = 'demographics_data/' + file

    df = pd.read_csv(filepath, delimiter=';')
    total_rows = total_rows + (df.shape[0])

    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.replace("-", "_")

    df.to_csv('demographics_data_clean/' + file, index=False)

    uploaded = upload_to_aws('demographics_data_clean/' + file, BUCKET, 'demographics/' + file)
    print(file + ' has been copied to S3')

    df = None

    copy_s3_to_redshift(cur, conn, dest_table, s3_folder)
    source_dest_check(cur, conn, total_rows, dest_table)

def load_reference_data(cur, conn, s3_folder):
    '''
    This code loads each csv file in the repo to check source rows, loads it to an S3 bucket,
    copies each csv in S3 to redshift, then checks that source and destination rows match.
    '''
    
    total_rows = 0
    files = ['aer_fuel_type_ref.csv', 'balancing_authority_ref.csv', 'census_region_ref.csv', 'eia_sector_ref.csv', 'nerc_region_ref.csv', 'reported_fuel_type_ref.csv', 'reported_prime_mover_ref.csv']
    
    for file in files:
        total_rows = 0
        
        filepath = 'reference_data/' + file

        df = pd.read_csv(filepath)
        total_rows = total_rows + (df.shape[0])
        
        uploaded = upload_to_aws('reference_data/' + file, BUCKET, 'reference/' + file)
        print(file + ' has been copied to S3')
        
        dest_table = file.replace('.csv', '')
        copy_s3_to_redshift(cur, conn, dest_table, s3_folder + '/' + file)
        source_dest_check(cur, conn, total_rows, dest_table)
        

def insert_tables(cur, conn):
    '''
    Inserts data into staging tables specified in the insert_table_queries list. 
    '''
    
    for query in insert_table_queries:
        cur.execute(query)
        cur.execute("commit;")
#         conn.commit()
        print('Insert Complete')
    
def data_quality_checks(cur, conn):
    '''
    Checks that all data have a unique key 
    '''
    expected_result = 0 
    
    for query in data_quality_check_queries:
        try:
            cur.execute(query)
            results = cur.fetchall()
        except Exception as e:
            print('Could not query table for data quality checks')
            print(e)

        if expected_result == results[0][0]:
            print('Test Passed!')
        else:
            print('Table did not pass data quality check. You should probably check that out...')
            print('Expected Result = {}'.format(expected_result))
            print('Actual Result = {}'.format(results[0][0]))  

def main():
    '''
    Connects to an AWS redshift cluster using connection details specified in dwh.cfg. 
    Then loads data to staging tables and inserts data from staging tables into the modeled tables.
    '''
        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_eia_staging(cur, conn, 'staging_eia', EIA_DATA)
    load_temperature_staging(cur, conn, 'staging_temperature', TEMPERATURE_DATA)
    load_demographics_staging(cur, conn, 'staging_demographics', DEMOGRAPHICS_DATA)
    print('ALL STAGING TABLES HAVE BEEN LOADED')
    
    load_reference_data(cur, conn, REFERENCE_DATA)
        
    insert_tables(cur, conn)
    
    data_quality_checks(cur, conn)

    conn.close()
    
    print('PIPELINE RUN COMPLETE')


if __name__ == "__main__":
    main()