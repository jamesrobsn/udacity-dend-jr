import configparser
import psycopg2
import pandas as pd
import boto3
import datetime
import numpy as np
import io
# from sql_queries import insert_table_queries, data_quality_check_queries
from sql.insert_queries import insert_table_queries, eia_insert_table_queries
from sql.data_cleansing import eia_data_cleansing_steps
from sql.dq_checks import data_quality_check_queries
from configs.eia_column_names import eia_raw_columns_names, eia_raw_text_columns, eia_raw_numeric_columns, eia_raw_dim_columns, eia_raw_fact_columns
from configs.ingestions_files import eia_files, eia_headers

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
BUCKET = config.get('S3', 'BUCKET')
ACCESS_KEY = config.get('KEYS', 'AWS_ACCESS_KEY_ID')
SECRET_KEY = config.get('KEYS', 'AWS_SECRET_ACCESS_KEY')

RAW_EIA_DATA = config.get('S3', 'RAW_EIA_DATA')
EIA_DATA = config.get('S3', 'EIA_DATA')

RAW_TEMPERATURE_DATA = config.get('S3', 'RAW_TEMPERATURE_DATA')
TEMPERATURE_DATA = config.get('S3', 'TEMPERATURE_DATA')

RAW_DEMOGRAPHICS_DATA = config.get('S3', 'RAW_DEMOGRAPHICS_DATA')
DEMOGRAPHICS_DATA = config.get('S3', 'DEMOGRAPHICS_DATA')

RAW_REFERENCE_DATA = config.get('S3', 'RAW_REFERENCE_DATA')
REFERENCE_DATA = config.get('S3', 'REFERENCE_DATA')

s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

def upload_to_aws(local_file, bucket, s3_file):
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

def copy_s3_to_redshift(cur, conn, dest_table, s3_folder, file):
    sql = """
    COPY {} FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    CSV
    IGNOREHEADER 1
    REGION 'us-east-2';
    """.format(dest_table, s3_folder + file, ARN)

    print(s3_folder + file)
    
    try:
        cur.execute(sql)
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

def insert_tables(cur, conn):
    '''
    Inserts data into staging tables specified in the insert_table_queries list. 
    '''
    
    for query in insert_table_queries:
        cur.execute(query)
        cur.execute("commit;")
        print('Insert Complete')

def eia_insert_tables(cur, conn):
    '''
    Inserts data into staging tables specified in the insert_table_queries list. 
    '''
    
    for query in eia_insert_table_queries:
        cur.execute(query)
        cur.execute("commit;")
        print('EIA Insert Complete')

def data_cleansing(cur, conn):
    '''
    Inserts data into staging tables specified in the insert_table_queries list. 
    '''
    
    for query in eia_data_cleansing_steps:
        cur.execute(query)
        cur.execute("commit;")
        print('EIA cleansing step complete')

def load_eia_staging(cur, conn, dest_table, s3_folder):
    '''
    This code loads each excel file in the repo, transforms it, exports it as a csv, loads it to an S3 bucket,
    then copies all csvs in S3 to redshift at the end, then checks that source and destination rows match.
    '''
    
    files = eia_files
    headers = eia_headers

    for i, file in enumerate(files):
        total_rows = 0

        # filepath = 'eia_data/' + file
        filepath = RAW_EIA_DATA + file
        key = 'source_eia/' + file
        
        # #Might need to use this to read file
        # s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_excel(io.BytesIO(obj['Body'].read()), sheet_name=0, header=headers[i])

        #Other replace statements don't work for some reason, will fix later
        df = df.replace(np.nan, 0)

        # df = pd.read_excel(filepath, sheet_name=0, header=headers[i])
        
        df.columns = eia_raw_columns_names #rename columns to cleaner names
        df[eia_raw_text_columns] = df[eia_raw_text_columns].replace('.', None)
        df[eia_raw_numeric_columns] = df[eia_raw_numeric_columns].replace(np.nan, 0)
        df[eia_raw_numeric_columns] = df[eia_raw_numeric_columns].replace('.', 0)
        
        df = df.astype(str) #converting columns objects to avoid issues in wide_to_long

        # Unpivot the data to get months on rows
        df = pd.wide_to_long(df, 
                            stubnames=['quantity','elec_quantity','mmbtu_per_unit','tot_mmbtu','elec_mmbtu','netgen'], 
                            i=eia_raw_dim_columns,
                            j='month', sep='_', suffix=r'\D+').reset_index()
        
        # print(list(df.columns))
        total_rows = total_rows + (df.shape[0])
        
        df['month_number'] = df['month'].apply(lambda x: datetime.datetime.strptime(x, "%B").month) #Add month number for calcs later on
        
        if file.find('xlsx') > 0:
            csv_file = file.lower().replace('.xlsx','.csv')
        else:
            csv_file = file.lower().replace('.xls','.csv')        
        
        # Try removing na_rep later
        df.to_csv('eia_data_clean/' + csv_file, index=False, na_rep='0')

        uploaded = upload_to_aws('eia_data_clean/' + csv_file, BUCKET, 'eia/' + csv_file)
        print(csv_file + ' has been copied to S3')

        copy_s3_to_redshift(cur, conn, dest_table, s3_folder, csv_file)
        source_dest_check(cur, conn, total_rows, dest_table)

        # Delete data from staging table
        try:
            cur.execute('''
            delete from staging_eia
            where quantity + elec_quantity + mmbtu_per_unit + tot_mmbtu + elec_mmbtu + netgen = 0
            ''')
            cur.execute("commit;")
            print('Zero total rows were deleted')
        except Exception as e:
            print('There was an error deleting zero total rows')
            print(e)

        data_cleansing(cur, conn)
        eia_insert_tables(cur, conn)

        # Delete data from staging table
        try:
            cur.execute('delete from staging_eia')
            cur.execute("commit;")
            print('Data was successfully deleted from the staging table')
        except Exception as e:
            print('There was an error deleting data from the staging table')
            print(e)
        
        df = None
    


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

    copy_s3_to_redshift(cur, conn, dest_table, s3_folder, file)
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

    copy_s3_to_redshift(cur, conn, dest_table, s3_folder, file)
    source_dest_check(cur, conn, total_rows, dest_table)

def load_reference_data(cur, conn, s3_folder):
    '''
    This code loads each csv file in the repo to check source rows, loads it to an S3 bucket,
    copies each csv in S3 to redshift, then checks that source and destination rows match.
    '''
    
    total_rows = 0
    files = ['dim_aer_fuel_type.csv', 'dim_balancing_authority.csv', 'dim_census_region.csv', 'dim_eia_sector.csv', 'dim_nerc_region.csv', 'dim_reported_fuel_type.csv', 'dim_reported_prime_mover.csv']
    
    for file in files:
        total_rows = 0
        
        filepath = 'reference_data/' + file

        df = pd.read_csv(filepath)
        total_rows = total_rows + (df.shape[0])
        
        uploaded = upload_to_aws('reference_data/' + file, BUCKET, 'reference/' + file)
        print(file + ' has been copied to S3')
        
        dest_table = file.replace('.csv', '')
        copy_s3_to_redshift(cur, conn, dest_table, s3_folder, file)
        source_dest_check(cur, conn, total_rows, dest_table)
    
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

    load_reference_data(cur, conn, REFERENCE_DATA)

    load_temperature_staging(cur, conn, 'staging_temperature', TEMPERATURE_DATA)
    load_demographics_staging(cur, conn, 'staging_demographics', DEMOGRAPHICS_DATA)
    insert_tables(cur, conn)

    print('ALL TEMP AND DEMO STAGING TABLES HAVE BEEN LOADED AND MODEL TABLE INSERTS ARE COMPLETE')

    load_eia_staging(cur, conn, 'staging_eia', EIA_DATA)

    print('ALL STAGING TABLES HAVE BEEN LOADED')
    
    # data_quality_checks(cur, conn)

    conn.close()
    
    print('\nPIPELINE RUN COMPLETE!')


if __name__ == "__main__":
    main()