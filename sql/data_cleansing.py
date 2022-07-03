replace_zeros = ('''
update staging_eia
set naics_code = null
where naics_code = '0.0'
''')

fix_year = ('''
update staging_eia
set year = (select max(year) from staging_eia)
where year = 0
''')


eia_data_cleansing_steps = [replace_zeros, fix_year]