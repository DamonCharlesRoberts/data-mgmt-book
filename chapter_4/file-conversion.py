# Title: File Conversion

# Notes:
    #* Description:
        #** A python script to convert the ./data/WaffleDivorce.csv file to a variety of formats
    #* Updated
        #** 2023-11-16
        #** dcr

# Load libraries
import polars as pl
import duckdb as db

# Download file file
df = pl.read_csv('./data/2020-anes/anes_timeseries_2020_csv_20220210.csv', separator=';')

# Convert
    #* To DuckDB
        #** Connect to DuckDB database
con = db.connect('./data/2020-anes/anes.db')
        #** Write to table 'main'
db.execute(
    '''
        CREATE TABLE
            main
        AS
            SELECT
                *
            FROM
                df
    '''
)
    #* To Parquet
df.write_parquet('./data/2020-anes/anes.parquet')
    #* To Excel
df.write_excel('./data/2020-anes/anes.xlsx')
    #* STATA
# DOWNLOADED MANUALLY
    #* SAV
# DOWNLOADED MANUALLY

# Close database connection
con.close()
# Print Confirmation
print('Script successfully executed.')

