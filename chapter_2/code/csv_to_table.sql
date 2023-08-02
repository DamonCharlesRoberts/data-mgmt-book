CREATE TABLE anes_raw AS 
SELECT *
FROM read_csv_auto('~/Desktop/anes_timeseries_cdf_csv_20220916/anes_timeseries_cdf_csv_20220916.csv', all_varchar = true)
COMMIT;