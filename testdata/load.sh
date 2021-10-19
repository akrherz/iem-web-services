# Load our testing data into the database

psql -v "ON_ERROR_STOP=1" -f _postgis_ugcs.sql postgis

for db in afos postgis mesosite other
do
    for fn in $(ls ${db}*.sql)
    do
        psql -v "ON_ERROR_STOP=1" -f $fn $db
    done
done
