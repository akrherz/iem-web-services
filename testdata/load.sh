# Load our testing data into the database

psql -f _postgis_ugcs.sql postgis

for db in afos postgis mesosite other
do
    for fn in $(ls ${db}*.sql)
    do
        psql -f $fn $db
    done
done
