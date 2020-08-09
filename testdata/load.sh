# Load our testing data into the database

psql -f _postgis_ugcs.sql postgis

for db in postgis
do
    for fn in $(ls ${db}*.sql)
    do
        psql -f $fn $db
    done
done
