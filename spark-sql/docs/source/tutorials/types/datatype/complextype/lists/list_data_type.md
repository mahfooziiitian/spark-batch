# List data type

    SELECT DEST_COUNTRY_NAME as new_name, collect_list(count) as flight_counts,
    collect_set(ORIGIN_COUNTRY_NAME) as origin_set
    FROM flights
    GROUP BY DEST_COUNTRY_NAME having size(collect_list(count))>1
            