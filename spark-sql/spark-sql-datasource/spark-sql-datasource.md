# CREATE DATASOURCE TABLE

    CREATE TABLE [ IF NOT EXISTS ] table_identifier
        [ ( col_name1 col_type1 [ COMMENT col_comment1 ], ... ) ]
        USING data_source
        [ OPTIONS ( key1=val1, key2=val2, ... ) ]
        [ PARTITIONED BY ( col_name1, col_name2, ... ) ]
        [ CLUSTERED BY ( col_name3, col_name4, ... ) 
            [ SORTED BY ( col_name [ ASC | DESC ], ... ) ] 
            INTO num_buckets BUCKETS ]
        [ LOCATION path ]
        [ COMMENT table_comment ]
        [ TBLPROPERTIES ( key1=val1, key2=val2, ... ) ]
        [ AS select_statement ]

## Json

    create table complex_json 
    using json
    options(
        'multiline'=true,header=true
    )

### view

    CREATE OR REPLACE TEMPORARY VIEW complex_type 
    USING json OPTIONS (
        path 'file:/mnt/d/Data/FileData/Json/complex_type_null_array.json',
        multiline true
    )
