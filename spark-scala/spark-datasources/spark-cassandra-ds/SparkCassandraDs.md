**1. Create docker-compose to set up the cassandra server**

    docker compse up    

**2. Connect to cassandra shell**

    docker exec -it cassandra bash
    cqlsh

**3. Create keyspace in cassandra**

    CREATE KEYSPACE popularbooks 
        WITH replication = {
            'class': 'SimpleStrategy', 
            'replication_factor' : 1
        };

     CREATE KEYSPACE transport 
        WITH replication = {
            'class': 'SimpleStrategy', 
            'replication_factor' : 1
        };

**4. Create table**
    
    CREATE TABLE books_by_author(
        author_name text primary key, 
        book_name text, 
        publish_year int, 
        rating float, 
        genres set<text>
    );

    create table flight(
        DEST_COUNTRY_NAME text ,
        ORIGIN_COUNTRY_NAME text primary key,
        count int
    );


# Optimization
    
