# View

        |CREATE VIEW if not exists just_usa_view AS
        |SELECT * FROM flights WHERE dest_country_name = 'United States'
        
        |CREATE TEMP VIEW just_usa_view_temp AS
        |SELECT * FROM flights WHERE dest_country_name = 'United States'
        
        |CREATE GLOBAL TEMP VIEW  just_usa_global_view_temp AS
        |SELECT * FROM flights WHERE dest_country_name = 'United States'
      

  
        |select * from global_temp.just_usa_global_view_temp
        

    
        |SHOW TABLES
