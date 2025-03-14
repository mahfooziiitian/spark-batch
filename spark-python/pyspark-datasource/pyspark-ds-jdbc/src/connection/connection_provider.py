"""
The name of the JDBC connection provider to use to connect to this URL, e.g. db2, mssql.
Must be one of the providers loaded with the JDBC data source.
Used to disambiguate when more than one provider can handle the specified driver and options.
The selected provider must not be disabled by spark.sql.sources.disabledJdbcConnProviderList.
"""
