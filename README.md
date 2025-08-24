# SQL Server to DuckDB Migration Utility
This utility examines the schemas contained within a list of provided SQL Server databases, then replicates the schema structure, exporting the data to DuckDB files

The application requires a config.json file in the root directory with the following information

```
{
    "server":{
        "host": "<host name or IP>",  
        "port": "<port>",  
        "drivername": "mssql+pymssql",
        "trusted_connection": "<yes if using Integrated Security>",
        "encrypt": "<yes" or no depending on server configuration>"
    },
    "databases": [
        "<database name>",
        "<database name>",
        "..."
    ]
}
```