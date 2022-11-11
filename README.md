# firebolt-net-sdk

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Nuget](https://img.shields.io/nuget/v/FireboltNetSDK?style=plastic)](https://www.nuget.org/packages/FireboltNetSDK/0.0.1)

This is an implementation of .NET Core driver(.NET 6) for Firebolt DB in a form of ADO.NET DbProvider API.
Supports all latest .NET frameworks and all platforms.

Array query parameters are not supported yet.

This project is developed under Visual Studio 2022. Earlier versions of Visual Studio are not supported.


Installing the Package
======================

Packages can be directly downloaded from [nuget.org](https://www.nuget.org/).

It can also be downloaded using Visual Studio UI (Tools > NuGet Package Manager > Manage NuGet Packages for Solution and search for "Firebolt")

Alternatively, packages can also be downloaded using Package Manager Console:
```{r, engine='bash', code_block_name}
PM> Install-Package FireboltNetSDK -Version 0.0.1
```

Examples
======================

The following example demonstrates how to open a connection to Firebolt

```cs
            string database = "****";
            var username = "****";
            var password = "****";
            string endpoint = "****";
            string account = "firebolt";
            string engine = "****";
            string conn_string = $"database={database};username={username};password={password};endpoint={endpoint};account={account}";
          
            using var conn = new FireboltConnection(conn_string);
           
            conn.Open();

            conn.Close();
```
Connect and set specific engine if empty will take default

```cs
    
            string conn_string = $"database={database};username={username};password={password};endpoint={endpoint};";
          
            using var conn = new FireboltConnection(conn_string);
           
            conn.Open();

            conn.SetEngine(engine);

            conn.Close();
```

Execute command

```cs

            var connString = $"database={_database};username={_username};password={_password};endpoint={_endpoint};";

            using var conn = new FireboltConnection(connString);
            conn.Open();

            var cursor = conn.CreateCursor();

            cursor.Execute("SELECT 1");

            conn.Close();
```

Execute command with SET parameter

```cs
            var connString = $"database={_database};username={_username};password={_password};endpoint={_endpoint};account={_account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();

            var cursor = conn.CreateCursor();
            cursor.Execute("SET use_standard_sql=0");

            cursor.Execute("SELECT 1");

            conn.Close();
```

Execute command with parameters (Collection of parameters is Parameters)

```cs
            var connString = $"database={_database};username={_username};password={_password};endpoint={_endpoint};account={_account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();
            var cursor = conn.CreateCursor();

            var p = cursor.CreateParameter();

            p.ParameterName = "@param1";
            p.Value = 199;
            p.DbType = DbType.Int32;
            p.Direction = ParameterDirection.Input;

            cursor.Parameters.Add(p);

            cursor.Parameters.AddWithValue("@pass", date);

            cursor.Parameters.Add(new FireboltParameter("@str_param1") { Value = 200 });

            cursor.Execute("SELECT * FROM users WHERE password = @pass AND Age = @param1 AND Distance = @str_param1");

            conn.Close();
```

