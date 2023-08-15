# firebolt-net-sdk

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Nuget](https://img.shields.io/nuget/v/FireboltNetSDK?style=plastic)](https://www.nuget.org/packages/FireboltNetSDK/0.0.1)
[![Build](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/build.yml/badge.svg)](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/build.yml)
[![Unit tests](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/unit-tests.yml)
[![Code quality checks](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/code-check.yml/badge.svg)](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/code-check.yml)

This is an implementation of .NET 6 Core driver for Firebolt in a form of a [DbConnection](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbconnection?view=net-6.0) class.
Supports all latest .NET frameworks and all platforms.

This project is developed under Visual Studio 2022. Earlier versions of Visual Studio are not supported.


Installing the Package
======================

Here is a FireboltNetSDK [NuGet page](https://www.nuget.org/packages/FireboltNetSDK/).
- Install using **.NET CLI**
```{r, engine='bash', code_block_name}
dotnet add package FireboltNetSDK
```   
- Install using **Visual Studio UI**
  - `Tools` > `NuGet Package Manager` > `Manage NuGet Packages for Solution` and search for `Firebolt`   
- Install using **Package Manager Console**:
```{r, engine='bash', code_block_name}
PM> Install-Package FireboltNetSDK -Version 0.*
```

Examples
======================

The following examples demonstrate how to connect and interact with Firebolt database using this driver:

###### Creating a connection string

```cs
// Name of your Firebolt account
string account = "my_firebolt_account";
// Client credentials, that you want to use to connect
string clientId = "my_client_id";
string clientSecret = "my_client_secret";
// Name of database and engine to connect to (Optional)
string database = "my_database_name";
string engine = "my_engine_name";

// Construct a connection string using defined parameter
string conn_string = $"account={account};clientid={clientId};clientsecret={clientSecret};database={database};engine={engine}";
```

###### Creating and closing a connection

```cs
using FireboltDotNetSdk.Client;

// Create a new connection using generated connection string
using var conn = new FireboltConnection(conn_string);
// Open a connection
conn.Open();

// Execute SQL, fetch data, ...

// Close the connection after all operations are done
conn.Close();
```

###### Executing a SQL command

```cs
// First you would need to create a cursor
var cursor = conn.CreateCursor();

// Execute a SQL query and get a response object
var resp = cursor.Execute("SELECT * FROM my_table");

// Get the amount of rows returned
Console.WriteLine($"Fetched {resp.Rows} rows")

// Get column names and types
var columns = String.Join(", ", resp.Meta.Select(x => $"{x.Name}({x.Type})"));
Console.WriteLine($"Result columns: {columns}");

// Fetch the data from response object
foreach (var row in resp.Data) {
    Console.WriteLine(String.Join(",", row));
}
```

Execute command with SET parameter

```cs
var cursor = conn.CreateCursor();
cursor.Execute("SET time_zone=America/New_York");

var resp = cursor.Execute("SELECT '2000-01-01 12:00:00.123456 Europe/Berlin'::timestamptz as t");

// 2000-01-01 06:00:00.123456-05
Console.WriteLine(resp.Data[0][0]);
```
