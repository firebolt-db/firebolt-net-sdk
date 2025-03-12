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
PM> Install-Package FireboltNetSDK
```

Examples
======================

Following examples demonstrate how to connect and interact with Firebolt database using this driver:

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

###### Opening and closing a connection

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

###### Executing a SQL command that does not return result
```cs
// First you would need to create a command
var command = conn.CreateCommand();

// ... and set the SQL query
command.CommandText = "CREATE DATABASE IF NOT EXISTS MY_DB";

// Execute a SQL query and get a DB reader
command.ExecuteNonQuery();

// Close the connection after all operations are done
conn.Close();
```


###### Executing a SQL command that returns a result

```cs
// First you would need to create a command
var command = conn.CreateCommand();

// ... and set the SQL query
command.CommandText = "SELECT * FROM my_table";

// Execute a SQL query and get a DB reader
DbDataReader reader = command.ExecuteReader();

// Optionally you can check whether the result set has rows
Console.WriteLine($"Has rows: {reader.HasRows}");

// Discover the result metadata
int n = reader.FieldCount();
for (int i = 0; i < n; i++)
{
  Type type = reader.GetFieldType();
  string name = reader.GetName();
}

// Iterate over the rows and get values
while (reader.Read())
{
    for (int i = 0; i < n; i++)
    {
        Console.WriteLine($"{reader.GetName(i)}:{reader.GetFieldType(i)}={reader.GetValue(i)}");
    }
}
```

###### Executing a command with SET parameter

```cs
var tz = conn.CreateCommand();
tz.CommandText = "SET time_zone=America/New_York";
tz.ExecuteNonQuery();

tz.CommandText = "SELECT '2000-01-01 12:00:00.123456 Europe/Berlin'::timestamptz as t";
DbDataReader tzr = tz.ExecuteReader();
if (tzr.Read())
{
  // 2000-01-01 06:00:00.123456-05
  Console.WriteLine(tzr.GetDateTime(0));
}
```

###### Server-side async query execution

Firebolt supports server-side asynchronous query execution. This feature allows you to run queries in the background and fetch the results later. This is especially useful for long-running queries that you don't want to wait for or maintain a persistent connection to the server.

**Execute Async Query**

Executes a query asynchronously. This is useful for long-running queries like data manipulation operations that you don't want to block execution with. The result does not contain data and is used to receive an async query token. This token can be saved and reused, even with a new connection, to check on this query later.

```cs
var command = conn.CreateCommand();
command.CommandText = "INSERT INTO large_table SELECT * FROM source_table";

// Execute the query asynchronously
command.ExecuteAsyncNonQuery();

// Alternatively execute it asynchronously
// await command.ExecuteAsyncNonQueryAsync();

// Get the token for checking status later
string token = command.AsyncToken;
```

**Check Async Query Status**

Check the status of an asynchronous query to determine if it's still running or has completed.

`IsAsyncQueryRunning` would return true or false if the query is running or has finished. `IsAsyncQuerySuccessful` would return true if the query has completed successfully, false if it has failed and null if the query is still running

```cs
// Check if the query is still running
bool isRunning = conn.IsAsyncQueryRunning(token);
// or asynchronously
bool isRunning = await conn.IsAsyncQueryRunningAsync(token);

// Check if the query completed successfully (returns null if still running)
bool? isSuccessful = conn.IsAsyncQuerySuccessful(token);
// or asynchronously
bool? isSuccessful = await conn.IsAsyncQuerySuccessfulAsync(token);
```

**Cancel Async Query**

Cancel a running asynchronous query if its execution is no longer needed.

```cs
// Cancel the async query
bool cancelled = conn.CancelAsyncQuery(token);
// or asynchronously
bool cancelled = await conn.CancelAsyncQueryAsync(token);
```
