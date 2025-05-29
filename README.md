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

### Server-side Asynchronous Query Execution

Firebolt supports **server-side asynchronous query execution**, allowing queries to run in the background while you retrieve results later. This is particularly useful for long-running queries, as it eliminates the need to maintain a persistent connection to the server while waiting for execution to complete.

⚠ **Note:** This is different from .NET's asynchronous programming model. Firebolt's server-side async execution means that the query runs independently on the server, while .NET async/await handles non-blocking execution on the client side.

###### **Execute an Asynchronous Query**

Executing a query asynchronously means the database will start processing it in the background. Instead of returning data immediately, the response contains a **query token**, which can be used later (even in a new connection) to check the query status or retrieve results.

```cs
FireboltCommand command = (FireboltCommand)conn.CreateCommand();
command.CommandText = "INSERT INTO large_table SELECT * FROM source_table";

// Execute the query asynchronously on the server
command.ExecuteServerSideAsyncNonQuery();

// Alternatively, use .NET's async/await to avoid blocking the client thread
await command.ExecuteServerSideAsyncNonQueryAsync();

// Store the async query token for later use
string token = command.AsyncToken;
```

###### **Check the Status of an Asynchronous Query**

You can check if the query is still running or if it has finished executing.

- `IsServerSideAsyncQueryRunning(token)` returns `true` if the query is still in progress and `false` if it has finished.
- `IsServerSideAsyncQuerySuccessful(token)` returns:  
  - `true` if the query completed successfully  
  - `false` if the query failed  
  - `null` if the query is still running  

```cs
using FireboltConnection conn = new FireboltConnection(conn_string);
conn.Open();
// Check if the query is still running
bool isRunning = conn.IsServerSideAsyncQueryRunning(token);

// Check if the query completed successfully (returns null if it's still running)
bool? isSuccessful = conn.IsServerSideAsyncQuerySuccessful(token);
```
or use .NET asynchronous eqivalents
```cs
// Check if the query is still running
bool isRunning = await conn.IsServerSideAsyncQueryRunningAsync(token);

// Check if the query completed successfully (returns null if it's still running)
bool? isSuccessful = await conn.IsServerSideAsyncQuerySuccessfulAsync(token);
```

###### **Cancel an Asynchronous Query**

If an asynchronous query is no longer needed, you can cancel it before execution completes.

```cs
using FireboltConnection conn = new FireboltConnection(conn_string);
conn.Open();
// Cancel the async query
bool cancelled = conn.CancelServerSideAsyncQuery(token);
```
or do so asynchronously 
```cs
bool cancelled = await conn.CancelServerSideAsyncQueryAsync(token);
```

This approach ensures that long-running queries do not block your application while allowing you to monitor, manage, and cancel them as needed.

### Server-side Prepared Statement Execution

Firebolt supports **server-side prepared statement execution**, allowing better safety when using client provided inputs, not constructing the query server-side.

###### Prerequisites

To make use of server-side prepared statements, you need to provide the following parameter in your connection string:
```plaintext
preparedStatementParamStyle=FbNumeric
```
This makes any prepared statement constructed from the connection to use the `FbNumeric` parameter style, which is required for server-side prepared statements.

⚠ **Note:** Using this parameter, normal prepared statements will not work, so you need to use server-side prepared statements only.

Other than this parameter, the API is the same, except for the command text.
```cs
FireboltCommand command = conn.CreateCommand();
command.CommandText = "SELECT * FROM my_table WHERE id = $1";
command.Parameters.AddWithValue("$1", 123);
//command.Prepare(); is not needed

// Execute the query as any other command
using var reader = command.ExecuteReader();
```

###### Prepared Statement Parameter Style
The `preparedStatementParamStyle` parameter in the connection string can take the following values:
- `Native`(@paramName) - default: Uses the native parameter style, which is compatible with client-side prepared statements.
- `FbNumeric`($number): Uses Firebolt's numeric parameter style, which is required for server-side prepared statements.


### Query result streaming

Firebolt supports **query result streaming**, allowing you to retrieve large datasets in a memory-efficient manner. This is particularly useful for queries that return a significant amount of data, as it avoids loading the entire result set into memory at once.

###### Executing a Streaming Query

To execute a query that returns a large result set, you can use the `ExecuteStreamedQuery` method  or it's asynchronous equivalent `ExecuteStreamedQueryAsync`. This method allows you to stream the results directly from the server without loading them all into memory at once.

```cs
FireboltCommand command = (FireboltCommand)conn.CreateCommand();
command.CommandText = "SELECT * FROM large_table";

// Execute the query asynchronously on the server
using var reader = command.ExecuteStreamedQuery();
// or use the asynchronous version
using var reader = await command.ExecuteStreamedQueryAsync();

// Iterate over the streamed results in the same way as with a regular DbDataReader
while (await reader.ReadAsync())
{
    for (int i = 0; i < reader.FieldCount; i++)
    {
        Console.WriteLine($"{reader.GetName(i)}: {reader.GetValue(i)}");
    }
}
```
