# firebolt-net-sdk

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Nuget](https://img.shields.io/nuget/v/FireboltNetSDK?style=plastic)](https://www.nuget.org/packages/FireboltNetSDK/0.0.1)
[![Build](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/build.yml/badge.svg)](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/build.yml)
[![Unit tests](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/unit-tests.yml)
[![Code quality checks](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/code-check.yml/badge.svg)](https://github.com/firebolt-db/firebolt-net-sdk/actions/workflows/code-check.yml)

This is an implementation of .NET Core driver(.NET 6) for Firebolt DB in a form of ADO.NET DbProvider API.
Supports all latest .NET frameworks and all platforms.

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
            var clientId = "****";
            var clientSecret = "****";
            string endpoint = "****";
            string account = "firebolt";
            string engine = "****";
            string conn_string = $"database={database};clientid={clientId};clientsecret={clientSecret};endpoint={endpoint};account={account}";

            using var conn = new FireboltConnection(conn_string);

            conn.Open();

            conn.Close();
```
Connect and set specific engine if empty will take default

```cs

            string conn_string = $"database={database};clientid={clientId};clientsecret={clientSecret};endpoint={endpoint};";

            using var conn = new FireboltConnection(conn_string);

            conn.Open();

            conn.SetEngine(engine);

            conn.Close();
```

Execute command

```cs

            var connString = $"database={_database};clientid={_clientId};clientsecret={_clientSecret};endpoint={_endpoint};";

            using var conn = new FireboltConnection(connString);
            conn.Open();

            var cursor = conn.CreateCursor();

            cursor.Execute("SELECT 1");

            conn.Close();
```

Execute command with SET parameter

```cs
            var connString = $"database={_database};clientid={_clientId};clientsecret={_clientSecret};endpoint={_endpoint};account={_account}";

            using var conn = new FireboltConnection(connString);
            conn.Open();

            var cursor = conn.CreateCursor();
            cursor.Execute("SET use_standard_sql=0");

            cursor.Execute("SELECT 1");

            conn.Close();
```
