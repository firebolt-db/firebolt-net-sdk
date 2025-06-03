#region License Apache 2.0
/* Copyright 2022 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion

using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net.Mime;
using System.Text.RegularExpressions;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using System.Text;

[assembly: InternalsVisibleTo("FireboltDotNetSdk.Tests")]
namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents an SQL statement to execute against a FireBolt database. This class cannot be inherited.
    /// </summary>
    public class FireboltCommand : DbCommand
    {
        private static readonly ISet<string> forbiddenParameters1 = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) { "DATABASE", "ENGINE", "ACCOUNT_ID", "OUTPUT_FORMAT" };
        private static readonly ISet<string> forbiddenParameters2 = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) { "DATABASE", "ENGINE", "OUTPUT_FORMAT" };
        private static readonly ISet<string> useSupporting = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) { "DATABASE", "ENGINE" };
        private static readonly string FORBIDDEN_PROPERTY_ERROR_PREFIX = "Could not set parameter. Set parameter '{0}' is not allowed. ";
        private static readonly string FORBIDDEN_PROPERTY_ERROR_USE_SUFFIX = "Try again with 'USE {0}' instead of SET.";
        private static readonly string FORBIDDEN_PROPERTY_ERROR_SET_SUFFIX = "Try again with a different parameter name.";
        private static readonly string USE_ERROR = FORBIDDEN_PROPERTY_ERROR_PREFIX + FORBIDDEN_PROPERTY_ERROR_USE_SUFFIX;
        private static readonly string SET_ERROR = FORBIDDEN_PROPERTY_ERROR_PREFIX + FORBIDDEN_PROPERTY_ERROR_SET_SUFFIX;
        private static readonly TimeSpan regexTimeout = TimeSpan.FromSeconds(5);
        internal static readonly string BYTE_ARRAY_PREFIX = "\\x";

        private string? _commandText;

        public HashSet<string> SetParamList { get; private set; }

        public string? AsyncToken { get; private set; }

        public FireboltCommand() : this(null, null)
        {
        }

        public FireboltCommand(FireboltConnection? connection, string? commandText, params DbParameter[] parameters) : this(connection, commandText, new FireboltParameterCollection(parameters))
        {
        }

        public FireboltCommand(FireboltConnection? connection, string? commandText, DbParameterCollection parameters)
        {
            Connection = connection;
            _commandText = commandText;
            SetParamList = Connection?.SetParamList ?? new();
            // as it is defined for MS SQL server https://learn.microsoft.com/en-us/dotnet/api/system.data.sqlclient.sqlcommand.commandtimeout?view=dotnet-plat-ext-7.0#remarks
            CommandTimeoutMillis = 30000;
        }

        /// <summary>
        ///Gets or sets the SQL statement to execute at the data source.
        /// </summary>
        [AllowNull]
        public override string CommandText
        {
            get => _commandText ?? string.Empty;
            set => _commandText = value;
        }

        private string StrictCommandText
        {
            get => _commandText ?? throw new InvalidOperationException("SQL command is null");
        }
        /// <summary>
        /// Gets the sets type of the command. The only supported type is <see cref="MediaTypeNames.Text"/>.
        /// </summary>
        /// <returns>The value <see cref="MediaTypeNames.Text"/>.</returns>
        /// <exception cref="NotSupportedException">The type set is not <see cref="MediaTypeNames.Text"/>.</exception>
        public override CommandType CommandType
        {
            get => CommandType.Text;

            set
            {
                if (value != CommandType.Text)
                    throw new NotSupportedException($"The type of the command \"{value}\" is not supported.");
            }
        }

        /// <summary>
        /// Gets or sets the <see cref="FireboltConnection"/> used by this command.
        /// </summary>
        public new FireboltConnection? Connection { get; set; }

        /// <summary>
        /// Gets or sets the connection within which the command executes. Always returns <b>null</b>.
        /// </summary>
        /// <returns><b>null</b></returns>
        /// <exception cref="NotSupportedException">The value set is not <b>null</b>.</exception>
        protected override DbConnection? DbConnection
        {
            get => Connection;
            set => Connection = value == null ? null : (FireboltConnection)value;
        }

        /// <summary>
        /// Gets or sets the transaction within which the command executes. The transation is ignored.
        /// </summary>
        /// <exception cref="NotSupportedException">The value set is not <b>null</b>.</exception>
        protected override DbTransaction? DbTransaction { get; set; }

        /// <summary>
        /// Gets the <see cref="FireboltParameterCollection"/>.
        /// </summary>
        /// <returns>The parameters of the SQL statement. The default is an empty collection.</returns>
        public new FireboltParameterCollection Parameters { get; } = new();

        /// <inheritdoc cref="Parameters"/>    
        protected sealed override DbParameterCollection DbParameterCollection => Parameters;

        public override int CommandTimeout
        {
            get => CommandTimeoutMillis / 1000;
            set => CommandTimeoutMillis = value * 1000;
        }

        // We store timeout in milliseconds and expose this property for tests. 
        // Otherwise it is impossible (or very difficult) to implement test that simulates query that runs longer than timeout. 
        internal int CommandTimeoutMillis { get; set; }

        internal QueryResult? Execute(string commandText)
        {
            return CreateQueryResult(ExecuteCommandAsyncWithCommandTimeout<string>(commandText, CancellationToken.None).GetAwaiter().GetResult());
        }

        private static QueryResult? CreateQueryResult(string? response)
        {
            return response == null ? new QueryResult() : GetOriginalJsonData(response);
        }

        private static DbDataReader CreateDbDataReader(QueryResult? queryResult)
        {
            return queryResult != null ? new FireboltDataReader(null, queryResult, 0) : throw new InvalidOperationException("No result produced");
        }

        private async Task<T?> ExecuteCommandAsyncWithCommandTimeout<T>(string commandText, CancellationToken cancellationToken)
        {
            if (CommandTimeoutMillis == 0)
            {
                return await ExecuteCommandAsync<T>(commandText, cancellationToken);
            }
            using (var timeoutSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(CommandTimeoutMillis)))
            using (var linkedTokenSource =
                   CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token))
            {
                try
                {
                    return await ExecuteCommandAsync<T>(commandText, linkedTokenSource.Token);
                }
                catch (TaskCanceledException) when (timeoutSource.Token.IsCancellationRequested)
                {
                    throw new FireboltTimeoutException(CommandTimeoutMillis);
                }
            }
        }

        private void VerifyConnection()
        {
            if (Connection == null)
            {
                throw new FireboltException("Unable to execute SQL as no connection was initialised. Create command using working connection");
            }
            if (Connection.Client == null)
            {
                throw new FireboltException("Client is undefined. Initialize connection properly");
            }
        }

        private async Task ProcessSetCommand(string commandText, CancellationToken cancellationToken)
        {
            VerifyConnection();
            commandText = ValidateSetCommand(commandText.Remove(0, 4).Trim());
            SetParamList.Add(commandText);
            try
            {
                await Connection!.ValidateConnection(cancellationToken);
            }
            catch (AggregateException)
            {
                SetParamList.Remove(commandText);
                throw;
            }
        }

        private async Task<T?> ExecuteCommandAsync<T>(string commandText, CancellationToken cancellationToken, bool isServerAsync = false)
        {
            VerifyConnection();
            var engineUrl = Connection!.EngineUrl;
            // If the command is a SET command, process it and return null
            // SET commands are not supported by the server-side async
            var isSetCommand = IsSetCommand(commandText);
            if (isSetCommand)
            {
                var isStreamingRequest = typeof(T) == typeof(StreamReader);
                if (isServerAsync || isStreamingRequest)
                {
                    throw new InvalidOperationException("SET commands are not supported by the server-side async");
                }
                await ProcessSetCommand(commandText, cancellationToken);
                return await Task.FromResult<T?>(default);
            }
            var newCommandText = commandText;
            var paramList = SetParamList;
            if (Parameters.Any())
            {
                if (Connection!.PreparedStatementParamStyle == PreparedStatementParamStyleType.FbNumeric)
                {
                    paramList.Add("query_parameters=" + GetServerSideQueryParameters());
                }
                else
                {
                    newCommandText = GetParamQuery(commandText);
                }
            }
            // Need to tell the backend we are running async
            if (isServerAsync)
            {
                paramList = new HashSet<string>(SetParamList)
                {
                    "async=true"
                };
            }

            return await Connection!.Client.ExecuteQueryAsync<T>(engineUrl, GetDatabase(), Connection.AccountId, newCommandText, paramList, cancellationToken);
        }

        private static bool IsSetCommand(string commandText)
        {
            return commandText.Trim().ToUpper().StartsWith("SET");
        }

        private string? GetDatabase()
        {
            return Connection?.Database != string.Empty ? Connection?.Database : null;
        }

        private string GetServerSideQueryParameters()
        {
            return JsonConvert.SerializeObject(Parameters.Select(parameter =>
                new
                {
                    name = parameter.ParameterName,
                    value = parameter.Value
                })
            );
        }

        private string ValidateSetCommand(string setCommand)
        {
            string name = setCommand.Split("=")[0].Trim();
            ISet<string> forbiddenParameters = Connection!.InfraVersion < 2 ? forbiddenParameters1 : forbiddenParameters2;
            if (forbiddenParameters.Contains(name))
            {
                throw new InvalidOperationException(string.Format(useSupporting.Contains(name) ? USE_ERROR : SET_ERROR, name));
            }
            return setCommand;
        }

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            if (cancellationToken.CanBeCanceled)
            {
                cancellationToken.Register(Cancel);
            }
            return ExecuteCommandAsyncWithCommandTimeout<string>(StrictCommandText, cancellationToken).ContinueWith(
                result => CreateDbDataReader(CreateQueryResult(result.Result)), cancellationToken);
        }

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            return ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ContinueWith(r => CreateScalar(r.Result), cancellationToken);
        }

        /// <summary>
        /// Get query with ready parse parameters
        /// </summary>
        /// <returns>Query with parameters ready to run.</returns>
        private string GetParamQuery(string commandText)
        {
            try
            {
                foreach (var parameter in Parameters.ToList())
                {
                    //need to validate parameters are of native type
                    if (!parameter.ParameterName.StartsWith('@'))
                    {
                        throw new FireboltException("Parameter name should start with '@' when using native PreparedStatementParamStyle");
                    }
                    string pattern = $@"\{parameter.ParameterName}\b";
                    string verifyParameters = GetParamValue(parameter.Value);
                    commandText = Regex.Replace(commandText, pattern, verifyParameters, RegexOptions.IgnoreCase, regexTimeout);
                }
                return commandText;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Error while verifying parameters for query", ex);
            }
        }

        private static string GetParamValue(object? value)
        {
            var escapeChars = new Dictionary<string, string>
            {
                { "\0", "\\0" },
                { "\\", "\\\\" },
                { "'", "\\'" }
            };
            var verifyParameters = value?.ToString() ?? "";
            switch (value)
            {
                case null:
                    verifyParameters = "NULL";
                    break;
                case string sourceText:
                    {
                        foreach (var escapedPair in escapeChars)
                        {
                            sourceText = sourceText.Replace(escapedPair.Key, escapedPair.Value);
                        }
                        verifyParameters = "'" + sourceText + "'";
                        break;
                    }
                case DateTime dateTime:
                    {
                        string format = dateTime is { Hour: 0, Minute: 0, Second: 0 } ? "yyyy-MM-dd" : "yyyy-MM-dd HH:mm:ss";
                        verifyParameters = "'" + dateTime.ToString(format) + "'";
                        break;
                    }
                case DateTimeOffset offset:
                    verifyParameters = "'" + offset.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFz") + "'";
                    break;
                case DateOnly dateOnly:
                    verifyParameters = new string("'" + dateOnly.ToString("yyyy-MM-dd") + "'");
                    break;
                case byte[] bytes:
                    verifyParameters = "E'" + BYTE_ARRAY_PREFIX + BitConverter.ToString(bytes).Replace("-", BYTE_ARRAY_PREFIX) + "'::BYTEA";
                    break;
                case IList list:
                    StringBuilder sb = new StringBuilder("[");
                    for (int i = 0; i < list.Count; i++)
                    {
                        sb.Append(GetParamValue(list[i]));
                        if (i < list.Count - 1)
                        {
                            sb.Append(',');
                        }
                    }
                    sb.Append(']');
                    verifyParameters = sb.ToString();
                    break;
                case IConvertible convertible:
                    // IConvertable is s a common interface for many numeric types, boolean and others.
                    // String representation of numbers (result of ToString()) depends on the current locale. 
                    // Some locales use comma instead or period to separate integer from the fractional part of number, 
                    // so making this representation portable requires replacing comma by dot in string. 
                    // The easier solution is to specify "standard" locale e.g. en_US.
                    verifyParameters = convertible.ToString(new CultureInfo("en-US", false));
                    break;
            }
            return verifyParameters;
        }

        /// <summary>
        /// Gets original data in JSON format for further manipulation.
        /// </summary>
        /// <returns>The data in JSON format</returns>
        private static QueryResult? GetOriginalJsonData(string? response)
        {
            if (response == null) throw new FireboltException("Response is empty while GetOriginalJSONData");
            try
            {
                var prettyJson = JToken.Parse(response).ToString(Formatting.Indented);
                return JsonConvert.DeserializeObject<QueryResult>(prettyJson);
            }
            catch (JsonReaderException e)
            {
                throw new FireboltException($"Failed to execute a query. Invalid response body format. Try again or contact support.", e);
            }
        }

        public void ClearSetList()
        {
            Connection?.SetParamList.Clear();
        }

        public override void Cancel()
        {
            // This is a call back method that is called when command is cancelled using CancellationTokenSource.Cancel()
            // when timeout is expired. Use this callback if any other closing operations are needed to completely cancel the running command.
            // Right now this implementation is empty.
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get => throw new NotImplementedException();
            set => throw new NotImplementedException();
        }

        /// <summary>
        /// Gets or sets a value indicating whether the command object should be visible in a customized interface control.
        /// </summary>
        public override bool DesignTimeVisible { get; set; } = true;

        /// <summary>
        /// Executes the command that should retrieve data against the connection
        /// </summary>
        /// <param name="behavior"><see cref="CommandBehavior"/>ignored by the implementation</param>
        /// <returns>Implementation of <see cref="DbDataReader"/></returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return CreateDbDataReader(Execute(StrictCommandText));
        }

        /// <summary>
        /// Creates DB parameter that can be used for query parameterization.
        /// </summary>
        /// <returns>Implementation of <see cref="DbParameterDbDataReader"/></returns>
        protected override DbParameter CreateDbParameter()
        {
            return new FireboltParameter();
        }

        /// <summary>
        /// Executes the command that does not retrieve data against the connection
        /// </summary>
        public override int ExecuteNonQuery()
        {
            return ExecuteNonQueryAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes the query and returns the first column of the first row.
        /// </summary>
        /// <returns>The first column of the first row in the result set.</returns>
        public override object? ExecuteScalar()
        {
            using (DbDataReader reader = ExecuteReader())
            {
                return CreateScalar(reader);
            }
        }

        private static object? CreateScalar(DbDataReader reader)
        {
            if (reader.Read() && reader.FieldCount > 0)
            {
                object result = reader.GetValue(0);
                if (DBNull.Value != result)
                {
                    return result;
                }
            }
            return null;
        }

        /// <summary>
        /// Creates prepared (or compiled) version of command. Right now does nothing.
        /// </summary>
        public override void Prepare()
        {
            // Empty implementation. Nothing to do here so far.
        }

        /// <summary>
        /// Asynchronous version of ExecuteNonQuery()
        /// </summary>
        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            await ExecuteCommandAsync<string>(StrictCommandText, cancellationToken);
            return await Task.FromResult(0);
        }

        /// <summary>
        /// Executes a query asynchronously on the server-side without returning results.
        /// The token to track the query status can be accessed via the AsyncToken property.
        /// </summary>
        /// <returns>Always returns 0.</returns>
        public int ExecuteServerSideAsyncNonQuery()
        {
            return ExecuteServerSideAsyncNonQueryAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes a query asynchronously on the server-side without returning results.
        /// The token to track the query status can be accessed via the AsyncToken property.
        /// </summary>
        /// <returns>A task representing the asynchronous operation. Always returns 0.</returns>
        public async Task<int> ExecuteServerSideAsyncNonQueryAsync(CancellationToken cancellationToken = default)
        {
            // Execute the query with the async parameter
            var response = await ExecuteCommandAsync<string>(StrictCommandText, cancellationToken, true);
            if (response == null)
            {
                throw new FireboltException("Failed to execute async query: no response received");
            }

            try
            {
                // Parse the async response which has a different format than regular queries
                var jsonResponse = JObject.Parse(response);
                var token = jsonResponse["token"]?.ToString();

                if (string.IsNullOrEmpty(token))
                {
                    throw new FireboltException("Invalid async query response format: missing or empty token");
                }

                // Store the token for later use
                AsyncToken = token;
                return 0;
            }
            catch (JsonReaderException ex)
            {
                throw new FireboltException("Failed to parse async query response", ex);
            }
        }

        /// <summary>
        /// Executes a query which returns it's results without storing them in memory.
        /// </summary>
        /// <returns>Always returns 0.</returns>
        public FireboltDataReader ExecuteStreamedQuery()
        {
            return ExecuteStreamedQueryAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Executes a query which returns it's results without storing them in memory.
        /// </summary>
        /// <returns>A task representing the asynchronous operation. Always returns 0.</returns>
        public async Task<FireboltDataReader> ExecuteStreamedQueryAsync(CancellationToken cancellationToken = default)
        {
            // Execute the query with the async parameter
            var streamReader = await ExecuteCommandAsyncWithCommandTimeout<StreamReader>(StrictCommandText, cancellationToken);
            if (streamReader == null)
            {
                throw new FireboltException("Failed to execute streamed query: no response received");
            }

            return new FireboltStreamingDataReader(streamReader);
        }
    }
}
