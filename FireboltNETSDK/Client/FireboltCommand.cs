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
        internal static readonly string BYTE_ARRAY_PREFIX = "\\x";

        private FireboltConnection? _connection;
        private string? _commandText;
        private bool _designTimeVisible = true;
        private DbParameterCollection _parameters;

        public readonly HashSet<string> SetParamList;

        public FireboltCommand() : this(null, null)
        {
        }

        public FireboltCommand(FireboltConnection? connection, string? commandText, params DbParameter[] parameters) : this(connection, commandText, new FireboltParameterCollection(parameters))
        {
        }

        public FireboltCommand(FireboltConnection? connection, string? commandText, DbParameterCollection parameters)
        {
            _connection = connection;
            _commandText = commandText;
            _parameters = parameters;
            SetParamList = _connection?.SetParamList ?? new();
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
        private new FireboltConnection? Connection
        {
            get => _connection;
            set => _connection = value;
        }

        /// <summary>
        /// Gets or sets the connection within which the command executes. Always returns <b>null</b>.
        /// </summary>
        /// <returns><b>null</b></returns>
        /// <exception cref="NotSupportedException">The value set is not <b>null</b>.</exception>
        protected override DbConnection? DbConnection
        {
            get => _connection;
            set => _connection = value == null ? null : (FireboltConnection)value;
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
            CancellationTokenSource cancellationTokenSource = CommandTimeoutMillis == 0 ? new CancellationTokenSource() : new CancellationTokenSource(CommandTimeoutMillis);
            return CreateQueryResult(ExecuteCommandAsync(commandText, cancellationTokenSource.Token).GetAwaiter().GetResult());
        }

        private QueryResult? CreateQueryResult(string? response)
        {
            return response == null ? new QueryResult() : GetOriginalJsonData(response);
        }

        private DbDataReader CreateDbDataReader(QueryResult? queryResult)
        {
            return queryResult != null ? new FireboltDataReader(null, queryResult, 0) : throw new InvalidOperationException("No result produced");
        }

        private async Task<string?> ExecuteCommandAsync(string commandText, CancellationToken cancellationToken)
        {
            if (Connection == null)
            {
                throw new FireboltException("Unable to execute SQL as no connection was initialised. Create command using working connection");
            }
            if (Connection.Client == null)
            {
                throw new FireboltException("Client is undefined. Initialize connection properly");
            }
            var engineUrl = Connection?.EngineUrl;
            if (commandText.Trim().ToUpper().StartsWith("SET"))
            {
                commandText = ValidateSetCommand(commandText.Remove(0, 4).Trim());
                SetParamList.Add(commandText);
                try
                {
                    Connection?.ValidateConnection();
                }
                catch (FireboltException e)
                {
                    SetParamList.Remove(commandText);
                    throw e;
                }
                return await Task.FromResult<string?>(null);
            }
            string newCommandText = commandText;
            if (Parameters.Any())
            {
                newCommandText = GetParamQuery(commandText);
            }

            var database = Connection?.Database != string.Empty ? Connection?.Database : null;

            Task<string?> t = Connection!.Client.ExecuteQueryAsync(engineUrl, database, Connection?.AccountId, newCommandText, SetParamList, cancellationToken);
            return await t;
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
            if (cancellationToken.IsCancellationRequested)
            {
                throw new TaskCanceledException();
            }
            CancellationTokenRegistration registration = new CancellationTokenRegistration();
            if (cancellationToken.CanBeCanceled)
            {
                registration = cancellationToken.Register(Cancel);
            }
            return ExecuteCommandAsync(StrictCommandText, cancellationToken).ContinueWith(result => CreateDbDataReader(CreateQueryResult(result.Result)));
        }

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            return ExecuteDbDataReaderAsync(CommandBehavior.Default, cancellationToken).ContinueWith(r => CreateScalar(r.Result));
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
                    string pattern = string.Format(@"\{0}\b", parameter.ParameterName);
                    string verifyParameters = GetParamValue(parameter.Value);
                    commandText = Regex.Replace(commandText, pattern, verifyParameters, RegexOptions.IgnoreCase);
                }
                return commandText;
            }
            catch (System.Exception ex)
            {
                throw new FireboltException("Error while verifying parameters for query", ex);
            }
        }

        private string GetParamValue(object? value)
        {
            var escape_chars = new Dictionary<string, string>
            {
                { "\0", "\\0" },
                { "\\", "\\\\" },
                { "'", "\\'" }
            };
            var verifyParameters = value?.ToString() ?? "";
            if (value is string && value != null)
            {
                string? sourceText = value.ToString();
                if (sourceText == null)
                    throw new FireboltException("Unexpected error: Unable to cast string value to string.");
                foreach (var item1 in escape_chars)
                {
                    sourceText = sourceText.Replace(item1.Key, item1.Value);
                }

                verifyParameters = "'" + sourceText + "'";
            }
            else if (value is DateTime)
            {
                DateTime dt = (DateTime)value;
                string format = dt.Hour == 0 && dt.Minute == 0 && dt.Second == 0 ? "yyyy-MM-dd" : "yyyy-MM-dd HH:mm:ss";
                verifyParameters = "'" + dt.ToString(format) + "'";
            }
            else if (value is DateTimeOffset)
            {
                verifyParameters = "'" + ((DateTimeOffset)value).ToString("yyyy-MM-dd HH:mm:ss.FFFFFFz") + "'";
            }
            else if (value is DateOnly)
            {
                verifyParameters = new string("'" + ((DateOnly)value).ToString("yyyy-MM-dd") + "'");
            }
            else if (value is null || value.ToString() == string.Empty)
            {
                verifyParameters = "NULL";
            }
            else if (value is byte[])
            {
                verifyParameters = "E'" + BYTE_ARRAY_PREFIX + BitConverter.ToString((byte[])value).Replace("-", BYTE_ARRAY_PREFIX) + "'::BYTEA";
            }
            else if (typeof(IList).IsAssignableFrom(value.GetType())) // works for lists and arrays
            {
                IList list = (IList)value;
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < list.Count; i++)
                {
                    sb.Append(GetParamValue(list[i]));
                    if (i < list.Count - 1)
                    {
                        sb.Append(",");
                    }
                }
                sb.Append("]");
                verifyParameters = sb.ToString();
            }
            else if (value is IConvertible)
            {
                // IConvertable is s a common interface for many numeric types, boolean and others.
                // String representation of numbers (result of ToString()) depends on the current locale. 
                // Some locales use comma instead or period to separate integer from the fractional part of number, 
                // so making this representation portable requires replacing comma by dot in string. 
                // The easier solution is to specify "standard" locale e.g. en_US.
                verifyParameters = ((IConvertible)value).ToString(new CultureInfo("en-US", false));
            }
            return verifyParameters;
        }

        /// <summary>
        /// Gets original data in JSON format for further manipulation.
        /// </summary>
        /// <returns>The data in JSON format</returns>
        private QueryResult? GetOriginalJsonData(string? Response)
        {
            if (Response == null) throw new FireboltException("Response is empty while GetOriginalJSONData");
            try
            {
                var prettyJson = JToken.Parse(Response).ToString(Formatting.Indented);
                return JsonConvert.DeserializeObject<QueryResult>(prettyJson);
            }
            catch (JsonReaderException e)
            {
                throw new FireboltException($"Failed to execute a query. Invalid response body format. Try again or contact support.", e);
            }
        }

        public void ClearSetList()
        {
            _connection?.SetParamList.Clear();
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
        public override bool DesignTimeVisible
        {
            get => _designTimeVisible;
            set => _designTimeVisible = value;
        }

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

        private object? CreateScalar(DbDataReader reader)
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
            await ExecuteCommandAsync(StrictCommandText, cancellationToken);
            return await Task.FromResult(0);
        }
    }
}
