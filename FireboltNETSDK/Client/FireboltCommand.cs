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
using System.Net.Http.Headers;
using System.Net.Mime;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Text.RegularExpressions;
using FireboltDoNetSdk.Utils;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static FireboltDotNetSdk.Client.FireRequest;
using static FireboltDotNetSdk.Client.FireResponse;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents an SQL statement to execute against a FireBolt database. This class cannot be inherited.
    /// </summary>
    public sealed class FireboltCommand : DbCommand
    {
        private string? _commandText;

        public string? Response { get; set; }

        public static readonly HashSet<string> SetParamList = new();

        /// <summary>
        /// Gets or sets the SQL statement to execute at the data source.
        /// </summary>
        [AllowNull]
        public override string CommandText
        {
            get => _commandText ?? string.Empty;
            set => _commandText = value;
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
        private new FireboltConnection? Connection { get; set; }

        /// <summary>
        /// Gets or sets the connection within which the command executes. Always returns <b>null</b>.
        /// </summary>
        /// <returns><b>null</b></returns>
        /// <exception cref="NotSupportedException">The value set is not <b>null</b>.</exception>
        protected override DbConnection? DbConnection { get; set; }


        /// <summary>
        /// Gets or sets the transaction within which the command executes. Always returns <b>null</b>.
        /// </summary>
        /// <returns><b>null</b></returns>
        /// <exception cref="NotSupportedException">The value set is not <b>null</b>.</exception>
        protected override DbTransaction? DbTransaction
        {
            get => null;
            set
            {
                if (value != null)
                    throw new NotSupportedException($"{nameof(DbTransaction)} is read only.'");
            }
        }

        /// <summary>
        /// Gets the <see cref="FireboltParameterCollection"/>.
        /// </summary>
        /// <returns>The parameters of the SQL statement. The default is an empty collection.</returns>
        public new FireboltParameterCollection Parameters { get; } = new FireboltParameterCollection();

        /// <inheritdoc cref="Parameters"/>    
        protected sealed override DbParameterCollection DbParameterCollection => Parameters;

        public override int CommandTimeout { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        internal FireboltCommand(FireboltConnection connection) => Connection = connection ?? throw new ArgumentNullException(nameof(connection));

        public QueryResult Execute(string commandText)
        {
            var engineUrl = Connection?.Engine?.engine?.endpoint ?? Connection?.DefaultEngine?.Engine_url;
            if (commandText.Trim().StartsWith("SET"))
            {
                commandText = commandText.Remove(0, 4).Trim();
                SetParamList.Add(commandText);
                return new QueryResult();
            }
            else
            {
                try
                {
                    string newCommandText = commandText;
                    if (Parameters.Any())
                    {
                        newCommandText = GetParamQuery(commandText);
                    }
                    Response = Connection?.Client.ExecuteQuery(engineUrl, Connection.Database, newCommandText).GetAwaiter().GetResult();
                    //return FormDataForResponse(Response);
                    return GetOriginalJsonData();
                }
                catch (FireboltException ex)
                {
                    throw new FireboltException(ex.Message);
                }
            }
        }

        /// <summary>
        /// Get query with ready parse parameters<b>null</b>.
        /// </summary>
        /// <returns><b>null</b></returns>
        public string GetParamQuery(string commandText)
        {
            var escape_chars = new Dictionary<string, string>
            {
                { "\0", "\\0" },
                { "\\", "\\\\" },
                { "'", "\\'" }
            };
            try
            {
                foreach (var item in Parameters.ToList())
                {
                    //if (item.Value == null) { throw new FireboltException("Query parameter value cannot be null"); }
                    string pattern = string.Format(@"\{0}\b", item.ParameterName.ToString());
                    RegexOptions regexOptions = RegexOptions.IgnoreCase;
                    var verifyParameters = item.Value;
                    if (item.Value is string & item.Value != null)
                    {
                        string sourceText = item.Value.ToString();
                        foreach (var item1 in escape_chars)
                        {
                            sourceText = sourceText.Replace(item1.Key, item1.Value);
                        }
                        verifyParameters = "'" + sourceText + "'";
                    }
                    else if (item.Value is DateTime)
                    {
                        DateTime dt = (DateTime)item.Value;
                        string date_str = dt.ToString("yyyy-MM-dd HH:mm:ss");
                        date_str = dt.Hour == 0 && dt.Minute == 0 && dt.Second == 0 ? date_str.Split(' ')[0] : date_str;
                        verifyParameters = new string("'" + date_str + "'");
                    }
                    else if (item.Value is null || item.Value.ToString() == string.Empty)
                    {
                        verifyParameters = "NULL";
                    }
                    else if (item.Value is bool)
                    {
                        if ((bool)item.Value)
                        {
                            verifyParameters = "1";
                        }
                        else
                        {
                            verifyParameters = "0";
                        }
                    }
                    else if (item.Value is IList && item.Value.GetType().IsGenericType)
                    {
                        throw new FireboltException("Array query parameters are not supported yet.");
                    }
                    else if (item.Value is int || item.Value is long || item.Value is Double || item.Value is float || item.Value is decimal)
                    {
                        switch (item.Value.GetType().Name)
                        {
                            case "Decimal":
                                var decValue = (decimal)item.Value;
                                verifyParameters = decValue.ToString().Replace(',', '.');
                                break;
                            case "Double":
                                var doubleValue = (double)item.Value;
                                verifyParameters = doubleValue.ToString().Replace(',', '.');
                                break;
                            case "Single":
                                var floatValue = (float)item.Value;
                                verifyParameters = floatValue.ToString().Replace(',', '.');
                                break;
                            case "Int32":
                                var intValue = (int)item.Value;
                                verifyParameters = intValue.ToString();
                                break;
                            case "Int64":
                                var longValue = (long)item.Value;
                                verifyParameters = longValue.ToString();
                                break;
                            default:
                                break;
                        }
                    }
                    commandText = Regex.Replace(commandText, pattern, verifyParameters.ToString(), regexOptions);
                }
                return commandText;
            }
            catch (System.Exception)
            {
                throw new FireboltException("Error while verify parameters for query");
            }

        }

        /// <summary>
        /// Gets original data in JSON format for further manipulation<b>null</b>.
        /// </summary>
        /// <returns><b>null</b></returns>
        public QueryResult? GetOriginalJsonData()
        {
            if (Response == null) throw new FireboltException("Response is empty while GetOriginalJSONData");
            var prettyJson = JToken.Parse(Response).ToString(Formatting.Indented);
            return JsonConvert.DeserializeObject<QueryResult>(prettyJson);
        }

        /// <summary>
        /// Gets rowscount parameter from return data<b>null</b>.
        /// </summary>
        /// <returns><b>null</b></returns>
        public int RowCount()
        {
            var prettyJson = JToken.Parse(Response ?? throw new FireboltException("RowCount is missing")).ToString(Formatting.Indented);
            var data = JsonConvert.DeserializeObject<QueryResult>(prettyJson);
            return ((int)data.Rows)!;
        }

        public void ClearSetList()
        {
            SetParamList.Clear();
        }


        /// <summary>
        /// Not supported. To cancel a command execute it asynchronously with an appropriate cancellation token.
        /// </summary>
        /// <exception cref="NotImplementedException">Always throws <see cref="NotImplementedException"/>.</exception>
        public override void Cancel()
        {
            throw new NotImplementedException();
        }

        public void PrepareRequest(HttpClient client)
        {
            //Added to avoid 403 Forbidden error
            var version = Assembly.GetEntryAssembly().GetName().Version.ToString();
            var specificUserAgent = $".NETSDK/{version} (.NET {Environment.Version.ToString()}; {Environment.OSVersion})";
            client.DefaultRequestHeaders.Add("User-Agent", ".NETSDK/.NET6_" + version);

            if (!string.IsNullOrEmpty(Token))
            {
                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + Token);
            }
        }

        public string? Token { get; set; }
        public string RefreshToken { get; set; }
        public string TokenExpired { get; set; }

        /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <summary>
        /// Executes a SQL query
        /// </summary>
        /// <param name="engineEndpoint">Engine endpoint (URL)</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="query">SQL query to execute</param>
        /// <returns>A successful response.</returns>
        /// <exception cref="FireboltException">A server side error occurred.</exception>
        public async Task<string?> ExecuteQueryAsync(string? engineEndpoint, string databaseName, string query, CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(engineEndpoint) || string.IsNullOrEmpty(databaseName) || string.IsNullOrEmpty(query))
                throw new FireboltException($"Something parameters are null or empty: engineEndpoint: {engineEndpoint}, databaseName: {databaseName} or query: {query}");

            var urlBuilder = new StringBuilder();
            var setParam = SetParamList.Aggregate(string.Empty, (current, item) => current + ("&" + item));
            urlBuilder.Append("https://").Append(engineEndpoint).Append("?database=").Append(databaseName).Append(setParam).Append("&output_format=JSONCompact");

            var client = new HttpClient();
            var disposeClient = true;
            try
            {
                using var request = new HttpRequestMessage();
                var content = new StringContent(query);
                content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
                request.Content = content;
                request.Method = new HttpMethod("POST");
                request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("text/plain"));

                PrepareRequest();

                var url = urlBuilder.ToString();
                request.RequestUri = new Uri(url, System.UriKind.RelativeOrAbsolute);

                PrepareRequest(client);

                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                const bool disposeResponse = true;
                try
                {
                    var headers = response.Headers.ToDictionary(header => header.Key, header => header.Value);
                    foreach (var item in response.Content.Headers)
                        headers[item.Key] = item.Value;

                    ProcessResponse();

                    var status = (int)response.StatusCode;
                    if (status == 200)
                    {
                        ReadResponseAsString = true;
                        var objectResponse = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                        if (objectResponse == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status, objectResponse ?? string.Empty, headers, null);
                        }
                        return objectResponse;
                    }
                    else
                    {
                        throw new FireboltException("Response was null which was not expected with status: " + status);
                    }
                }
                finally
                {
                    if (disposeResponse)
                        response.Dispose();
                }
            }
            finally
            {
                this.ReadResponseAsString = false;
                if (disposeClient)
                    client.Dispose();
            }
        }

        private readonly Lazy<JsonSerializerSettings> _settings;

        public FireboltCommand(string baseUrl)
        {
            BaseUrl = baseUrl;
            _settings = new Lazy<JsonSerializerSettings>(CreateSerializerSettings);
        }

        public static JsonSerializerSettings CreateSerializerSettings()
        {
            var settings = new JsonSerializerSettings();
            UpdateJsonSerializerSettings();
            return settings;
        }

        private string BaseUrl { get; } = "";

        private JsonSerializerSettings JsonSerializerSettings => _settings.Value;
        private static void UpdateJsonSerializerSettings() { }
        private static void PrepareRequest() { }
        private static void ProcessResponse() { }

        /// <summary>
        /// Creates new user session
        /// </summary>
        /// <param name="body">Login credentials</param>
        /// <returns>A successful response.</returns>
        /// <exception cref="FireboltException">A server side error occurred.</exception>
        public Task<LoginResponse> AuthV1LoginAsync(LoginRequest body)
        {
            return AuthV1LoginAsync(body, CancellationToken.None);
        }

        /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <summary>
        /// Creates new user session
        /// </summary>
        /// <param name="body">Login credentials</param>
        /// <returns>A successful response.</returns>
        /// <exception cref="FireboltException">A server side error occurred.</exception>
        private async Task<LoginResponse> AuthV1LoginAsync(LoginRequest body, CancellationToken cancellationToken)
        {
            if (body == null)
                throw new ArgumentNullException(nameof(body));

            var urlBuilder = new StringBuilder();
            urlBuilder.Append(BaseUrl.TrimEnd('/')).Append("/auth/v1/login");

            var client = new HttpClient();
            const bool disposeClient = true;
            try
            {
                using var request = new HttpRequestMessage();
                var content = new StringContent(JsonConvert.SerializeObject(body, _settings.Value));
                content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");
                request.Content = content;
                request.Method = new HttpMethod("POST");
                request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

                PrepareRequest();

                var url = urlBuilder.ToString();
                request.RequestUri = new System.Uri(url, System.UriKind.RelativeOrAbsolute);

                PrepareRequest(client);

                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                const bool disposeResponse = true;
                try
                {
                    var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
                    foreach (var item in response.Content.Headers)
                        headers[item.Key] = item.Value;

                    ProcessResponse();

                    var status = (int)response.StatusCode;
                    if (status == 200)
                    {
                        var objectResponse =
                            await ReadObjectResponseAsync<LoginResponse>(response, headers, cancellationToken)
                                .ConfigureAwait(false);
                        if (objectResponse.Object == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status,
                                objectResponse.Text, headers, null);
                        }

                        return objectResponse.Object;
                    }
                    else
                    {
                        var objectResponse = await ReadObjectResponseAsync<Error>(response, headers, cancellationToken)
                            .ConfigureAwait(false);
                        if (objectResponse.Object == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status,
                                objectResponse.Text, headers, null);
                        }

                        throw new FireboltException<Error>("An unexpected error response", status, objectResponse.Text,
                            headers, objectResponse.Object, null);
                    }
                }
                finally
                {
                    if (disposeResponse)
                        response.Dispose();
                }
            }
            //catch (System.Exception ex)
            //{
            //    throw new FireboltException("");

            //}
            finally
            {
                if (disposeClient)
                    client.Dispose();
            }
        }

        /// <param name="engine"></param>
        /// <param name="account"></param>
        /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <summary>
        /// Returns engine URL by database name given.
        /// </summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <returns>A successful response.</returns>
        /// <exception cref="FireboltException">A server side error occurred.</exception>
        public async Task<GetEngineUrlByDatabaseNameResponse> CoreV1GetEngineUrlByDatabaseNameAsync(string? databaseName, string? account, CancellationToken cancellationToken)
        {
            var urlBuilder = new StringBuilder();

            if (account == null)
            {
                urlBuilder.Append(BaseUrl.TrimEnd('/')).Append("/core/v1/account/engines:" + "getURLByDatabaseName" + "?");
                urlBuilder.Append(Uri.EscapeDataString("database_name") + "=").Append(Uri.EscapeDataString(ConvertToString(databaseName,
                    System.Globalization.CultureInfo.InvariantCulture))).Append("&");
                urlBuilder.Length--;
            }
            else
            {
                var accountId = await GetAccountIdByNameAsync(account, cancellationToken);
                if (accountId == null) throw new FireboltException("Account id is missing");
                urlBuilder.Append(BaseUrl.TrimEnd('/'))
                    .Append("/core/v1/accounts/" + accountId.Account_id + "/engines:" + "getURLByDatabaseName" + "?");
                urlBuilder.Append(Uri.EscapeDataString("database_name") + "=").Append(Uri.EscapeDataString(ConvertToString(databaseName,
                    System.Globalization.CultureInfo.InvariantCulture))).Append("&");
                urlBuilder.Length--;
            }

            var client = new HttpClient();
            var disposeClient = true;
            try
            {
                using var request = new HttpRequestMessage();
                request.Method = new HttpMethod("GET");
                request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

                PrepareRequest();

                var url = urlBuilder.ToString();
                request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

                PrepareRequest(client);

                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                var disposeResponse = true;
                try
                {
                    var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
                    foreach (var item in response.Content.Headers)
                        headers[item.Key] = item.Value;

                    ProcessResponse();

                    var status = (int)response.StatusCode;
                    if (status == 200)
                    {
                        var objectResponse = await ReadObjectResponseAsync<GetEngineUrlByDatabaseNameResponse>(response, headers, cancellationToken).ConfigureAwait(false);

                        if (objectResponse.Object == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status, objectResponse.Text, headers, null);
                        }
                        return objectResponse.Object;
                    }
                    else
                    {
                        var objectResponse = await ReadObjectResponseAsync<Error>(response, headers, cancellationToken).ConfigureAwait(false);
                        if (objectResponse.Object == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status, objectResponse.Text, headers, null);
                        }
                        throw new FireboltException<Error>("An unexpected error response", status, objectResponse.Text, headers, objectResponse.Object, null);
                    }
                }
                finally
                {
                    if (disposeResponse)
                        response.Dispose();
                }
            }
            finally
            {
                if (disposeClient)
                    client.Dispose();
            }
        }

        public async Task<GetEngineNameByEngineIdResponse> CoreV1GetEngineUrlByEngineNameAsync(string engine, string? account, CancellationToken cancellationToken)
        {
            if (engine == null)
            {
                throw new FireboltException("Engine name is incorrect or missing");
            }
            var urlBuilder = new StringBuilder();
            var accountName = account == null ? "firebolt" : account;
            var accountId = await GetAccountIdByNameAsync(accountName, cancellationToken);
            if (accountId == null) throw new FireboltException("Account id is missing");
            urlBuilder.Append(BaseUrl.TrimEnd('/'))
                    .Append("/core/v1/accounts/" + accountId.Account_id + "/engines:" + "getIdByName" + "?");
            urlBuilder.Append(Uri.EscapeDataString("engine_name") + "=").Append(Uri.EscapeDataString(ConvertToString(engine,
                    System.Globalization.CultureInfo.InvariantCulture))).Append("&");
            urlBuilder.Length--;


            var client = new HttpClient();
            var disposeClient = true;
            try
            {
                using var request = new HttpRequestMessage();
                request.Method = new HttpMethod("GET");
                request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

                PrepareRequest();

                var url = urlBuilder.ToString();
                request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

                PrepareRequest(client);

                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                var disposeResponse = true;
                try
                {
                    var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
                    foreach (var item in response.Content.Headers)
                        headers[item.Key] = item.Value;

                    ProcessResponse();

                    var status = (int)response.StatusCode;
                    if (status == 200)
                    {
                        var objectResponse = await ReadObjectResponseAsync<GetEngineNameByEngineIdResponse>(response, headers, cancellationToken).ConfigureAwait(false);

                        if (objectResponse.Object == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status, objectResponse.Text, headers, null);
                        }
                        return objectResponse.Object;
                    }
                    else
                    {
                        var objectResponse = await ReadObjectResponseAsync<Error>(response, headers, cancellationToken).ConfigureAwait(false);
                        if (objectResponse.Object == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status, objectResponse.Text, headers, null);
                        }
                        throw new FireboltException<Error>("An unexpected error response", status, objectResponse.Text, headers, objectResponse.Object, null);
                    }
                }
                finally
                {
                    if (disposeResponse)
                        response.Dispose();
                }
            }
            finally
            {
                if (disposeClient)
                    client.Dispose();
            }
        }

        public async Task<GetEngineUrlByEngineNameResponse> CoreV1GetEngineUrlByEngineIdAsync(string engineId, string accountId, CancellationToken cancellationToken)
        {
            if (engineId == null)
            {
                throw new FireboltException("Engine name is incorrect or missing");
            }
            var urlBuilder = new StringBuilder();
            urlBuilder.Append(BaseUrl.TrimEnd('/'))
                    .Append("/core/v1/accounts/" + accountId + "/engines/" + engineId);

            var client = new HttpClient();
            var disposeClient = true;
            try
            {
                using var request = new HttpRequestMessage();
                request.Method = new HttpMethod("GET");
                request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

                PrepareRequest();

                var url = urlBuilder.ToString();
                request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

                PrepareRequest(client);

                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                var disposeResponse = true;
                try
                {
                    var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
                    foreach (var item in response.Content.Headers)
                        headers[item.Key] = item.Value;

                    ProcessResponse();

                    var status = (int)response.StatusCode;
                    if (status == 200)
                    {
                        var objectResponse = await ReadObjectResponseAsync<GetEngineUrlByEngineNameResponse>(response, headers, cancellationToken).ConfigureAwait(false);

                        if (objectResponse.Object == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status, objectResponse.Text, headers, null);
                        }
                        return objectResponse.Object;
                    }
                    else
                    {
                        var objectResponse = await ReadObjectResponseAsync<Error>(response, headers, cancellationToken).ConfigureAwait(false);
                        if (objectResponse.Object == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status, objectResponse.Text, headers, null);
                        }
                        throw new FireboltException<Error>("An unexpected error response", status, objectResponse.Text, headers, objectResponse.Object, null);
                    }
                }
                finally
                {
                    if (disposeResponse)
                        response.Dispose();
                }
            }
            finally
            {
                if (disposeClient)
                    client.Dispose();
            }
        }
        /// <param name="account"></param>
        /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <summary>
        /// Returns Account id by account name given.
        /// </summary>
        /// <returns>A successful response.</returns>
        /// <exception cref="FireboltException">A server side error occurred.</exception>
        public async Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string account, CancellationToken cancellationToken)
        {
            if (account == null)
            {
                throw new FireboltException("Account name is empty");
            }
            //Get на /iam/v2/accounts:getIdByName
            var urlBuilder = new StringBuilder();
            urlBuilder.Append(BaseUrl.TrimEnd('/')).Append("/iam/v2/accounts:getIdByName?");
            urlBuilder.Append(Uri.EscapeDataString("account_name") + "=").Append(System.Uri.EscapeDataString(ConvertToString(account, System.Globalization.CultureInfo.InvariantCulture))).Append("&");
            urlBuilder.Length--;
            var client = new HttpClient();
            var disposeClient = true;
            try
            {
                using var request = new HttpRequestMessage();
                request.Method = new HttpMethod("GET");
                request.Headers.Accept.Add(MediaTypeWithQualityHeaderValue.Parse("application/json"));

                PrepareRequest();

                var url = urlBuilder.ToString();
                request.RequestUri = new Uri(url, UriKind.RelativeOrAbsolute);

                PrepareRequest(client);

                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

                var disposeResponse = true;
                try
                {
                    var headers = response.Headers.ToDictionary(h => h.Key, h => h.Value);
                    foreach (var item in response.Content.Headers)
                        headers[item.Key] = item.Value;

                    ProcessResponse();

                    var status = (int)response.StatusCode;
                    if (status == 200)
                    {
                        var objectResponse = await ReadObjectResponseAsync<GetAccountIdByNameResponse>(response, headers, cancellationToken).ConfigureAwait(false);

                        if (objectResponse.Object == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status, objectResponse.Text, headers, null);
                        }
                        return objectResponse.Object;
                    }
                    else
                    {
                        var objectResponse = ReadObjectResponseAsync<Error>(response, headers, cancellationToken).ConfigureAwait(false);
                        throw new FireboltException("An unexpected error response");
                    }
                }
                finally
                {
                    if (disposeResponse)
                        response.Dispose();
                }
            }
            finally
            {
                if (disposeClient)
                    client.Dispose();
            }
        }

        private struct ObjectResponseResult<T>
        {
            public ObjectResponseResult(T responseObject, string responseText)
            {
                this.Object = responseObject;
                this.Text = responseText;
            }

            public T Object { get; }

            public string Text { get; }
        }

        private bool ReadResponseAsString { get; set; }
        public override UpdateRowSource UpdatedRowSource { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public override bool DesignTimeVisible { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        private async Task<ObjectResponseResult<T?>> ReadObjectResponseAsync<T>(HttpResponseMessage? response, IReadOnlyDictionary<string, IEnumerable<string>> headers, CancellationToken cancellationToken)
        {
            if (response == null)
            {
                return new ObjectResponseResult<T?>(default, string.Empty);
            }

            if (ReadResponseAsString)
            {
                var responseText = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    var typedBody = JsonConvert.DeserializeObject<T>(responseText, JsonSerializerSettings);
                    return new ObjectResponseResult<T?>(typedBody, responseText);
                }
                catch (JsonException exception)
                {
                    var message = "Could not deserialize the response body string as " + typeof(T).FullName + ".";
                    throw new FireboltException(message, (int)response.StatusCode, responseText, headers, exception);
                }
            }

            try
            {
                await using var responseStream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                using var streamReader = new StreamReader(responseStream);
                using var jsonTextReader = new JsonTextReader(streamReader);
                var serializer = JsonSerializer.Create(JsonSerializerSettings);
                var typedBody = serializer.Deserialize<T>(jsonTextReader);
                return new ObjectResponseResult<T?>(typedBody, string.Empty);
            }
            catch (JsonException exception)
            {
                var message = "Could not deserialize the response body stream as " + typeof(T).FullName + ".";
                throw new FireboltException(message, (int)response.StatusCode, string.Empty, headers, exception);
            }
        }

        private static string ConvertToString(object? value, System.Globalization.CultureInfo cultureInfo)
        {
            switch (value)
            {
                case Enum:
                    {
                        var name = System.Enum.GetName(value.GetType(), value);
                        if (name != null)
                        {
                            var field = value.GetType().GetTypeInfo().GetDeclaredField(name);
                            if (field != null)
                            {
                                if (field.GetCustomAttribute(typeof(EnumMemberAttribute)) is EnumMemberAttribute attribute)
                                {
                                    return attribute.Value ?? name;
                                }
                            }

                            var converted = Convert.ToString(Convert.ChangeType(value, Enum.GetUnderlyingType(value.GetType()), cultureInfo));
                            return converted ?? string.Empty;
                        }

                        break;
                    }
                case bool b:
                    return Convert.ToString(b, cultureInfo).ToLowerInvariant();
                case byte[] bytes:
                    return Convert.ToBase64String(bytes);
                default:
                    {
                        if (value.GetType().IsArray)
                        {
                            IEnumerable<object?> array = Enumerable.OfType<object>((Array)value);
                            return string.Join(",", array.Select(o => ConvertToString(o, cultureInfo)));
                        }

                        break;
                    }
            }

            var result = Convert.ToString(value, cultureInfo);
            return result ?? "";
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            throw new NotImplementedException();
        }

        protected override DbParameter CreateDbParameter()
        {
            return new FireboltParameter();
        }

        public override int ExecuteNonQuery()
        {
            throw new NotImplementedException();
        }

        public override object ExecuteScalar()
        {
            throw new NotImplementedException();
        }

        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        public static IEnumerable<NewMeta> FormDataForResponse(string response)
        {
            if (response == null)
            {
                throw new FireboltException("JSON data is missing");
            }
            var prettyJson = JToken.Parse(response).ToString(Formatting.Indented);
            var data = JsonConvert.DeserializeObject<QueryResult>(prettyJson);
            var newListData = new List<NewMeta>();
            try
            {
                foreach (var t in data.Data)
                {
                    for (var j = 0; j < t.Count; j++)
                    {
                        newListData.Add(new NewMeta()
                        {
                            Data = new ArrayList() { TypesConverter.ConvertToCSharpVal(t[j].ToString(), (string)TypesConverter.ConvertFireBoltMetaTypes(data.Meta[j])) },
                            Meta = (string)TypesConverter.ConvertFireBoltMetaTypes(data.Meta[j])
                        });
                    }
                }
                return newListData;
            }
            catch (System.Exception e)
            {
                throw new FireboltException(e.Message);
            }
        }
    }
}

