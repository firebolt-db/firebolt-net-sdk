﻿#region License Apache 2.0
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
using System.Net.Http.Headers;
using System.Net.Mime;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
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

        private string? Response { get; set; }

        private static readonly HashSet<string> SetParamList = new();

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

        /// <inheritdoc cref="Connection"/>    
        protected override DbConnection? DbConnection
        {
            get => Connection;
            set => Connection = (FireboltConnection?)value;
        }

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

        protected override DbParameterCollection DbParameterCollection => throw new NotImplementedException();

        public override int CommandTimeout { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        internal FireboltCommand(FireboltConnection connection) => Connection = connection ?? throw new ArgumentNullException(nameof(connection));

        public IEnumerable<NewMeta> Execute(string commandText)
        {
            if (commandText.StartsWith("SET"))
            {
                commandText = commandText.Remove(0, 4).Trim();
                SetParamList.Add(commandText);
                return new List<NewMeta>();
            }
            else
            {
                try
                {
                    Response = Connection?.Client.ExecuteQuery(Connection.Engine.Engine_url, Connection.Database, commandText).GetAwaiter().GetResult();
                    return FormDataForResponse(Response);
                }
                catch (System.Exception ex)
                {
                    throw new FireboltException(ex.Message);
                }
            }
        }

        /// <summary>
        /// Gets original data in JSON format for further manipulation<b>null</b>.
        /// </summary>
        /// <returns><b>null</b></returns>
        public Welcome? GetOriginalJSONData()
        {
            if (Response != null && !Response.Any()) return null;
            var prettyJson = JToken.Parse(Response).ToString(Formatting.Indented);
            return JsonConvert.DeserializeObject<Welcome>(prettyJson);
        }

        /// <summary>
        /// Gets rowscount parameter from return data<b>null</b>.
        /// </summary>
        /// <returns><b>null</b></returns>
        public int RowCount()
        {
            var prettyJson = JToken.Parse(Response ?? throw new FireboltException("Data is missing")).ToString(Formatting.Indented);
            var data = JsonConvert.DeserializeObject<Welcome>(prettyJson);
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

        private void PrepareRequest(HttpClient client)
        {

            //Added to avoid 403 Forbidden error
            client.DefaultRequestHeaders.Add("User-Agent", "Other");

            if (!string.IsNullOrEmpty(Token))
            {
                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + Token);
            }
        }

        public string? Token { get; set; }
        public string RefreshToken { get; set; }
        public string TokenExpired { get; set; }

        /// <summary>
        /// Executes a SQL query
        /// </summary>
        /// <param name="engineEndpoint">Engine endpoint (URL)</param>
        /// <param name="databaseName">Database name</param>
        /// <param name="query">SQL query to execute</param>
        /// <returns>A successful response.</returns>
        /// <exception cref="FireboltException">A server side error occurred.</exception>
        public Task<string?> ExecuteQueryAsync(string? engineEndpoint, string databaseName, string query)
        {
            return ExecuteQueryAsync(engineEndpoint, databaseName, query, CancellationToken.None);
        }

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
                throw new ArgumentNullException(nameof(query));

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
                        var objectResponse = await ReadObjectResponseAsync<Error>(response, headers, cancellationToken).ConfigureAwait(false);
                        if (objectResponse.Object == null)
                        {
                            throw new FireboltException("Response was null which was not expected.", status, objectResponse.Text, headers, innerException: null);
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

        private static JsonSerializerSettings CreateSerializerSettings()
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
                        var objectResponse = await ReadObjectResponseAsync<LoginResponse>(response, headers, cancellationToken).ConfigureAwait(false);
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

        /// <summary>
        /// Returns engine URL by database name given.
        /// </summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="engine"></param>
        /// <param name="account"></param>
        /// <returns>A successful response.</returns>
        /// <exception cref="FireboltException">A server side error occurred.</exception>
        public Task<GetEngineUrlByDatabaseNameResponse> CoreV1GetEngineUrlByDatabaseNameAsync(string? databaseName, string? engine, string? account)
        {
            return CoreV1GetEngineUrlByDatabaseNameAsync(databaseName, engine,account, CancellationToken.None);
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
        public async Task<GetEngineUrlByDatabaseNameResponse> CoreV1GetEngineUrlByDatabaseNameAsync(string? databaseName,string? engine, string? account ,CancellationToken cancellationToken)
        {
            var urlBuilder = new StringBuilder();
            var correctUrl = engine == null ? "getURLByDatabaseName" : "getIdByName";
            var correctParam = engine == null ? "database_name" : "engine_name";
            if (account == null)
            {
                urlBuilder.Append(BaseUrl.TrimEnd('/')).Append("/core/v1/account/engines:" + correctUrl + "?");
                urlBuilder.Append(Uri.EscapeDataString(correctParam) + "=").Append(System.Uri.EscapeDataString(ConvertToString(databaseName, System.Globalization.CultureInfo.InvariantCulture))).Append("&");
                urlBuilder.Length--;
            }
            else
            {
                var accountId = await GetAccountIdByNameAsync(account, cancellationToken);
                if (accountId == null) throw new FireboltException("Account id is missing");
                urlBuilder.Append(BaseUrl.TrimEnd('/'))
                    .Append("/core/v1/accounts/" + accountId.Account_id + "/engines:" + correctUrl + "?");
                urlBuilder.Append(Uri.EscapeDataString(correctParam) + "=").Append(System.Uri.EscapeDataString(ConvertToString(databaseName, System.Globalization.CultureInfo.InvariantCulture))).Append("&");
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

        /// <param name="account"></param>
        /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
        /// <summary>
        /// Returns Account id by account name given.
        /// </summary>
        /// <returns>A successful response.</returns>
        /// <exception cref="FireboltException">A server side error occurred.</exception>
        private async Task<GetAccountIdByNameResponse> GetAccountIdByNameAsync(string? account, CancellationToken cancellationToken)
        {
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


        /// <inheritdoc/>
        public override ValueTask DisposeAsync()
        {
            return base.DisposeAsync();
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            throw new NotImplementedException();
        }

        protected override DbParameter CreateDbParameter()
        {
            throw new NotImplementedException();
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

        private static IEnumerable<NewMeta> FormDataForResponse(string? response)
        {
            var prettyJson = JToken.Parse(response).ToString(Formatting.Indented);
            var data = JsonConvert.DeserializeObject<Welcome>(prettyJson);
            var newListData = new List<NewMeta>();
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
    }
}

