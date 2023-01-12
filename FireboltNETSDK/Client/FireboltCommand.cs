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
using System.Net.Mime;
using System.Text.RegularExpressions;
using FireboltDoNetSdk.Utils;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FireboltDotNetSdk.Client
{
    /// <summary>
    /// Represents an SQL statement to execute against a FireBolt database. This class cannot be inherited.
    /// </summary>
    public sealed class FireboltCommand : DbCommand
    {
        private string? _commandText;

        public string? Response { get; set; }

        public readonly HashSet<string> SetParamList = new();

        public FireboltCommand()
        { }

        /// <summary>
        ///Gets or sets the SQL statement to execute at the data source.
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
        private new FireboltConnection? Connection { get; }

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
        public new FireboltParameterCollection Parameters { get; } = new();

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

                    if (Connection != null)
                    {
                        Response = Connection.Client
                            .ExecuteQuery(engineUrl, Connection.Database, newCommandText)
                            .GetAwaiter().GetResult();
                        //return FormDataForResponse(Response); 
                    }
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
                    string pattern = string.Format(@"\{0}\b", item.ParameterName);
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
                            verifyParameters = "1";
                        else
                            verifyParameters = "0";
                    }
                    else if (item.Value is IList && item.Value.GetType().IsGenericType)
                    {
                        throw new FireboltException("Array query parameters are not supported yet.");
                    }
                    else if (item.Value is int || item.Value is long || item.Value is double || item.Value is float ||
                             item.Value is decimal)
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
            var prettyJson = JToken.Parse(Response ?? throw new FireboltException("RowCount is missing"))
                .ToString(Formatting.Indented);
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

        public override UpdateRowSource UpdatedRowSource { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public override bool DesignTimeVisible { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

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
                    for (var j = 0; j < t.Count; j++)
                        newListData.Add(new NewMeta
                        {
                            Data = new ArrayList
                            {
                                TypesConverter.ConvertToCSharpVal(t[j].ToString(),
                                    (string)TypesConverter.ConvertFireBoltMetaTypes(data.Meta[j]))
                            },
                            Meta = (string)TypesConverter.ConvertFireBoltMetaTypes(data.Meta[j])
                        });
                return newListData;
            }
            catch (System.Exception e)
            {
                throw new FireboltException(e.Message);
            }
        }
    }
}