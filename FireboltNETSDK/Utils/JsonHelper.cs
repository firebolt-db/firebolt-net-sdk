using Newtonsoft.Json;

namespace FireboltDotNetSdk.Utils
{
    public class QueryResult
    {
        [JsonProperty("query", NullValueHandling = NullValueHandling.Ignore)]
        public Query Query { get; set; } = null!;

        [JsonProperty("meta", NullValueHandling = NullValueHandling.Ignore)]
        public List<Meta> Meta { get; set; } = null!;

        [JsonProperty("data", NullValueHandling = NullValueHandling.Ignore)]
        public List<List<object?>> Data { get; set; } = null!;

        [JsonProperty("rows", NullValueHandling = NullValueHandling.Ignore)]
        public long? Rows { get; set; }

        [JsonProperty("statistics", NullValueHandling = NullValueHandling.Ignore)]
        public Statistics Statistics { get; set; } = null!;
    }

    public class Meta
    {
        [JsonProperty("name", NullValueHandling = NullValueHandling.Ignore)]
        public string Name { get; set; } = null!;

        [JsonProperty("type", NullValueHandling = NullValueHandling.Ignore)]
        public string Type { get; set; } = null!;
    }

    public class Query
    {
        [JsonProperty("query_id", NullValueHandling = NullValueHandling.Ignore)]
        public string QueryId { get; set; } = null!;
    }

    public class Statistics
    {
        [JsonProperty("elapsed", NullValueHandling = NullValueHandling.Ignore)]
        public double? Elapsed { get; set; }

        [JsonProperty("rows_read", NullValueHandling = NullValueHandling.Ignore)]
        public long? RowsRead { get; set; }

        [JsonProperty("bytes_read", NullValueHandling = NullValueHandling.Ignore)]
        public long? BytesRead { get; set; }

        [JsonProperty("time_before_execution", NullValueHandling = NullValueHandling.Ignore)]
        public double? TimeBeforeExecution { get; set; }

        [JsonProperty("time_to_execute", NullValueHandling = NullValueHandling.Ignore)]
        public double? TimeToExecute { get; set; }

        [JsonProperty("scanned_bytes_cache", NullValueHandling = NullValueHandling.Ignore)]
        public long? ScannedBytesCache { get; set; }

        [JsonProperty("scanned_bytes_storage", NullValueHandling = NullValueHandling.Ignore)]
        public long? ScannedBytesStorage { get; set; }
    }
}





