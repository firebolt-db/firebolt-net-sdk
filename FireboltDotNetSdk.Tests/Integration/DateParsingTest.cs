using System.Data.Common;
using FireboltDotNetSdk.Client;
using Newtonsoft.Json;

namespace FireboltDotNetSdk.Tests
{
    // Only the column type decides whether a value is a date: text that looks like an ISO date stays text, and real
    // date and timestamp columns are parsed the same way whether the result is streamed or not.
    [TestFixture]
    internal class DateParsingTest : IntegrationTest
    {
        private const string IsoText = "2023-10-24T16:21:42.5+02:00";
        private const string UtcIsoText = "2023-10-24T16:21:42Z";

        private const string Query = "SELECT " +
            $"'{IsoText}'::text AS iso_text, " +
            "'2023-10-24'::text AS date_text, " +
            $"['{IsoText}', '{UtcIsoText}']::array(text) AS iso_text_array, " +
            "'2023-10-24'::date AS d, " +
            "'2023-10-24 16:21:42.123456'::timestamp AS ts, " +
            "'2023-10-24 16:21:42.123456+00'::timestamptz AS tstz, " +
            "['2023-10-24 16:21:42.5', '2023-10-25 00:00:00']::array(timestamp) AS ts_array";

        public enum ReadMode { NonStreamed, Streamed }

        [TestCase(ReadMode.NonStreamed)]
        [TestCase(ReadMode.Streamed)]
        [Category("engine-v2")]
        public void ColumnTypeDecidesHowDatesAreRead(ReadMode mode)
        {
            AssertRow(mode);
        }

        // A host application's global Json.NET settings must not change what the SDK returns.
        [TestCase(ReadMode.NonStreamed)]
        [TestCase(ReadMode.Streamed)]
        [Category("engine-v2")]
        [NonParallelizable]
        public void GlobalJsonDefaultSettingsDoNotChangeDates(ReadMode mode)
        {
            var previous = JsonConvert.DefaultSettings;
            JsonConvert.DefaultSettings = () => new JsonSerializerSettings
            {
                DateParseHandling = DateParseHandling.DateTimeOffset,
                FloatParseHandling = FloatParseHandling.Decimal,
            };
            try
            {
                AssertRow(mode);
            }
            finally
            {
                JsonConvert.DefaultSettings = previous;
            }
        }

        private static void AssertRow(ReadMode mode)
        {
            using var connection = new FireboltConnection(ConnectionString());
            connection.Open();
            var command = (FireboltCommand)connection.CreateCommand();
            command.CommandText = Query;
            using DbDataReader reader = mode == ReadMode.Streamed ? command.ExecuteStreamedQuery() : command.ExecuteReader();

            Assert.That(reader.Read(), Is.True);
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetString(0), Is.EqualTo(IsoText));
                Assert.That(reader.GetValue(0), Is.EqualTo(IsoText));
                Assert.That(reader.GetString(1), Is.EqualTo("2023-10-24"));
                Assert.That(reader.GetValue(2), Is.EqualTo(new[] { IsoText, UtcIsoText }));
                Assert.That(reader.GetDateTime(3), Is.EqualTo(new DateTime(2023, 10, 24)));
                Assert.That(reader.GetDateTime(4), Is.EqualTo(new DateTime(2023, 10, 24, 16, 21, 42).AddTicks(1234560)));
                Assert.That(reader.GetDateTime(5), Is.EqualTo(DateTime.Parse("2023-10-24 16:21:42.123456Z")));
                Assert.That(reader.GetValue(6), Is.EqualTo(new[] { new DateTime(2023, 10, 24, 16, 21, 42, 500), new DateTime(2023, 10, 25) }));
            });
            Assert.That(reader.Read(), Is.False);
        }
    }
}
