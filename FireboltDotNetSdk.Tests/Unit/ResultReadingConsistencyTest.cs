using System.Data.Common;
using System.Text;
using FireboltDotNetSdk.Client;
using FireboltDotNetSdk.Tests.Helpers;
using Newtonsoft.Json;

namespace FireboltDotNetSdk.Tests
{
    // Streamed and non-streamed queries must return the same values for the same server data.
    [TestFixture]
    [NonParallelizable]
    public class ResultReadingConsistencyTest
    {
        private const string IsoText = "2023-10-24T16:21:42.5+02:00";

        private static readonly (string Name, string Type)[] Columns =
        {
            ("t", "text"), ("a", "array(text)"), ("s", "struct(x text)"), ("d", "double"), ("ts", "timestamp"),
        };

        private static readonly object?[] Row =
        {
            IsoText, new[] { IsoText }, new Dictionary<string, object> { ["x"] = IsoText }, 1.5, "2023-10-24 16:21:42.5",
        };

        private static readonly string QueryResultBody = JsonConvert.SerializeObject(new
        {
            meta = Columns.Select(c => new { name = c.Name, type = c.Type }),
            data = new[] { Row },
            rows = 1,
        });

        private static readonly string StreamBody = string.Join("\n",
            JsonLinesMessages.Start(Columns), JsonLinesMessages.Data(new[] { Row }), JsonLinesMessages.Finish) + "\n";

        private static DbDataReader NonStreamed() => new FireboltDataReader(null, FireboltCommand.GetOriginalJsonData(QueryResultBody)!);

        private static DbDataReader Streamed() => new FireboltStreamingDataReader(new StreamReader(new MemoryStream(Encoding.UTF8.GetBytes(StreamBody))));

        private static IEnumerable<TestCaseData> Readers()
        {
            yield return new TestCaseData((Func<DbDataReader>)NonStreamed).SetArgDisplayNames("non-streamed");
            yield return new TestCaseData((Func<DbDataReader>)Streamed).SetArgDisplayNames("streamed");
        }

        [TestCaseSource(nameof(Readers))]
        public void TextThatLooksLikeAnIsoDateIsReturnedAsSent(Func<DbDataReader> createReader)
        {
            using var reader = createReader();
            Assert.That(reader.Read(), Is.True);
            AssertRow(reader);
        }

        [TestCaseSource(nameof(Readers))]
        public void GlobalJsonDefaultSettingsDoNotAffectValues(Func<DbDataReader> createReader)
        {
            var previous = JsonConvert.DefaultSettings;
            JsonConvert.DefaultSettings = () => new JsonSerializerSettings
            {
                DateParseHandling = DateParseHandling.DateTimeOffset,
                FloatParseHandling = FloatParseHandling.Decimal,
                MissingMemberHandling = MissingMemberHandling.Error,
            };
            try
            {
                using var reader = createReader();
                Assert.That(reader.Read(), Is.True);
                AssertRow(reader);
            }
            finally
            {
                JsonConvert.DefaultSettings = previous;
            }
        }

        private static void AssertRow(DbDataReader reader)
        {
            Assert.Multiple(() =>
            {
                Assert.That(reader.GetString(0), Is.EqualTo(IsoText));
                Assert.That(reader.GetValue(1), Is.EqualTo(new[] { IsoText }));
                Assert.That(reader.GetValue(2), Is.EqualTo(new Dictionary<string, object?> { ["x"] = IsoText }));
                Assert.That(reader.GetValue(3), Is.EqualTo(1.5).And.TypeOf<double>());
                Assert.That(reader.GetDateTime(4), Is.EqualTo(new DateTime(2023, 10, 24, 16, 21, 42, 500)));
            });
        }
    }
}
