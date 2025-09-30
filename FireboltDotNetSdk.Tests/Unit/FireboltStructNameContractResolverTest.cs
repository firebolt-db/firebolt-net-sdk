using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FireboltDotNetSdk.Tests
{
    [TestFixture]
    internal class FireboltStructNameContractResolverTest
    {
        private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            ContractResolver = new FireboltStructNameContractResolver()
        };

        [Test]
        public void Serialize_And_Deserialize_With_Property_Attributes()
        {
            var obj = new PocoWithAttributes
            {
                IntVal = 42,
                StrVal = "hello",
                NoAttrProp = "should_be_snake_case"
            };

            var json = JsonConvert.SerializeObject(obj, Settings);
            var jo = JObject.Parse(json);

            Assert.Multiple(() =>
            {
                Assert.That(jo["int_val"]!.Value<int>(), Is.EqualTo(42));
                Assert.That(jo["str_val"]!.Value<string>(), Is.EqualTo("hello"));
                Assert.That(jo["no_attr_prop"]!.Value<string>(), Is.EqualTo("should_be_snake_case"));
            });

            var roundTrip = JsonConvert.DeserializeObject<PocoWithAttributes>(json, Settings)!;
            Assert.Multiple(() =>
            {
                Assert.That(roundTrip.IntVal, Is.EqualTo(42));
                Assert.That(roundTrip.StrVal, Is.EqualTo("hello"));
                Assert.That(roundTrip.NoAttrProp, Is.EqualTo("should_be_snake_case"));
            });
        }

        [Test]
        public void Deserialize_With_Ctor_Parameter_Attributes()
        {
            var json = "{\"int_val\":7,\"str_val\":\"abc\"}";
            var obj = JsonConvert.DeserializeObject<CtorMapped>(json, Settings)!;
            Assert.Multiple(() =>
            {
                Assert.That(obj.A, Is.EqualTo(7));
                Assert.That(obj.B, Is.EqualTo("abc"));
            });
        }

        [Test]
        public void Serialize_With_Getter_Accessor_Attribute()
        {
            var obj = new AccessorMapped("ok");
            var json = JsonConvert.SerializeObject(obj, Settings);
            var jo = JObject.Parse(json);
            Assert.That(jo["value"]!.Value<string>(), Is.EqualTo("ok"));
        }

        private class PocoWithAttributes
        {
            [FireboltStructName("int_val")] public int IntVal { get; set; }
            [FireboltStructName("str_val")] public string StrVal { get; set; } = string.Empty;
            public string NoAttrProp { get; set; } = string.Empty;
        }

        private class CtorMapped
        {
            public int A { get; }
            public string B { get; }

            public CtorMapped([FireboltStructName("int_val")] int a,
                              [FireboltStructName("str_val")] string b)
            {
                A = a;
                B = b;
            }
        }

        private class AccessorMapped
        {
            public AccessorMapped(string v)
            {
                Value = v;
            }

            [FireboltStructName("value")]
            public string Value { get; }
        }
    }
}
