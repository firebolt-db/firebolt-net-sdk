using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace FireboltDotNetSdk.Utils
{
    public sealed class FireboltStructNameContractResolver : DefaultContractResolver
    {
        public FireboltStructNameContractResolver()
        {
            // Optional: fallback for props *without* [FromName]
            NamingStrategy = new SnakeCaseNamingStrategy();
        }

        /// <summary>
        /// Override JSON.NET's property creation.
        /// If a member has a [FireboltStructName("...")] mapping, use that name as the JSON property name instead of the default.
        /// </summary>
        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var prop = base.CreateProperty(member, memberSerialization);

            var map = GetFromName(member);
            if (map is { Name: { Length: > 0 } name })
                prop.PropertyName = name;

            return prop;
        }

        /// <summary>
        /// Override JSON.NET's default constructor parameter mapping.
        /// If a parameter has [FireboltStructName("...")], use that value as the JSON property name instead of the parameter's name.
        /// </summary>
        protected override IList<JsonProperty> CreateConstructorParameters(
            ConstructorInfo constructor, JsonPropertyCollection memberProperties)
        {
            var parameters = base.CreateConstructorParameters(constructor, memberProperties);

            foreach (var pinfo in constructor.GetParameters())
            {
                var map = pinfo.GetCustomAttribute<FireboltStructNameAttribute>(true);
                if (map is { Name: { Length: > 0 } name })
                {
                    var jp = parameters.FirstOrDefault(p => p.PropertyName == pinfo.Name);
                    if (jp != null) jp.PropertyName = name;
                }
            }

            return parameters;
        }

        private static FireboltStructNameAttribute? GetFromName(MemberInfo member)
        {
            // Attribute on property/field
            if (member.GetCustomAttribute<FireboltStructNameAttribute>(true) is { } a) return a;

            // …or on accessors
            if (member is PropertyInfo pi)
            {
                return pi.GetGetMethod(true)?.GetCustomAttribute<FireboltStructNameAttribute>(true)
                       ?? pi.GetSetMethod(true)?.GetCustomAttribute<FireboltStructNameAttribute>(true);
            }
            return null;
        }
    }
}


