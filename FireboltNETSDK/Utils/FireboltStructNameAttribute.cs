namespace FireboltDotNetSdk.Utils
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter | AttributeTargets.Method)]
    public sealed class FireboltStructNameAttribute : Attribute
    {
        public string Name { get; }
        public FireboltStructNameAttribute(string name) => Name = name;
    }

}


