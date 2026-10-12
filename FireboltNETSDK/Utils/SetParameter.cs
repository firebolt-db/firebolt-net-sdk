namespace FireboltDotNetSdk.Utils
{
    internal static class SetParameter
    {
        /// <summary>
        /// The name in a "name=value" parameter as written after SET; spaces around it are allowed.
        /// </summary>
        internal static string NameOf(string parameter)
        {
            return parameter.Split('=', 2)[0].Trim();
        }
    }
}
