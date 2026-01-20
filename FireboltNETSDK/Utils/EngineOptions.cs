namespace FireboltDotNetSdk.Utils
{
    /// <summary>
    /// Represents cached engine options including the engine URL and parameters that were set 
    /// as side effects of executing the USE ENGINE query.
    /// </summary>
    public class EngineOptions
    {
        public string EngineUrl { get; }
        public IList<KeyValuePair<string, string>> Parameters { get; }

        public EngineOptions(string engineUrl, IList<KeyValuePair<string, string>> parameters)
        {
            EngineUrl = engineUrl ?? throw new ArgumentNullException(nameof(engineUrl));
            Parameters = parameters;
        }
    }
}
