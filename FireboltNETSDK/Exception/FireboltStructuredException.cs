using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;

namespace FireboltNETSDK.Exception
{
    public class FireboltStructuredException : FireboltException
    {

        // public FireboltStructuredException(string message) : base(ParseErrors(message))
        // {
        // }

        public FireboltStructuredException(List<StructuredError> errors): base(ParseErrors(errors))
        {
        }

        private static string ParseErrors(List<StructuredError> errors)
        {
            string parsedErrors = "";
            // "{severity}: {name} ({code}) - {source}, {description}, resolution: {resolution} at {location} see {helpLink}"
            foreach (var error in errors)
            {
                if (!string.IsNullOrEmpty(error.Severity))
                {
                    parsedErrors += $"{error.Severity}:";
                }
                if (!string.IsNullOrEmpty(error.Name))
                {
                    parsedErrors += $" {error.Name}";
                }
                if (!string.IsNullOrEmpty(error.Code))
                {
                    parsedErrors += $" ({error.Code})";
                }
                if (!string.IsNullOrEmpty(error.Source))
                {
                    parsedErrors += $" - {error.Source}";
                }
                if (!string.IsNullOrEmpty(error.Description))
                {
                    parsedErrors += $", {error.Description}";
                }
                if (!string.IsNullOrEmpty(error.Resolution))
                {
                    parsedErrors += $", resolution: {error.Resolution}";
                }
                if (error.Location != null)
                {
                    parsedErrors += $" at {GetReadableLocation(error.Location)}";
                }
                if (!string.IsNullOrEmpty(error.HelpLink))
                {
                    parsedErrors += $", see {error.HelpLink}";
                }
                parsedErrors += "\n";
            }
            return parsedErrors;
        }
        private static string GetReadableLocation(Location location)
        {
            string readableLocation = "";
            if (location.FailingLine != null)
            {
                readableLocation += $"FailingLine: {location.FailingLine}";
            }
            if (location.StartOffset != null)
            {
                readableLocation += $", StartOffset: {location.StartOffset}";
            }
            if (location.EndOffset != null)
            {
                readableLocation += $", EndOffset: {location.EndOffset}";
            }
            return readableLocation;
        }
    }
}