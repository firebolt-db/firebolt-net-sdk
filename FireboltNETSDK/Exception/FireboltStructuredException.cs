using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;
using Newtonsoft.Json;

namespace FireboltNETSDK.Exception
{
    public class FireboltStructuredException : FireboltException
    {

        public FireboltStructuredException(List<StructuredError> errors) : base(ParseErrors(errors))
        {
        }

        private static string ParseErrors(List<StructuredError> errors)
        {
            string parsedErrors = "";
            // "{severity}: {name} ({code}) - {source}, {description}, resolution: {resolution} at {location} see {helpLink}"
            foreach (var error in errors)
            {
                var errorStart = parsedErrors.Length;
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
                    // The engine often sends only a description, which should not start with a separator.
                    parsedErrors += parsedErrors.Length > errorStart ? $", {error.Description}" : error.Description;
                }
                if (!string.IsNullOrEmpty(error.Resolution))
                {
                    parsedErrors += $", resolution: {error.Resolution}";
                }
                var readableLocation = error.Location != null ? GetReadableLocation(error.Location) : "";
                if (readableLocation.Length > 0)
                {
                    parsedErrors += $" at {readableLocation}";
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