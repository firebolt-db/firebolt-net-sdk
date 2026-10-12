using System.Text;
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
            var parsedErrors = new StringBuilder();
            // "{severity}: {name} ({code}) - {source}, {description}, resolution: {resolution} at {location} see {helpLink}"
            foreach (var error in errors)
            {
                var errorStart = parsedErrors.Length;
                if (!string.IsNullOrEmpty(error.Severity))
                {
                    parsedErrors.Append($"{error.Severity}:");
                }
                if (!string.IsNullOrEmpty(error.Name))
                {
                    parsedErrors.Append($" {error.Name}");
                }
                if (!string.IsNullOrEmpty(error.Code))
                {
                    parsedErrors.Append($" ({error.Code})");
                }
                if (!string.IsNullOrEmpty(error.Source))
                {
                    parsedErrors.Append($" - {error.Source}");
                }
                if (!string.IsNullOrEmpty(error.Description))
                {
                    // The engine often sends only a description, which should not start with a separator.
                    parsedErrors.Append(parsedErrors.Length > errorStart ? $", {error.Description}" : error.Description);
                }
                if (!string.IsNullOrEmpty(error.Resolution))
                {
                    parsedErrors.Append($", resolution: {error.Resolution}");
                }
                var readableLocation = error.Location != null ? GetReadableLocation(error.Location) : "";
                if (readableLocation.Length > 0)
                {
                    parsedErrors.Append($" at {readableLocation}");
                }
                if (!string.IsNullOrEmpty(error.HelpLink))
                {
                    parsedErrors.Append($", see {error.HelpLink}");
                }
                parsedErrors.Append('\n');
            }
            return parsedErrors.ToString();
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