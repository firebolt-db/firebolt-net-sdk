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
            // "{severity}: {name} ({code}) - {source}, {description}, resolution: {resolution} at {location}, see {helpLink}"
            foreach (var error in errors)
            {
                parsedErrors.Append(ParseError(error)).Append('\n');
            }
            return parsedErrors.ToString();
        }

        private static string ParseError(StructuredError error)
        {
            var segments = new List<string>();
            var header = JoinPresent(" ", error.Name, Wrap("(", error.Code, ")"), Wrap("- ", error.Source));
            AddIfPresent(segments, header);
            // The engine's description can end with a newline after its query snippet.
            AddIfPresent(segments, error.Description?.TrimEnd());
            AddIfPresent(segments, Wrap("resolution: ", error.Resolution));
            var location = error.Location != null ? GetReadableLocation(error.Location) : "";
            if (location.Length > 0)
            {
                if (segments.Count > 0)
                {
                    segments[^1] += $" at {location}";
                }
                else
                {
                    segments.Add($"at {location}");
                }
            }
            AddIfPresent(segments, Wrap("see ", error.HelpLink));

            var body = string.Join(", ", segments);
            if (string.IsNullOrEmpty(error.Severity))
            {
                return body;
            }
            return body.Length > 0 ? $"{error.Severity}: {body}" : error.Severity;
        }

        private static string GetReadableLocation(Location location)
        {
            return JoinPresent(", ",
                location.FailingLine != null ? $"FailingLine: {location.FailingLine}" : null,
                location.StartOffset != null ? $"StartOffset: {location.StartOffset}" : null,
                location.EndOffset != null ? $"EndOffset: {location.EndOffset}" : null);
        }

        private static string? Wrap(string prefix, string? value, string suffix = "")
        {
            return string.IsNullOrEmpty(value) ? null : prefix + value + suffix;
        }

        private static string JoinPresent(string separator, params string?[] parts)
        {
            return string.Join(separator, parts.Where(part => !string.IsNullOrEmpty(part)));
        }

        private static void AddIfPresent(List<string> segments, string? segment)
        {
            if (!string.IsNullOrEmpty(segment))
            {
                segments.Add(segment);
            }
        }
    }
}
