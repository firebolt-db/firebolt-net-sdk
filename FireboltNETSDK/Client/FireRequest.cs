#region License Apache 2.0
/* Copyright 2022 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion

using Newtonsoft.Json;

namespace FireboltDotNetSdk.Client
{
    public class FireRequest
    {
        public class UsernamePasswordLoginRequest
        {
            public UsernamePasswordLoginRequest(string username, string password)
            {
                Password = password;
                Username = username;
            }

            /// <summary>
            /// Password.
            /// </summary>
            [JsonProperty]
            private string Password { get; }

            /// <summary>
            /// Username.
            /// </summary>
            [JsonProperty]
            private string Username { get; }

        }

        public class ServiceAccountLoginRequest
        {
            private const string Audience = "https://api.firebolt.io";
            private const string GrantType = "client_credentials";

            public ServiceAccountLoginRequest(string clientId, string clientSecret)
            {
                ClientId = clientId;
                ClientSecret = clientSecret;
            }

            /// <summary>
            /// Transforms object to a FormUrlEncodedContent object.
            /// </summary>
            public FormUrlEncodedContent GetFormUrlEncodedContent()
            {
                var values = new Dictionary<string, string>
                {
                    { "client_id", ClientId },
                    { "client_secret", ClientSecret },
                    { "grant_type", GrantType },
                    { "audience",  Audience}
                };
                return new FormUrlEncodedContent(values);
            }

            /// <summary>
            /// ClientId.
            /// </summary>
            private string ClientId { get; }

            /// <summary>
            /// ClientSecret.
            /// </summary>
            private string ClientSecret { get; }
        }
    }
}
