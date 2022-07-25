﻿#region License Apache 2.0
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

namespace FireboltDotNetSdk.Client
{
    public class FireRequest
    {
        public class LoginRequest
        {
            /// <summary>
            /// Password.
            /// </summary>
            public string Password { get; set; }

            /// <summary>
            /// Username.
            /// </summary>
            public string Username { get; set; }

        }

        public class RefreshRequest
        {
            /// <summary>
            /// Refresh token.
            /// </summary>
            public string RefreshToken { get; set; }

        }
        public partial class RestartEngineRequest
        {
            /// <summary>
            /// Required. ID of the engine.
            /// </summary>
            public string EngineId { get; set; }
        }
    }
}
