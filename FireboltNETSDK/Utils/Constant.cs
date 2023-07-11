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


namespace FireboltDotNetSdk.Utils
{
    static class Constant
    {
        public static string DEFAULT_ENV = "api";
        public static string DEFAULT_ENDPOINT = "https://api.app.firebolt.io";
        public static string AUTH_SERVICE_ACCOUNT_URL = "/oauth/token";

        public static string DATABASES_URL = "/core/v1/account/databases";

        public static string ENGINES_URL = "/core/v1/account/engines";
        public static string ENGINES_BY_IDS_URL = "/core/v1/engines:getByIds";

        public static string ACCOUNT_URL = "/iam/v2/account";
        public static string ACCOUNT_BY_NAME_URL = "/web/v3/account/{0}/resolve";

        public static string ACCOUNT_ENGINE_URL = "/core/v1/accounts/{account_id}/engines/{engine_id}";
        public static string ACCOUNT_ENGINE_START_URL = ACCOUNT_ENGINE_URL + ":start";
        public static string ACCOUNT_ENGINE_RESTART_URL = ACCOUNT_ENGINE_URL + ":restart";
        public static string ACCOUNT_ENGINE_STOP_URL = ACCOUNT_ENGINE_URL + ":stop";
        public static string ACCOUNT_ENGINES_URL = "/core/v1/accounts/{account_id}/engines";
        public static string ACCOUNT_ENGINE_BY_NAME_URL = ACCOUNT_ENGINES_URL + ":getIdByName";
        public static string ACCOUNT_ENGINE_REVISION_URL = ACCOUNT_ENGINE_URL + "/engineRevisions/{revision_id}";
        public static string ACCOUNT_ENGINE_URL_BY_DATABASE_NAME = ACCOUNT_ENGINES_URL + ":getURLByDatabaseName";

        public static string ACCOUNT_DATABASES_URL = "/core/v1/accounts/{account_id}/databases";
        public static string ACCOUNT_DATABASE_URL = "/core/v1/accounts/{account_id}/databases/{database_id}";
        public static string ACCOUNT_DATABASE_BINDING_URL = ACCOUNT_DATABASE_URL + "/bindings/{engine_id}";
        public static string ACCOUNT_DATABASE_BY_NAME_URL = ACCOUNT_DATABASES_URL + ":getIdByName";

        public static string ACCOUNT_BINDINGS_URL = "/core/v1/accounts/{account_id}/bindings";

        public static string ACCOUNT_INSTANCE_TYPES_URL = "/aws/v2/accounts/{account_id}/instanceTypes";

        public static string PROVIDERS_URL = "/compute/v1/providers";
        public static string REGIONS_URL = "/compute/v1/regions";

        public static string ACCOUNT_SYSTEM_ENGINE_URL = "/web/v3/account/{0}/engineUrl";
    }
}
