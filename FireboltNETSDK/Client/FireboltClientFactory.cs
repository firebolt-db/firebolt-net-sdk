using System.Data.Common;
using System.Security.Permissions;

namespace FireboltDotNetSdk.Client
{
    public sealed class FireboltClientFactory : DbProviderFactory
    {

        public static readonly FireboltClientFactory Instance = new FireboltClientFactory();

        private FireboltClientFactory()
        {
        }

        public override bool CanCreateDataSourceEnumerator
        {
            get => false;
        }

        public override DbCommand CreateCommand()
        {
            return new FireboltCommand();
        }

        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new FireboltCommandBuilder();
        }

        public override DbConnection CreateConnection()
        {
            return new FireboltConnection();
        }

        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new FireboltConnectionStringBuilder();
        }

        public override DbDataAdapter CreateDataAdapter()
        {
            return new FireboltDataAdapter();
        }

        public override DbParameter CreateParameter()
        {
            return new FireboltParameter();
        }

        // TODO: implement FireboltDataSourceEnumerator and change value of CanCreateDataSourceEnumerator to true
        // public override DbDataSourceEnumerator CreateDataSourceEnumerator() {
        //     return FireboltDataSourceEnumerator.Instance;
        // }
    }
}