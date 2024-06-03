using System.Data.Common;

namespace FireboltDotNetSdk.Client
{
    public class FireboltDataAdapter : DbDataAdapter
    {
        public FireboltDataAdapter() : base()
        {
        }

        public FireboltDataAdapter(FireboltCommand selectCommand) : this()
        {
            SelectCommand = selectCommand;
        }

        public FireboltDataAdapter(string selectCommandText, FireboltConnection selectConnection) : this(new FireboltCommand(selectConnection, selectCommandText))
        {
        }

        public FireboltDataAdapter(string selectCommandText, string selectConnectionString) : this(selectCommandText, new FireboltConnection(selectConnectionString))
        {
        }
    }
}
