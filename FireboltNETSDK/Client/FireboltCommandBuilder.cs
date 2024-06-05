using System.Data.Common;
using System.Data;

namespace FireboltDotNetSdk.Client
{
    public sealed class FireboltCommandBuilder : DbCommandBuilder
    {

        override protected void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
        {
            FireboltParameter fbp = (FireboltParameter)parameter;

            if (row.Table.Columns.Contains("ParameterName"))
            {
                fbp.ParameterName = (string)row["ParameterName"];
            }

            if (row.Table.Columns.Contains(SchemaTableColumn.ProviderType))
            {
                fbp.DbType = (DbType)row[SchemaTableColumn.ProviderType];
            }
        }

        override protected string GetParameterName(int parameterOrdinal)
        {
            return "@p" + parameterOrdinal.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        override protected string GetParameterName(string parameterName)
        {
            return "@" + parameterName;
        }

        override protected string GetParameterPlaceholder(int parameterOrdinal)
        {
            return "@p" + parameterOrdinal.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        override protected void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            throw new NotSupportedException();
        }
    }
}