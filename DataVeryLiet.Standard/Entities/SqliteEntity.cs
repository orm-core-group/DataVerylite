using DataVeryLite.Core;

namespace DataVeryLite.Entities
{
    public class SqliteEntity:Entity
    {
        internal override string ProviderName
        {
            get
            {
                return DataBaseNames.Sqlite;
            }
        }
    }
}
