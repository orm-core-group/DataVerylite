using DataVeryLite.Core;

namespace DataVeryLite.EntityPools
{
    public class SqliteEntityPool : EntityPool
    {
        public override string ProviderName
        {
            get { return DataBaseNames.Sqlite; }
        }
    }
}