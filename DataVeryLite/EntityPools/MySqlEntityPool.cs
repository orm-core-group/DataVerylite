using DataVeryLite.Core;

namespace DataVeryLite.EntityPools
{
    public class MySqlEntityPool : EntityPool
    {
        public override string ProviderName
        {
            get
            {
                return DataBaseNames.MySql;
            }
        }
    }
}
