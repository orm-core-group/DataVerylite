using DataVeryLite.Core;

namespace DataVeryLite.Entities
{
    public class MySqlEntity : Entity
    {
        internal override string ProviderName
        {
            get { return DataBaseNames.MySql; }
        }
    }
}
