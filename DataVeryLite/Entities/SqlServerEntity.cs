using System;
using System.Linq;
using System.Reflection;
using System.Data;
using DataVeryLite.Core;
using DataVeryLite.Util;

namespace DataVeryLite.Entities
{
    public class SqlServerEntity : Entity
    {
        internal override string ProviderName
        {
            get
            {
                return DataBaseNames.SqlServer;
            }
        }
    }
}
