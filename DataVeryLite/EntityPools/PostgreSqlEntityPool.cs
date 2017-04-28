using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataVeryLite.Core;

namespace DataVeryLite.EntityPools
{
    public class PostgreSqlEntityPool : EntityPool
    {
        public override string ProviderName
        {
            get { return DataBaseNames.PostgreSql; }
        }
    }
}
