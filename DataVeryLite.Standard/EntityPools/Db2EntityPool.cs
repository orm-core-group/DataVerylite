using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataVeryLite.Core;

namespace DataVeryLite.EntityPools
{
    public class Db2EntityPool : EntityPool
    {
        public override string ProviderName
        {
            get { return DataBaseNames.Db2; }
        }
    }
}
