using System;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Data;
using System.Reflection;
using DataVeryLite.Core;
using DataVeryLite.Util;
using System.Collections.Generic;

namespace DataVeryLite.EntityPools
{
    public class AccessEntityPool : EntityPool
    {
        public override string ProviderName
        {
            get
            {
                return DataBaseNames.Access;
            }
        }
    }
}
