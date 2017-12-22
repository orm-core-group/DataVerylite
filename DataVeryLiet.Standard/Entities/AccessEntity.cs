using System;
using System.Linq;
using System.Data;
using System.Reflection;
using System.Data.Common;
using DataVeryLite.Bys;
using DataVeryLite.Core;
using DataVeryLite.Util;

namespace DataVeryLite.Entities
{
    public class AccessEntity : Entity
    {
        internal override string ProviderName
        {
            get
            {
                return DataBaseNames.Access;
            }
        }
    }
}
