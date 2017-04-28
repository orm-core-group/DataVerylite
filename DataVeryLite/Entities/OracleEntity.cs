using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using DataVeryLite.Core;
using DataVeryLite.Util;

namespace DataVeryLite.Entities
{
    public class OracleEntity : Entity
    {
        internal override string ProviderName
        {
            get { return DataBaseNames.Oracle; }
        }
    }
}