using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataVeryLite.Core;

namespace DataVeryLite.Entities
{
    public class Db2Entity:Entity
    {
        internal override string ProviderName
        {
            get { return DataBaseNames.Db2; }
        }
    }
}
