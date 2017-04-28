using System.Data.Common;
using DataVeryLite.Core;

namespace DataVeryLite.Bys
{
    public class ById : By
    {
        public object IdValue { get; set; }
        public DbTransaction Tran { get; set; }
    }
}
