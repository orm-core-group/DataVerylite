using System.Data.Common;
using DataVeryLite.Core;

namespace DataVeryLite.Bys
{
    public class ByPage : By
    {
        public int Page { get; set; }

        public int PageSize { get; set; }

        public bool Asc { get; set; }

        public DbTransaction Trans { get; set; }
    }
}
