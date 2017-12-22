using System.Data.Common;
using DataVeryLite.Core;

namespace DataVeryLite.Bys
{
    public class ByTop : By
    {
        public int Top { get; set; }

        public bool Asc { get; set; }

        public DbTransaction Trans { get; set; }
    }
}