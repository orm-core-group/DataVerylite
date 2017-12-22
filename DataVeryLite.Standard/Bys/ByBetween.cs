using System.Data.Common;
using DataVeryLite.Core;

namespace DataVeryLite.Bys
{
    public class ByBetween : By
    {
        public int From { get; set; }

        public int To { get; set; }

        public bool Asc { get; set; }

        public DbTransaction Trans { get; set; }
    }
}
