using DataVeryLite.Core;

namespace DataVeryLite.Bys
{
    public class ByAttach : By
    {
        public object IdValue { get; set; }
        public string[] Fields { get; set; }
    }
}
