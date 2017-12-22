using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite
{
    [AttributeUsage(AttributeTargets.Class)]
    public class ViewAttribute : Attribute
    {
        public string Name { get; set; }
    }
}
