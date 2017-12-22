using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DataVeryLite.Aop;

namespace DataVeryLite
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute:Attribute
    {
        public string Name { get; set; }

        public Type EntityPool { get; set; }

        public string Key { get; set; }

        public bool EnableSync = false;
    }
}
