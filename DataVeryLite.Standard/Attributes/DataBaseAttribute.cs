using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite
{
    [AttributeUsage(AttributeTargets.Class)]
    public class DataBaseAttribute : Attribute
    {
        public string Name { get; set; }
        /// <summary>
        /// connectionString's node  name
        /// </summary>
        public string Key { get; set; }
    }
}
