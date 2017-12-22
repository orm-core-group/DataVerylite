using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite
{
    /// <summary>
    /// Infomation of Column at Db
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute :Attribute
    {
        public string Name { get; set; }

        public bool IsPrimaryKey { get; set; }

        public int Length { get; set; }

        public string Type { get; set; }

        public bool IsAutoGrow { get; set; }

        public bool IsNullAble = true;
    }
}