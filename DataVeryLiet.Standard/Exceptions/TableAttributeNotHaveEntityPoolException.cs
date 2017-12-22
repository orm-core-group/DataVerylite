using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Exceptions
{
    public class TableAttributeNotHaveEntityPoolException : Exception
    {
        public TableAttributeNotHaveEntityPoolException(string message)
            : base(message)
        {

        }
    }
}
