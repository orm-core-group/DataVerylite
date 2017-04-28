using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Exceptions
{
    public class TableNotHavePrimaryKeyException : Exception
    {
        public TableNotHavePrimaryKeyException(string message)
            : base(message)
        {

        }
    }
}
