using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Exceptions
{
    public class SqlHelperCanNotExists : Exception
    {
        public SqlHelperCanNotExists(string message)
            : base(message)
        {

        }
    }
}