using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Exceptions
{
    public class DriverNotFoundException : Exception
    {
         public DriverNotFoundException(string message)
            : base(message)
        {

        }
    }
}
