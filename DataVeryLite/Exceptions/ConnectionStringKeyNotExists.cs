using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Exceptions
{
    public class ConnectionStringKeyNotExists : Exception
    {
        public ConnectionStringKeyNotExists(string message)
            : base(message)
        {

        }
    }
}
