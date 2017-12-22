using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Exceptions
{
    public class ConnectionStringProviderNameNotExists : Exception
    {
        public ConnectionStringProviderNameNotExists(string message)
            : base(message)
        {

        }
    }
}
