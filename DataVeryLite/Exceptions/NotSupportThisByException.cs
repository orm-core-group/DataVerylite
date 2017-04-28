using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Exceptions
{
    public class NotSupportThisByException : Exception
    {
        public NotSupportThisByException(string message)
            : base(message)
        {

        }
    }
}
