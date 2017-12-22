using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Exceptions
{
    public class ClassNotHaveTableAttributeException : Exception
    {
        public ClassNotHaveTableAttributeException(string message)
            : base(message)
        {

        }
    }
}
