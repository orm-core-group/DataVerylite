using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Exceptions
{
    public class ClassNotHaveDataBaseAttributeException : Exception
    {
        public ClassNotHaveDataBaseAttributeException(string message)
            : base(message)
        {

        }
    }
}
