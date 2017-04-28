using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DataVeryLite.Exceptions
{
    public class DataBaseAttributeKeyNotExists : Exception
    {
         public DataBaseAttributeKeyNotExists(string message)
            : base(message)
        {

        }
    }
}
