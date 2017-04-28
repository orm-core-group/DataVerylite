using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace DataVeryLite.Core
{
    public abstract class ConnectionStringSettingsProvider
    {
        public abstract List<ConnectionStringSettings> ToConnectionString();
    }
}