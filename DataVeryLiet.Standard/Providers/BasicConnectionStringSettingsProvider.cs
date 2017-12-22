using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using DataVeryLite.Core;

namespace DataVeryLite.Providers
{
    internal class BasicConnectionStringSettingsProvider : ConnectionStringSettingsProvider
    {
        public override List<ConnectionStringSettings> ToConnectionString()
        {
            return ConfigurationManager.ConnectionStrings.Cast<ConnectionStringSettings>().ToList();
        }
    }
}
