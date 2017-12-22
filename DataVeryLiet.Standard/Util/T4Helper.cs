using System.Collections.Generic;

namespace DataVeryLite.Util
{
    public  class T4Helper
    {
        public  Dictionary<string, SqlHelper> SqlHelpers = new Dictionary<string, SqlHelper>();
        public  Dictionary<string, string> DataBaseNames = new Dictionary<string, string>();
        public  void Init(Dictionary<string,string> providerNames,string driversPath)
        {
            foreach (var conStr in providerNames.Keys)
            {
                SqlHelper helper = new SqlHelper(conStr, providerNames[conStr], driversPath);
                SqlHelpers.Add(conStr, helper);
                DataBaseNames.Add(conStr,helper.DataBaseName);
            }
        }
        public string ToPascalCase(string input)
        {
            return input.ToPascalCase();
        }
    }
}
