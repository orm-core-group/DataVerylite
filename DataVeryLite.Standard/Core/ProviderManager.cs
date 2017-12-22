using System;
using System.Collections.Generic;
using System.Reflection;

namespace DataVeryLite.Core
{
    public class ProviderManager
    {
        internal static readonly Dictionary<string, IProvider> Providers;
        internal static readonly Dictionary<string, string> Entities;
        internal static readonly Dictionary<string, string> EntityPools;

        static ProviderManager()
        {
            Providers = new Dictionary<string, IProvider>();
            Entities = new Dictionary<string, string>();
            EntityPools = new Dictionary<string, string>();

            Entities.Add(DataBaseNames.Access, "DataVeryLite.Entities.AccessEntity");
            Entities.Add(DataBaseNames.Db2, "DataVeryLite.Entities.Db2Entity");
            Entities.Add(DataBaseNames.MySql, "DataVeryLite.Entities.MySqlEntity");
            Entities.Add(DataBaseNames.Oracle, "DataVeryLite.Entities.OracleEntity");
            Entities.Add(DataBaseNames.PostgreSql, "DataVeryLite.Entities.PostgreSqlEntity");
            Entities.Add(DataBaseNames.SqlServer, "DataVeryLite.Entities.SqlServerEntity");
            Entities.Add(DataBaseNames.Sqlite, "DataVeryLite.Entities.SqliteEntity");

            EntityPools.Add(DataBaseNames.Access, "DataVeryLite.EntityPools.AccessEntityPool");
            EntityPools.Add(DataBaseNames.Db2, "DataVeryLite.EntityPools.Db2EntityPool");
            EntityPools.Add(DataBaseNames.MySql, "DataVeryLite.EntityPools.MySqlEntityPool");
            EntityPools.Add(DataBaseNames.Oracle, "DataVeryLite.EntityPools.OracleEntityPool");
            EntityPools.Add(DataBaseNames.PostgreSql, "DataVeryLite.EntityPools.PostgreSqlEntityPool");
            EntityPools.Add(DataBaseNames.SqlServer, "DataVeryLite.EntityPools.SqlServerEntityPool");
            EntityPools.Add(DataBaseNames.Sqlite, "DataVeryLite.EntityPools.SqliteEntityPool");

            Assembly ass = typeof(IProvider).Assembly;
            Type[] types = ass.GetTypes();
            foreach (var type in types)
            {
                Type[] interfaceTypes = type.GetInterfaces();
                foreach (var interfaceType in interfaceTypes)
                {
                    if (interfaceType == typeof (IProvider))
                    {
                        var provider = (IProvider) Activator.CreateInstance(type);
                        Providers.Add(provider.ProviderName, provider);
                    }
                }
            }
        }

        public static IProvider GetProvider(string providerName)
        {
            return Providers[providerName];
        }
        public static string GetEntity(string providerName)
        {
            return Entities[providerName];
        }
        public static string GetEntityPool(string providerName)
        {
            return EntityPools[providerName];
        }
    }
}
