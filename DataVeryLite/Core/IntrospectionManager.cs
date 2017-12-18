using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using DataVeryLite.Exceptions;
using DataVeryLite.Util;

namespace DataVeryLite.Core
{
    /// <summary>
    /// self-controller
    /// </summary>
    internal static class IntrospectionManager
    {
        private static readonly Dictionary<string, DataBaseAttribute> DataBases = new Dictionary<string, DataBaseAttribute>();

        private static readonly Dictionary<string, TableAttribute> Tables = new Dictionary<string, TableAttribute>();

        private static readonly Dictionary<string, string> TableNames = new Dictionary<string, string>();

        private static readonly Dictionary<string, Dictionary<string, ColumnAttribute>> Columns = new Dictionary<string, Dictionary<string, ColumnAttribute>>();

        private static readonly Dictionary<string, Dictionary<string, string>> ColumnNames = new Dictionary<string, Dictionary<string, string>>();

        private static readonly Dictionary<string, Dictionary<string, Type>> ColumnTypes = new Dictionary<string, Dictionary<string, Type>>();

        private static readonly Dictionary<string, Dictionary<string, Type>> PrimaryKeys = new Dictionary<string, Dictionary<string, Type>>();

        private static readonly Dictionary<string, ConnectionStringSettings> ConnectionStrings = new Dictionary<string, ConnectionStringSettings>();

        private static readonly Dictionary<string, Func<object, string, object>> GetDelegates = new Dictionary<string, Func<object, string, object>>();

        private static readonly Dictionary<string, Action<object, string, object>> SetDelegates = new Dictionary<string, Action<object, string, object>>();

        private static readonly Dictionary<string, Assembly> Drivers = new Dictionary<string, Assembly>();

        private static readonly Dictionary<string, SqlHelper> SqlHelpers = new Dictionary<string, SqlHelper>();

        static IntrospectionManager()
        {
            var filters = ConfigurationManager.AppSettings["AssemblyFilter"]??"";
            var rejects = ConfigurationManager.AppSettings["AssemblyReject"]??"";
            var filterArr = filters.Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries).ToList();
            var rejectArr = rejects.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            PreLoad();
            IEnumerable<Assembly> assemblies;
            if (filterArr.Any() && rejectArr.Any())
            {
                assemblies = AppDomain.CurrentDomain.GetAssemblies().Where(x =>
                { return filterArr.Any(filter => x.FullName.Contains(filter)) && !rejectArr.Any(reject => x.FullName.Contains(reject)); });
            }
            else if (rejectArr.Any())
            {
                assemblies = AppDomain.CurrentDomain.GetAssemblies().Where(x =>
                { return !rejectArr.Any(reject => x.FullName.Contains(reject)); });
            }
            else if (filterArr.Any())
            {
                assemblies = AppDomain.CurrentDomain.GetAssemblies().Where(x =>
                    { return filterArr.Any(filter => x.FullName.Contains(filter)); });
            }
            else
            {
                assemblies = AppDomain.CurrentDomain.GetAssemblies();

            }
            foreach (var assembly in assemblies)
            {
                try
                {
                    assembly.GetType();
                    assembly.GetTypes();
                }
                catch (Exception ex)
                {
                    LogHelper.LogWarning(ex.Message);
                    continue;
                }

                var types = assembly.GetTypes();
                foreach (var type in types)
                {
                    try
                    {
                        type.GetType();
                        type.GetCustomAttributes(false).Any(x => x is DataBaseAttribute);
                        type.GetCustomAttributes(false).Any(x => x is TableAttribute);
                    }
                    catch (Exception ex)
                    {
                        LogHelper.LogWarning(ex.Message);
                        continue;
                    }
                    //db
                    if (type.GetCustomAttributes(false).Any(x => x is DataBaseAttribute))
                    {
                        DataBases.Add(type.FullName, (DataBaseAttribute)type.GetCustomAttributes(false).Where(x => x is DataBaseAttribute).FirstOrDefault());
                    }
                    //table
                    else if (type.GetCustomAttributes(false).Any(x => x is TableAttribute))
                    {
                        GetDelegates.Add(type.FullName, GenerateGetValue(type));
                        SetDelegates.Add(type.FullName, GenerateSetValue(type));
                        var ta =(TableAttribute)type.GetCustomAttributes(false).Where(x => x is TableAttribute).FirstOrDefault();
                        Tables.Add(type.FullName,ta);
                        TableNames.Add(type.FullName, string.IsNullOrEmpty(ta.Name) ? type.Name : ta.Name);
                        //attri
                        var pis = new Dictionary<string, ColumnAttribute>();
                        Columns.Add(type.FullName,pis);
                        //column
                        var names = new Dictionary<string, string>();
                        var memberTypes = new Dictionary<string, Type>();
                        ColumnNames.Add(type.FullName, names);
                        ColumnTypes.Add(type.FullName, memberTypes);
                        //pk
                        var pk = new Dictionary<string, Type>();
                        PrimaryKeys.Add(type.FullName, pk);

                        var columns =type.GetProperties()
                                                    .Where(
                                                        x =>
                                                        x.CanRead && x.CanWrite &&
                                                        x.GetCustomAttributes(false).Any(y => y is ColumnAttribute));
                        foreach (var propertyInfo in columns)
                        {
                            var ca = (ColumnAttribute)propertyInfo.GetCustomAttributes(false).Where(x => x is ColumnAttribute).FirstOrDefault();
                            //attri
                            pis.Add(propertyInfo.Name,ca );
                            //column
                            names.Add(propertyInfo.Name, string.IsNullOrEmpty(ca.Name) ? propertyInfo.Name : ca.Name);
                            memberTypes.Add(propertyInfo.Name, propertyInfo.PropertyType);
                            //pk
                            if (ca.IsPrimaryKey)
                            {
                                pk.Add(propertyInfo.Name , propertyInfo.PropertyType);
                            }
                        }
                    }
                    else if (type.BaseType == typeof (ConnectionStringSettingsProvider))
                    {
                        var config = Activator.CreateInstance(type) as ConnectionStringSettingsProvider;
                        if (config != null)
                        {
                            var conns = config.ToConnectionString();
                            foreach (var concetionStr in conns)
                            {
                                if (!string.IsNullOrEmpty(concetionStr.ProviderName) &&
                                    ProviderManager.Providers.Keys.Contains(concetionStr.ProviderName))
                                {
                                    //conn
                                    ConnectionStrings.Add(concetionStr.Name, concetionStr);
                                    //sqlhelper
                                    SqlHelpers.Add(concetionStr.Name, new SqlHelper(concetionStr.Name));
                                }
                            }
                        }
                    }
                }
            }
            SyncDbInfo();
        }

        private static void PreLoad()
        {
            var files = new List<string>();
            var p = AppDomain.CurrentDomain.BaseDirectory;
            //files.AddRange(Directory.GetFiles(p, "db2app64.dll", SearchOption.TopDirectoryOnly).ToList());
            files.AddRange(Directory.GetFiles(p, "IBM.Data.DB2.dll", SearchOption.TopDirectoryOnly).ToList());
            files.AddRange(Directory.GetFiles(p, "MySql.Data.dll", SearchOption.TopDirectoryOnly).ToList());
            files.AddRange(Directory.GetFiles(p, "Npgsql.dll", SearchOption.TopDirectoryOnly).ToList());
            files.AddRange(Directory.GetFiles(p, "Mono.Security.dll", SearchOption.TopDirectoryOnly).ToList());
            files.AddRange(Directory.GetFiles(p, "System.Data.SQLite.dll", SearchOption.TopDirectoryOnly).ToList());
            files.AddRange(Directory.GetFiles(p, "Oracle.ManagedDataAccess.dll", SearchOption.TopDirectoryOnly).ToList());
            foreach (var s in files)
            {
                AssemblyName a = null;
                try
                {
                    a = AssemblyName.GetAssemblyName(s);
                }
                catch
                {
                    string info = string.Format("AppDomain can't load file {0}", s);
                    LogHelper.LogWarning(info);
                    continue;
                }

                if (!AppDomain.CurrentDomain.GetAssemblies().Any(
                    assembly
                    =>
                    AssemblyName.ReferenceMatchesDefinition(assembly.GetName(), a))
                    )
                {
                    try
                    {
                        Assembly assembly;
                        if (Path.GetFileName(s) == "System.Data.SQLite.dll")
                        {
                            assembly = Assembly.UnsafeLoadFrom(s);
                        }
                        else
                        {
                            assembly = Assembly.LoadFrom(s);
                        }
                        
                        var assemblyName = assembly.FullName.Split(',')[0];
                        if (assemblyName == "System.Data.SQLite")
                        {
                            Drivers.Add(DataBaseNames.Sqlite, assembly);
                        }
                        else if (assemblyName == "Npgsql")
                        {
                            Drivers.Add(DataBaseNames.PostgreSql, assembly);
                        }
                        else if (assemblyName == "MySql.Data")
                        {
                            Drivers.Add(DataBaseNames.MySql, assembly);
                        }
                        else if (assemblyName == "IBM.Data.DB2")
                        {
                            Drivers.Add(DataBaseNames.Db2, assembly);
                        }
                        else if (assemblyName == "Oracle.ManagedDataAccess")
                        {
                            Drivers.Add(DataBaseNames.Oracle, assembly);
                        }
                    }
                    catch
                    {
                        string info = string.Format("AppDomain can't load file {0}", s);
                        LogHelper.LogWarning(info);
                    }
                }
            }
        }

        private static void SyncDbInfo()
        {
            if (!Configure.EnableSync) return;

            foreach (var className in TableNames.Keys)
            {
                if (!Tables[className].EnableSync) continue;

                var tableName = TableNames[className];
                var key = GetTableKey(className);
                if (string.IsNullOrWhiteSpace(key) && Configure.SetKey != null)
                {
                    key = Configure.SetKey.Invoke(className);
                }
                if (string.IsNullOrWhiteSpace(key)) continue;
                try
                {
                    var providerName = GetProviderName(key);
                    var provier = ProviderManager.GetProvider(providerName);
                    try
                    {
                        Configure.LogSqlLevel = TraceLevel.Info;
                        LogHelper.LogInfo(string.Format("Sync {0} to {1}", className, tableName));
                        provier.SyncDbInfo(key, className, tableName);
                        LogHelper.LogInfo(string.Format("Sync {0} to {1} done", className, tableName));
                    }
                    catch (Exception ex)
                    {
                        LogHelper.LogError(ex.ToString());
                    }
                    finally
                    {
                        Configure.LogSqlLevel = TraceLevel.Verbose;
                    }
                }
                catch (ConnectionStringKeyNotExists ex)
                {
                    LogHelper.LogWarning(ex.ToString());
                }
            }
        }

        public static bool ExistsTableAttribute(string tableName)
        {
            return Tables.Keys.Contains(tableName);
        }

        public static string GetTableKey(string typeName)
        {
            if (!Tables.Keys.Contains(typeName))
            {
                throw new ClassNotHaveTableAttributeException(typeName);
            }

            string key = "";
            if (Tables[typeName].EntityPool != null)
            {
                key = DataBases[Tables[typeName].EntityPool.FullName].Key;
            }
            else
            {
                key = Tables[typeName].Key ?? "";
            }

            if (ConnectionStrings.Keys.Contains(key) &&
                string.IsNullOrWhiteSpace(ConnectionStrings.FirstOrDefault(x => x.Key == key).Value.ProviderName))
            {
                throw new ConnectionStringProviderNameNotExists(key);
            }

            return key;
        }

        public static string GetDbKey(string typeName)
        {
            if (!DataBases.Keys.Contains(typeName))
            {
                throw new ClassNotHaveDataBaseAttributeException(typeName);
            }
            else
            {
                return DataBases[typeName].Key ?? "";
            }
        }

        public static SqlHelper GetSqlHelper(string typeName)
        {
            if (!Tables.Keys.Contains(typeName))
            {
                throw new ClassNotHaveTableAttributeException(typeName);
            }

            if (Tables[typeName].EntityPool == null)
            {
                throw new TableAttributeNotHaveEntityPoolException(typeName);
            }

            if (!DataBases.Keys.Contains(Tables[typeName].EntityPool.FullName))
            {
                throw new Exception("no database");
            }

            var key = DataBases[Tables[typeName].EntityPool.FullName].Key;

            if (!ConnectionStrings.Keys.Contains(key))
            {
                throw new DataBaseAttributeKeyNotExists(key);
            }
            var providerName = ConnectionStrings.Where(x => x.Key == key).FirstOrDefault().Value.ProviderName;
            if (string.IsNullOrEmpty(providerName))
            {
                throw new ConnectionStringProviderNameNotExists(providerName);
            }
            if (SqlHelpers.Keys.Contains(key))
            {
                return SqlHelpers[key];
            }
            else
            {
                return null;
            }
        }

        public static SqlHelper GetSqlHelperByKey(string key)
        {
            if (SqlHelpers.Keys.Contains(key))
            {
                return SqlHelpers[key];
            }
            else
            {
                string info = string.Format("Can't found key name {0}", key);
                LogHelper.LogError(info);
                return null;
            }
        }

        public static string GetTableName(string typeName)
        {
            return TableNames[typeName];
        }

        public static Dictionary<string,string> GetColumns(string typeName)
        {
            return ColumnNames[typeName];
        }

        public static void SetColumns(string typeName,Dictionary<string,string> info)
        {
            ColumnNames[typeName] = info;
        }

        public static bool IsExistsColumnName(string typeName)
        {
            return ColumnNames.ContainsKey(typeName);
        }

        private static Func<object, string, object> GenerateGetValue(Type type)
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var memberName = Expression.Parameter(typeof(string), "memberName");
            var nameHash = Expression.Variable(typeof(int), "nameHash");
            var calHash = Expression.Assign(nameHash,
                                            Expression.Call(memberName, typeof(object).GetMethod("GetHashCode")));
            var cases = new List<SwitchCase>();
            foreach (var propertyInfo in type.GetProperties().Where(x => x.CanRead))
            {
                var property = Expression.Property(Expression.Convert(instance, type), propertyInfo.Name);
                var propertyHash = Expression.Constant(propertyInfo.Name.GetHashCode(), typeof(int));

                cases.Add(Expression.SwitchCase(Expression.Convert(property, typeof(object)), propertyHash));
            }
            var switchEx = Expression.Switch(nameHash, Expression.Constant(null), cases.ToArray());
            var methodBody = Expression.Block(typeof(object), new[] { nameHash }, calHash, switchEx);

            return Expression.Lambda<Func<object, string, object>>(methodBody, instance, memberName).Compile();
        }


        private static Action<object, string, object> GenerateSetValue(Type type)
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var memberName = Expression.Parameter(typeof(string), "memberName");
            var newValue = Expression.Parameter(typeof(object), "newValue");
            var nameHash = Expression.Variable(typeof(int), "nameHash");
            var calHash = Expression.Assign(nameHash, Expression.Call(memberName, typeof(object).GetMethod("GetHashCode")));
            var cases = new List<SwitchCase>();
            foreach (var propertyInfo in type.GetProperties().Where(x=>x.CanWrite))
            {
                var property = Expression.Property(Expression.Convert(instance, type), propertyInfo.Name);
                Expression convert;
                if (propertyInfo.PropertyType.IsDigital())
                {
                    var newValueToString = Expression.Call(newValue, "ToString", null);
                    convert = Expression.Call(propertyInfo.PropertyType, "Parse", null, newValueToString);
                }
                else
                {
                    convert = Expression.Convert(newValue, propertyInfo.PropertyType);
                }
                var setValue = Expression.Assign(property, convert);
                var detDefault = Expression.Assign(property, Expression.Default(propertyInfo.PropertyType));
                var ifThenElse = Expression.IfThenElse(Expression.Equal(Expression.Constant(null), newValue), detDefault, setValue);
                var propertyHash = Expression.Constant(propertyInfo.Name.GetHashCode(), typeof(int));

                cases.Add(Expression.SwitchCase(ifThenElse, propertyHash));
            }
            var switchEx = Expression.Switch(nameHash, null, cases.ToArray());
            var methodBody = Expression.Block(new[] { nameHash }, calHash, switchEx);

            return Expression.Lambda<Action<object, string, object>>(methodBody, instance, memberName, newValue).Compile();
        }

        public static string GetPrimaryMemberName(string typeName)
        {
            var pk = PrimaryKeys[typeName].Keys;
            if (!pk.Any())
            {
                throw new TableNotHavePrimaryKeyException(typeName);
            }
            return pk.ToList()[0];
        }
        public static string GetPrimaryColumnName(string typeName)
        {
            var pk = GetPrimaryMemberName(typeName);
            return GetColumns(typeName)[pk];
        }
        public static string GetAutoGrowMemberName(string typeName)
        {
            foreach (var columnName in ColumnNames[typeName])
            {
                if (Columns[typeName][columnName.Key].IsAutoGrow)
                {
                    return columnName.Key;
                }
            }
            return "";
        }

        public static string GetAutoGrowColumnName(string typeName)
        {
            foreach (var columnName in ColumnNames[typeName])
            {
                if (Columns[typeName][columnName.Key].IsAutoGrow)
                {
                    return columnName.Value;
                }
            }
            return "";
        }

        public static Dictionary<string, Type> GetMemberTypes(string typeName)
        {
            return ColumnTypes[typeName];
        }

        public static Type GetMemberType(string typeName,string memberName)
        {
            return ColumnTypes[typeName][memberName];
        }

        public static Type GetPrimaryKeyType(string typeName)
        {
            return PrimaryKeys[typeName].Values.ToList()[0];
        }

        public static Dictionary<string, ColumnAttribute> GetColumnAttributes(string typeName)
        {
            return Columns[typeName];
        }

        public static ColumnAttribute GetColumnAttribute(string typeName,string memberName)
        {
            return Columns[typeName][memberName];
        }

        public static int GetColumnLength(string typeName, string memberName)
        {
            return Columns[typeName][memberName].Length;
        }

        public static string GetColumnType(string typeName, string memberName)
        {
            return Columns[typeName][memberName].Type ?? "";
        }

        public static string GetColumnName(string typeName, string memberName)
        {
            return ColumnNames[typeName][memberName];
        }

        public static bool GetColumnIsNullAble(string typeName, string memberName)
        {
            return Columns[typeName][memberName].IsNullAble;
        }

        public static bool GetColumnIsAutoGrow(string typeName, string memberName)
        {
            return Columns[typeName][memberName].IsAutoGrow;
        }

        public static bool GetColumnIsPrimaryKey(string typeName, string memberName)
        {
            return Columns[typeName][memberName].IsPrimaryKey;
        }

        public static Func<object, string, object> GetGetDelegate(string typeName)
        {
            return GetDelegates[typeName];
        }

        public static Action<object, string, object> GetSetDelegate(string typeName)
        {
            return SetDelegates[typeName];
        }

        public static void SetDelegate(Type type)
        {
            if (type == null)
            {
                return;
            }
            SetDelegates.Add(type.FullName, GenerateSetValue(type));
        }
        public static Dictionary<string, SqlHelper> GetSqlHelpers()
        {
            return SqlHelpers;
        }

        public static Assembly GetDriver(string providerName)
        {
            if (Drivers.Keys.Contains(providerName))
            {
                return Drivers[providerName];
            }
            else
            {
                return null;
            }
        }

        public static ConnectionStringSettings GetConnectionStringSettings(string key)
        {
            if (!ConnectionStrings.Keys.Contains(key))
            {
                throw new ConnectionStringKeyNotExists(key);
            }
            return ConnectionStrings[key];
        }

        public static bool IsExistsKey(string key)
        {
            if (ConnectionStrings.Keys.Contains(key))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static string GetConnectionString(string key)
        {
            return GetConnectionStringSettings(key).ConnectionString;
        }

        public static string GetProviderName(string key)
        {
            return GetConnectionStringSettings(key).ProviderName;
        }
    }
}
