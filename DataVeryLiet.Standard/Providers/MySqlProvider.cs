using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Data.Common;
using System.Dynamic;
using DataVeryLite.Core;
using DataVeryLite.Util;

namespace DataVeryLite.Providers
{
    class MySqlProvider : IProvider
    {

        public string ProviderName
        {
            get
            {
                return DataBaseNames.MySql;
            }
        }

        public System.Reflection.Assembly GetAssembly(string path)
        {
            return Assembly.LoadFile(path + "MySql.Data.dll");
        }

        public string GetDataBaseName(DbConnectionStringBuilder builder)
        {
            object database = "";
            builder.TryGetValue("database", out database);
            if (database == null || string.IsNullOrEmpty(database.ToString()))
            {
                builder.TryGetValue("initial catalog", out database);
            }
            if (database == null || string.IsNullOrEmpty(database.ToString()))
            {
                throw new Exception(ProviderName + " must have databse");
            }
            return database.ToString().FirstLetterToUpper();
        }


        public List<dynamic> GetTables(SqlHelper sqlHelper, string dataBaseName)
        {
            var list = new List<dynamic>();
            var dr = sqlHelper.ExecuteReader("Select TABLE_NAME name from INFORMATION_SCHEMA.tables where TABLE_SCHEMA='" +dataBaseName + "'");
            while (dr.Read())
            {
                dynamic eo = new ExpandoObject();
                eo.name = dr["name"].ToString().FirstLetterToUpper();
                list.Add(eo);
            }
            dr.Close();
            dr.Dispose();
            return list;
        }

        public List<dynamic> GetColumns(SqlHelper sqlHelper, string dataBaseName, string tableName)
        {
            return
                     sqlHelper.ExecuteDynamic("select COLUMN_NAME name,DATA_TYPE type,IS_NULLABLE isnullable,CHARACTER_MAXIMUM_LENGTH length  from INFORMATION_SCHEMA.columns where TABLE_SCHEMA='" + dataBaseName + "' and TABLE_NAME='" + tableName + "'");
        }

        private List<dynamic> _tableInfos = null;
        private string _lastTableName = null;
        public string GetColumnType(SqlHelper sqlHelper, string dataBaseName, string tableName, string columnName)
        {
            if (tableName != _lastTableName || _tableInfos == null)
            {
                _tableInfos = new List<dynamic>();
                DbDataReader dr =
                    sqlHelper.ExecuteReader("select * from information_schema.columns where table_schema='" + dataBaseName +
                                  "' and table_name ='" + tableName + "'");
                while (dr.Read())
                {
                    string type = dr["data_type"].ToString().ToLower();

                    #region typedef

                    if (type == "varchar" || type == "char" || type == "text")
                    {
                        type = "string";
                    }
                    else if (type == "real" || type == "float")
                    {
                        type = "float";
                    }
                    else if (type == "double")
                    {
                        type = "double";
                    }
                    else if (type == "decimal" || type == "numeric" || type == "money")
                    {
                        type = "decimal";
                    }
                    else if (type == "bigint")
                    {
                        type = "long";
                    }
                    else if (type == "int" || type == "integer" || type == "mediumint")
                    {
                        type = "int";
                    }
                    else if (type == "tinyint")
                    {
                        type = "byte";
                    }
                    else if (type == "smallint")
                    {
                        type = "short";
                    }
                    else if (type == "bit" || type == "boolean")
                    {
                        type = "bool";
                    }
                    else if (type == "datetime" || type == "date" || type == "time" || type == "timestamp" ||
                             type == "year")
                    {
                        type = "DateTime";
                    }
                    else if (type == "blob" || type == "varbinary" || type == "binary" || type.Contains("blob"))
                    {
                        type = "byte[]";
                    }
                    else
                    {
                        type = "string";
                    }

                    #endregion

                    dynamic tableInfo = new ExpandoObject();
                    tableInfo.columnName = dr["column_name"].ToString();
                    tableInfo.type = type;
                    _tableInfos.Add(tableInfo);
                }
                dr.Close();
                dr.Dispose();
            }
            _lastTableName = tableName;
            foreach (dynamic item in _tableInfos)
            {
                if (item.columnName == columnName)
                {
                    return item.type;
                }
            }
            return "string";
        }

        public object GetPrimaryKey(SqlHelper sqlHelper, string dataBaseName, string tableName)
        {
            return
                   sqlHelper.ExecuteScalar("select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='" + dataBaseName + "' and table_name='" + tableName + "' AND COLUMN_KEY='PRI'");
        }

        public DbCommand GetCmd(Assembly driverAssembly)
        {
            return (DbCommand)driverAssembly.CreateInstance("MySql.Data.MySqlClient.MySqlCommand");
        }

        public DbConnection GetConn(Assembly driverAssembly, string dbConnectionStr)
        {
            return
                   (DbConnection)
                   driverAssembly.CreateInstance("MySql.Data.MySqlClient.MySqlConnection", true, BindingFlags.Default, null,
                                      new object[] { dbConnectionStr }, null, null);
        }

        public System.Data.IDataAdapter GetAdapter(Assembly driverAssembly, DbCommand cmd)
        {
           return (System.Data.IDataAdapter)
                     driverAssembly.CreateInstance("MySql.Data.MySqlClient.MySqlDataAdapter", true, BindingFlags.Default, null,
                                        new object[] { cmd }, null, null);
        }
        public string GetLastInsertIdSql(string tableName)
        {
            return "SELECT LAST_INSERT_ID();";
        }

        public string GetLeftEscape()
        {
            return "`";
        }

        public string GetRightEscape()
        {
            return "`";
        }
        public string GetPageSql(int page, int pageSize, bool asc, string talbeName, string idColumnName)
        {
            string _left = GetLeftEscape();
            string _right = GetRightEscape();
            string sql;
            if (asc)
            {
                sql = string.Format("select * from {0} limit {1},{2}", _left + talbeName + _right, (page - 1) * pageSize,
                                         pageSize);
            }
            else
            {
                sql = string.Format("select * from {0} order by {1} desc limit {2},{3}", _left + talbeName + _right,
                                         _left + idColumnName + _right, (page - 1) * pageSize, pageSize);
            }
            return sql;
        }
        public string GetTopSql(int top, bool asc, string talbeName, string idColumnName)
        {
            string _left = GetLeftEscape();
            string _right = GetRightEscape();
            string sql;
            if (asc)
            {
                sql = string.Format("select * from {0} limit {1}", _left + talbeName + _right, top);
            }
            else
            {
                sql = string.Format("select * from {0} order by {1} desc limit {2}", _left + talbeName + _right, _left + idColumnName + _right, top);
            }
            return sql;
        }

        public bool IsSupportBatch
        {
            get { return true; }
        }

        public string BatchBegin
        {
            get { return ""; }
        }

        public string BatchEnd
        {
            get { return ""; }
        }


        public void SyncDbInfo(string key, string className, string tableName)
        {
            var sqlhelper = IntrospectionManager.GetSqlHelperByKey(key);


            if (IsNeedAddTable(sqlhelper, tableName))
            {
                var columns = IntrospectionManager.GetColumnAttributes(className).OrderByDescending(x => x.Value.IsPrimaryKey);
                var realAutoGrowName = GetRealAutoGrowName(sqlhelper, tableName);
                var cols = Warp.ShieldLogSql(() => GetColumns(sqlhelper, sqlhelper.DataBaseName, tableName));
                foreach (var column in columns)
                {
                    var memberType = IntrospectionManager.GetMemberType(className, column.Key);
                    var columnType = IntrospectionManager.GetColumnType(className, column.Key);
                    var columnName = IntrospectionManager.GetColumnName(className, column.Key);
                    var length = IntrospectionManager.GetColumnLength(className, column.Key);
                    var isAutoGrow = IntrospectionManager.GetColumnIsAutoGrow(className, column.Key);
                    var isRealAutoGrow = IsRealAutoGrow(sqlhelper, tableName, columnName);
                    var isNullAble = IntrospectionManager.GetColumnIsNullAble(className, column.Key);
                    var isPk = IntrospectionManager.GetColumnIsPrimaryKey(className, column.Key);
                    var isRealPk = IsRealPkInDb(sqlhelper, tableName, columnName);

                    var autoGrowSql = isAutoGrow ? "auto_increment" : "";
                    var nullAbleSql = isNullAble ? " null" : " not null";
                    nullAbleSql = isPk ? " not null" : nullAbleSql;
                    var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                    if (cols.Any(x => x.name.ToUpper() == columnName.ToUpper()))
                    {
                        if (!IsNeedModiy(cols, memberType, columnName, lastColumnType, length, isNullAble, isPk, isRealPk,isAutoGrow,isRealAutoGrow))
                        {
                            continue;
                        }
                        if (isPk != isRealPk)
                        {
                            if (realAutoGrowName != null)
                            {
                                var dropautogrow = string.Format("alter table `{0}` change `{1}` `{1}`  {2} not null", tableName, realAutoGrowName, lastColumnType);
                                sqlhelper.ExecuteNonQuery(dropautogrow);
                            }

                            var getconstraint = string.Format(
                                    "select constraint_name name from information_schema.key_column_usage where table_name='{0}'",
                                    tableName);
                            var constraints = Warp.ShieldLogSql(() => sqlhelper.ExecuteDynamic(getconstraint));
                            if (constraints.Any())
                            {
                                var dropconstraint = "alter table `" + tableName + "` drop primary key";
                                sqlhelper.ExecuteNonQuery(dropconstraint);
                            }
                        }
                        if (isPk && isPk != isRealPk)
                        {

                            var alterSql = string.Format("alter table `{0}` add primary key(`{1}`)", tableName, columnName);
                            sqlhelper.ExecuteNonQuery(alterSql);

                            alterSql = string.Format("alter table `{0}` modify column `{1}` {2} not null {3}", tableName, columnName, lastColumnType, autoGrowSql);
                            sqlhelper.ExecuteNonQuery(alterSql);
                        }
                        else
                        {
                            if (isRealPk)
                            {
                                if (realAutoGrowName != null&&isRealAutoGrow)
                                {
                                    var dropautogrow = string.Format("alter table `{0}` change `{1}` `{1}`  {2} not null", tableName, realAutoGrowName, lastColumnType);
                                    sqlhelper.ExecuteNonQuery(dropautogrow);
                                }
                                var alterSql1 = string.Format("alter table `{0}` drop primary key", tableName);
                                sqlhelper.ExecuteNonQuery(alterSql1);
                            }
                            var alterSql = string.Format("alter table `{0}` modify column `{1}` {2} {3}", tableName, columnName, lastColumnType, nullAbleSql);
                            sqlhelper.ExecuteNonQuery(alterSql);
                        }
                    }
                    else
                    {
                        if (isPk != isRealPk)
                        {
                            if (realAutoGrowName != null)
                            {
                                var dropautogrow = string.Format("alter table `{0}` change `{1}` `{1}`  {2} not null", tableName, realAutoGrowName, lastColumnType);
                                sqlhelper.ExecuteNonQuery(dropautogrow);
                            }

                            var getconstraint = string.Format(
                                    "select constraint_name name from information_schema.key_column_usage where table_name='{0}'",
                                    tableName);
                            var constraints = Warp.ShieldLogSql(() => sqlhelper.ExecuteDynamic(getconstraint));
                            if (constraints.Any())
                            {
                                var dropconstraint = "alter table `" + tableName + "` drop primary key";
                                sqlhelper.ExecuteNonQuery(dropconstraint);
                            }
                        }

                        var alterSql = string.Format("alter table `{0}` add `{1}` {2} {3} {4}", tableName, columnName, lastColumnType, nullAbleSql, autoGrowSql);
                        sqlhelper.ExecuteNonQuery(alterSql);
                        if (isPk != isRealPk)
                        {
                            alterSql = string.Format("alter table `{0}` add  primary key(`{1}`)", tableName, columnName);
                            sqlhelper.ExecuteNonQuery(alterSql);
                        }

                    }
                }
            }
            else
            {
                var columns = IntrospectionManager.GetColumns(className);
                var columnsArr = columns.Select(x =>
                {
                    var memberType = IntrospectionManager.GetMemberType(className, x.Key);
                    var columnType = IntrospectionManager.GetColumnType(className, x.Key);
                    var length = IntrospectionManager.GetColumnLength(className, x.Key);
                    var isAutoGrow = IntrospectionManager.GetColumnIsAutoGrow(className, x.Key);
                    var isPk = IntrospectionManager.GetColumnIsPrimaryKey(className, x.Key);
                    var autoGrowSql = isAutoGrow ? "auto_increment" : "";
                    var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                    if (isPk)
                    {
                        return string.Format("`{0}` {1} primary key {2}", x.Value, lastColumnType, autoGrowSql);
                    }
                    return string.Format("`{0}` {1} {2}", x.Value, lastColumnType, autoGrowSql);
                });
                var columnsSql = string.Join(",", columnsArr);
                var createTable = "create table `" + tableName + "`(" + columnsSql + ")";
                sqlhelper.ExecuteNonQuery(createTable);
            }
        }


        public string GetColumnTypeByMebmberType(Type type, string columnType, int length)
        {
            if (length == 0)
            {
                length = 50;
            }

            if (!string.IsNullOrWhiteSpace(columnType))
            {
                if (_filterType.Contains(columnType))
                {
                    return columnType;
                }
                else
                {
                    return columnType + "(" + length + ")";
                }
            }

            if (type == typeof(int) || type == typeof(uint))
            {
                return "int";
            }
            else if (type == typeof(long) || type == typeof(ulong))
            {
                return "bigint";
            }
            else if (type == typeof(short) || type == typeof(ushort))
            {
                return "smallint";
            }
            else if (type == typeof(decimal))
            {
                return "decimal";
            }
            else if (type == typeof(double))
            {
                return "double";
            }
            else if (type == typeof(float))
            {
                return "float";
            }
            else if (type == typeof(bool))
            {
                return "bit";
            }
            else if (type == typeof(byte))
            {
                return "tinyint";
            }
            else if (type == typeof(byte[]))
            {
                return "blob";
            }
            else if (type.IsDigital())
            {
                return "int";
            }
            else if (type == typeof(DateTime))
            {
                return "datetime";
            }
            else if (type == typeof(string) && length == int.MaxValue)
            {
                return "text";
            }
            else if (type == typeof(string))
            {
                return "varchar(" + length + ")";
            }
            return "varchar(" + length + ")";
        }

        private readonly List<string> _filterType = new List<string>
            {
                "int",
                "bigint",
                "smallint",
                "tinyint",
                "decimal",
                "double",
                "float",
                "real",
                "text",
                "ntext",
                "uniqueidentifier",
                "datetime",
                "bit",
                "date",
                "datetime2",
                "datetimeoffset",
                "smalldatetime",
                "time",
                "timestamp",
                "money",
                "numeric",
                "smallmoney",
                "binary",
                "image",
                "varbinary",
                "blob",
                "clob"
            };

        private bool IsNeedModiy(IEnumerable<dynamic> cols, Type memberType, string columnName, string columnType,
            int length, bool isNullAble, bool isPk, bool isRealPk,bool isAutoGrow,bool isRealAutoGrow)
        {
            if (length == 0) length = 50;

            if (isPk != isRealPk)
            {
                return true;
            }

            if (isPk&&isAutoGrow != isRealAutoGrow)
            {
                return true;
            }

            if (isPk)
            {
                if (_filterType.Contains(columnType.ToLower()))
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0]);
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0] && x.length == length.ToString());
                }
            }
            else
            {
                if (_filterType.Contains(columnType.ToLower()))
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0] && x.isnullable == isNullAble.ToYesNo());
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0] && x.length == length.ToString() && x.isnullable == isNullAble.ToYesNo());
                }
            }
        }

        private bool IsRealPkInDb(SqlHelper sqlhelper, string tableName, string columnName)
        {
            var getconstraint = string.Format("select table_name,column_name from information_schema.key_column_usage where table_name='{0}' and column_name='{1}'",
                                              tableName, columnName);
            var constraints = Warp.ShieldLogSql(() => sqlhelper.ExecuteDynamic(getconstraint));
            return constraints.Any();
        }

        private bool IsRealAutoGrow(SqlHelper sqlHelper, string tableName, string columnName)
        {
            var getconstraint = string.Format("select column_name from information_schema.columns where table_name='{0}' and extra='auto_increment' and column_name='{1}'", tableName, columnName);
            var constraints = Warp.ShieldLogSql(() => sqlHelper.ExecuteDynamic(getconstraint));
            return constraints.Any();
        }

        private string GetRealAutoGrowName(SqlHelper sqlHelper, string tableName)
        {
            var getconstraint = string.Format("select column_name name from information_schema.columns where table_name='{0}' and extra='auto_increment'", tableName);
            var constraints = Warp.ShieldLogSql(() => sqlHelper.ExecuteDynamic(getconstraint));
            var firstOrDefault = constraints.FirstOrDefault();
            if (firstOrDefault != null)
            {
                return firstOrDefault.name;
            }
            else
            {
                return null;
            }
        }

        private bool IsNeedAddTable(SqlHelper sqlhelper, string tableName)
        {
            var tables = Warp.ShieldLogSql(() => GetTables(sqlhelper, sqlhelper.DataBaseName));
            return tables.Any(x => x.name.ToUpper() == tableName.ToUpper());
        }
    }
}
