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
    class SqlServerProvider:IProvider
    {
        public string ProviderName
        {
            get { return DataBaseNames.SqlServer; }
        }

        public System.Reflection.Assembly GetAssembly(string path)
        {
            return Assembly.GetAssembly(typeof(System.Data.SqlClient.SqlConnection));
        }

        public string GetDataBaseName(System.Data.Common.DbConnectionStringBuilder builder)
        {
            object database = "";
            builder.TryGetValue("database", out database);
            if (database == null || string.IsNullOrEmpty(database.ToString()))
            {
                builder.TryGetValue("initial catalog", out database);
            }
            if (database == null || string.IsNullOrEmpty(database.ToString()))
            {
                builder.TryGetValue("data source", out database);
            }
            if (database == null || string.IsNullOrEmpty(database.ToString()))
            {
                throw new Exception(ProviderName+" must have databse");
            }
            return database.ToString();
        }


        public List<dynamic> GetTables(SqlHelper sqlHelper, string dataBaseName)
        {
                return
                    sqlHelper.ExecuteDynamic("select Name name from sysobjects where xtype='u'");
        }

        public List<dynamic> GetColumns(SqlHelper sqlHelper, string dataBaseName, string tableName)
        {
                return sqlHelper.ExecuteDynamic("select name name,data_type type,length length,is_nullable isnullable " +
                                                " from syscolumns " +
                                                "inner join information_schema.columns " +
                                                "on(name=column_name and table_name='" + tableName + "') " +
                                                "where id=object_id('" + tableName + "')");
        }

        private List<dynamic> _tableInfos = null;
        private string _lastTableName = null;
        public string GetColumnType(SqlHelper sqlHelper, string dataBaseName, string tableName, string columnName)
        {
            if (tableName != _lastTableName || _tableInfos == null)
            {
                _tableInfos = new List<dynamic>();
                DbDataReader dr = sqlHelper.ExecuteReader("select COLUMN_NAME,DATA_TYPE  from information_schema.columns where table_name='" + tableName + "'");
                while (dr.Read())
                {
                    string type = dr["DATA_TYPE"].ToString().ToLower();
                    if (type == "int")
                    {
                        type = "int";
                    }
                    else if (type == "bigint")
                    {
                        type = "long";
                    }
                    else if (type == "smallint")
                    {
                        type = "short";
                    }
                    else if (type == "decimal" || type == "money" || type == "numeric" || type == "smallmoney")
                    {
                        type = "decimal";
                    }
                    else if (type == "float")
                    {
                        type = "double";
                    }
                    else if (type == "real")
                    {
                        type = "float";
                    }
                    else if (type == "varchar" || type == "text" || type == "char" || type == "nchar" || type == "ntext" || type == "nvarchar")
                    {
                        type = "string";
                    }
                    else if (type == "datetime" || type == "date" || type == "datetime2" || type == "datetimeoffset" || type == "smalldatetime" || type == "time" || type == "timestamp")
                    {
                        type = "DateTime";
                    }
                    else if (type == "bit")
                    {
                        type = "bool";
                    }
                    else if (type == "tinyint")
                    {
                        type = "byte";
                    }
                    else if (type == "binary" || type == "image" || type == "varbinary")
                    {
                        type = "byte[]";
                    }
                    else
                    {
                        type = "string";
                    }
                    dynamic tableInfo = new ExpandoObject();
                    tableInfo.columnName = dr["COLUMN_NAME"].ToString();
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
            return sqlHelper.ExecuteScalar("select  name from syscolumns where id=object_id('" + tableName + "') and colid=(select top 1 keyno from sysindexkeys where id=object_id('" + tableName + "'))");
        }

        public DbCommand GetCmd(Assembly driverAssembly)
        {
            return (DbCommand)driverAssembly.CreateInstance("System.Data.SqlClient.SqlCommand");
        }

        public DbConnection GetConn(Assembly driverAssembly, string dbConnectionStr)
        {
            return
                    (DbConnection)
                    driverAssembly.CreateInstance("System.Data.SqlClient.SqlConnection", true, BindingFlags.Default, null,
                                       new object[] { dbConnectionStr }, null, null);
        }

        public System.Data.IDataAdapter GetAdapter(Assembly driverAssembly, System.Data.Common.DbCommand cmd)
        {
            return
                     (System.Data.IDataAdapter)
                     driverAssembly.CreateInstance("System.Data.SqlClient.SqlDataAdapter", true, BindingFlags.Default, null,
                                        new object[] { cmd }, null, null);
        }
        public string GetLastInsertIdSql(string tableName)
        {
            return "select scope_identity();";
        }

        public string GetLeftEscape()
        {
            return "[";
        }

        public string GetRightEscape()
        {
            return "]";
        }
        public string GetPageSql(int page, int pageSize, bool asc, string talbeName, string idColumnName)
        {
            string _left = GetLeftEscape();
            string _right = GetRightEscape();
            string sql;
            if (asc)
            {
                sql =
                        string.Format(
                            "select * from (select row_number() over(order by {3}) as row_number,* from {0}) a where row_number>{1} and row_number < {2} order by {3}",
                            _left + talbeName + _right, (page - 1) * pageSize, page * pageSize+1, _left + idColumnName + _right);
            }
            else
            {
                sql =
                        string.Format(
                            "select * from (select row_number() over(order by {3} desc) as row_number,* from {0}) a where row_number>{1} and row_number < {2} order by {3} desc",
                            _left + talbeName + _right, (page - 1) * pageSize, page * pageSize+1, _left + idColumnName + _right);
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
                sql = string.Format("select top {1} * from {0}", _left + talbeName + _right, top);
            }
            else
            {
                sql = string.Format("select top {1} * from {0} order by {2} desc", _left + talbeName + _right, top, _left + idColumnName + _right);
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

        public void SyncDbInfo(string key,string className,string tableName)
        {
            var sqlhelper = IntrospectionManager.GetSqlHelperByKey(key);


            if (IsNeedAddTable(sqlhelper, tableName))
            {
                #region Alter
                var columns = IntrospectionManager.GetColumnAttributes(className).OrderByDescending(x => x.Value.IsPrimaryKey);
                var cols = Warp.ShieldLogSql(() => GetColumns(sqlhelper, sqlhelper.DataBaseName, tableName));

                var needRebuild = false;
                foreach (var column in columns)
                {
                    var columnName = IntrospectionManager.GetColumnName(className, column.Key);
                    var isAutoGrow = IntrospectionManager.GetColumnIsAutoGrow(className, column.Key);
                    var isRealAutoGrow = IsRealAutoGrow(sqlhelper, tableName, columnName);
                    if (isAutoGrow != isRealAutoGrow)
                    {
                        needRebuild = true;
                    }
                }

                if (needRebuild)
                {
                    ModiyTable(sqlhelper, className, tableName);
                    return;
                }

                foreach (var column in columns)
                {
                    #region Alter
                    var memberType = IntrospectionManager.GetMemberType(className, column.Key);
                    var columnType = IntrospectionManager.GetColumnType(className, column.Key);
                    var columnName = IntrospectionManager.GetColumnName(className, column.Key);
                    var length = IntrospectionManager.GetColumnLength(className, column.Key);
                    var isAutoGrow = IntrospectionManager.GetColumnIsAutoGrow(className, column.Key);
                    var isRealAutoGrow = IsRealAutoGrow(sqlhelper, tableName, columnName);
                    var isNullAble = IntrospectionManager.GetColumnIsNullAble(className, column.Key);
                    var isPk = IntrospectionManager.GetColumnIsPrimaryKey(className, column.Key);
                    var isRealPk = IsRealPkInDb(sqlhelper, tableName, columnName);

                    var autoGrowSql = isAutoGrow ? "identity(1,1)" : "";
                    var nullAbleSql = isNullAble ? " null" : " not null";
                    nullAbleSql = isPk ||isRealAutoGrow ? " not null" : nullAbleSql;
                    var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                    if (cols.Any(x => x.name.ToUpper() == columnName.ToUpper()))
                    {
                        if (!IsNeedModiy(cols, memberType, columnName, lastColumnType, length, isNullAble, isPk, isRealPk,isAutoGrow,isRealAutoGrow))
                        {
                            continue;
                        }
                        if (isPk != isRealPk)
                        {
                            var getconstraint = string.Format("select constraint_name name from information_schema.key_column_usage where table_name='{0}'",tableName);
                            var constraints = Warp.ShieldLogSql(() => sqlhelper.ExecuteDynamic(getconstraint));
                            foreach (var constraint in constraints)
                            {
                                var dropconstraint = "alter table [" + tableName + "] drop constraint [" + constraint.name+"]";
                                sqlhelper.ExecuteNonQuery(dropconstraint);
                            }
                        }
                        if (isPk && isPk != isRealPk)
                        {
                            var alterSql = string.Format("alter table [{0}] alter column [{1}] {2} not null", tableName, columnName, lastColumnType);
                            sqlhelper.ExecuteNonQuery(alterSql);
                            alterSql = string.Format("alter table [{0}] add constraint [{1}] primary key([{2}])", tableName, "pk_" + tableName + "_" + columnName, columnName);
                            sqlhelper.ExecuteNonQuery(alterSql);
                        }
                        else
                        {
                            var alterSql = string.Format("alter table [{0}] alter column [{1}] {2} {3}", tableName, columnName, lastColumnType, nullAbleSql);
                            sqlhelper.ExecuteNonQuery(alterSql);
                        }
                    }
                    else
                    {
                        if (isPk != isRealPk)
                        {
                            var getconstraint = string.Format("select  name from sysobjects so " +
                                                              "join sysconstraints sc " +
                                                              "on so.id = sc.constid " +
                                                              "where object_name(so.parent_obj) = '{0}'" +
                                                              " and so.xtype = 'PK'", tableName);
                            var constraints = Warp.ShieldLogSql(() =>sqlhelper.ExecuteDynamic(getconstraint));
                            foreach (var constraint in constraints)
                            {
                                var dropconstraint = "alter table [" + tableName + "] drop constraint [" + constraint.name+"]";
                                sqlhelper.ExecuteNonQuery(dropconstraint);
                            }
                        }
                        var alterSql = string.Format("alter table [{0}] add [{1}] {2} {3} {4}", tableName, columnName, lastColumnType, nullAbleSql, autoGrowSql);
                        sqlhelper.ExecuteNonQuery(alterSql);
                        if (isPk != isRealPk)
                        {
                            alterSql = string.Format("alter table [{0}] add constraint [{1}] primary key([{2}])", tableName, "pk_" + tableName + "_" + columnName, columnName);
                            sqlhelper.ExecuteNonQuery(alterSql);
                        }
                           
                    }
                    
                    #endregion
                }
                #endregion
            }
            else
            {
                CreateTable(sqlhelper, null, className, tableName);
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

            if (type == typeof (int) || type == typeof (uint))
            {
                return "int";
            }
            else if (type == typeof (long) || type == typeof (ulong))
            {
                return "bigint";
            }
            else if (type == typeof (short) || type == typeof (ushort))
            {
                return "smallint";
            }
            else if (type == typeof (decimal))
            {
                return "decimal";
            }
            else if (type == typeof (double))
            {
                return "float";
            }
            else if (type == typeof (float))
            {
                return "real";
            }
            else if (type == typeof (bool))
            {
                return "bit";
            }
            else if (type == typeof (byte))
            {
                return "tinyint";
            }
            else if (type == typeof (byte[]))
            {
                return "varbinary";
            }
            else if (type.IsDigital())
            {
                return "int";
            }
            else if (type == typeof (DateTime))
            {
                return "datetime";
            }
            else if (type == typeof(Guid))
            {
                return "uniqueidentifier";
            }
            else if (type == typeof (string) && length == int.MaxValue)
            {
                return "text";
            }
            else if (type == typeof (string))
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
                "varbinary"
            };

        private bool IsNeedModiy(IEnumerable<dynamic> cols, Type memberType, string columnName, string columnType,
            int length, bool isNullAble, bool isPk, bool isRealPk,bool isAutoGrow,bool isRealAutoGrow)
        {
            if (length == 0) length = 50;

            if (isPk != isRealPk)
            {
                return true;
            }

            if (isPk || isAutoGrow)
            {
                if ( _filterType.Contains(columnType.ToLower()))
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0]);
                }
                else if (columnType.Split('(')[0] == "nvarchar" || columnType.Split('(')[0] == "nchar")
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0] && x.length == 2 * length);
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0] && x.length == length);
                }
            }
            else
            {
                if (_filterType.Contains(columnType.ToLower()))
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0] && x.isnullable == isNullAble.ToYesNo());
                }
                else if (columnType.Split('(')[0] == "nvarchar" || columnType.Split('(')[0] == "nchar")
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0] && x.length == 2*length && x.isnullable == isNullAble.ToYesNo());
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0] && x.length == length && x.isnullable == isNullAble.ToYesNo());
                }
            }
        }

        private bool IsRealPkInDb(SqlHelper sqlhelper, string tableName, string columnName)
        {
            var getconstraint = string.Format("select table_name,column_name from information_schema.key_column_usage where table_name='{0}' and column_name='{1}'",
                                              tableName, columnName);
            var constraints = Warp.ShieldLogSql(() =>sqlhelper.ExecuteDynamic(getconstraint));
            return constraints.Any();
        }

        private bool IsRealAutoGrow(SqlHelper sqlHelper, string tableName, string columnName)
        {
            var getconstraint = string.Format("select name from sys.columns where object_id=object_id('{0}') and is_identity=1 and name='{1}'", tableName, columnName);
            var constraints = Warp.ShieldLogSql(() => sqlHelper.ExecuteDynamic(getconstraint));
            return constraints.Any();
        }

        private bool IsNeedAddTable(SqlHelper sqlhelper, string tableName)
        {
            var tables = Warp.ShieldLogSql(() => GetTables(sqlhelper, sqlhelper.DataBaseName));
            return tables.Any(x => x.name.ToUpper() == tableName.ToUpper());
        }

        public void CreateTable(SqlHelper sqlhelper,DbTransaction tran, string className, string tableName)
        {
            var columns = IntrospectionManager.GetColumns(className);
            var columnsArr = columns.Select(x =>
            {
                var memberType = IntrospectionManager.GetMemberType(className, x.Key);
                var columnType = IntrospectionManager.GetColumnType(className, x.Key);
                var length = IntrospectionManager.GetColumnLength(className, x.Key);
                var isAutoGrow = IntrospectionManager.GetColumnIsAutoGrow(className, x.Key);
                var isPk = IntrospectionManager.GetColumnIsPrimaryKey(className, x.Key);
                var isNullAble = IntrospectionManager.GetColumnIsNullAble(className, x.Key);
                var nullAbleSql = isNullAble ? "" : "not null";
                nullAbleSql = isPk || isAutoGrow ? "not null" : nullAbleSql;
                var autoGrowSql = isAutoGrow ? "identity(1,1)" : "";
                var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                if (isPk)
                {
                    return string.Format("[{0}] {1} primary key {2}", x.Value, lastColumnType, autoGrowSql);
                }
                return string.Format("[{0}] {1} {2} {3}", x.Value, lastColumnType, autoGrowSql, nullAbleSql);
            });
            var columnsSql = string.Join(",", columnsArr);
            var createTable = "create table [" + tableName + "](" + columnsSql + ")";
            if (tran == null)
            {
                sqlhelper.ExecuteNonQuery(createTable);
            }
            else
            {
                sqlhelper.ExecuteNonQuery(tran, createTable);
            }
        }

        public void ModiyTable(SqlHelper sqlhelper, string className, string tableName)
        {
            DbConnection conn = sqlhelper.GetConn();
            conn.Open();
            DbTransaction tran = conn.BeginTransaction();
            try
            {
                var columns = IntrospectionManager.GetColumns(className);
                var columnsArr = string.Join(",", columns.Values.Select(x => "[" + x + "]"));
                var newTableName = tableName + DateTime.Now.ToString("yyyyMMdd_HHmmss");
                CreateTable(sqlhelper, tran, className, newTableName);
                var autoGrowName = IntrospectionManager.GetAutoGrowColumnName(className);
                var sql = "";
                if (!string.IsNullOrWhiteSpace(autoGrowName))
                {
                    sql = "set identity_insert [" + newTableName + "] on;";
                }
                sql += string.Format("insert into [{0}]({1}) select {1} from [{2}];", newTableName, columnsArr, tableName);
                if (!string.IsNullOrWhiteSpace(autoGrowName))
                {
                    sql += "set identity_insert [" + newTableName + "] off;";
                }
                sqlhelper.ExecuteNonQuery(tran, sql);
                sqlhelper.ExecuteNonQuery(tran, "drop table [" + tableName+"]");
                sqlhelper.ExecuteNonQuery(tran, "sp_rename '" + newTableName + "','" + tableName + "'");
                tran.Commit();
            }
            catch (Exception ex)
            {
                tran.Rollback();
                LogHelper.LogError(ex.ToString());
            }
            finally
            {
                conn.Close();
            }
        }
    }
}
