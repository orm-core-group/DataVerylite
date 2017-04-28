using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.IO;
using System.Dynamic;
using System.Data.Common;
using DataVeryLite.Core;
using DataVeryLite.Util;

namespace DataVeryLite.Providers
{
    class SqliteProvider:IProvider
    {
        public string ProviderName
        {
            get { return DataBaseNames.Sqlite; }
        }

        public System.Reflection.Assembly GetAssembly(string path)
        {
            //byte[] assemblyBuffer = File.ReadAllBytes(path + "Drivers\\System.Data.SQLite.dll");
            //Asm = Assembly.Load(assemblyBuffer);
            return Assembly.UnsafeLoadFrom(path + "System.Data.SQLite.dll");
        }

        public string GetDataBaseName(System.Data.Common.DbConnectionStringBuilder builder)
        {
            object database = "";
            builder.TryGetValue("Data Source", out database);
            if (database == null || string.IsNullOrEmpty(database.ToString()))
            {
                throw new Exception(ProviderName + " Data Source error!");
            }
            database = Path.GetFileName(database.ToString()).Replace(".db", "");
            return database.ToString();
        }


        public List<dynamic> GetTables(SqlHelper sqlHelper, string dataBaseName)
        {
            return
                   sqlHelper.ExecuteDynamic("select name from sqlite_master where type='table'");
        }

        public List<dynamic> GetColumns(SqlHelper sqlHelper, string dataBaseName, string tableName)
        {
            var list = new List<dynamic>();
            DbDataReader dr = sqlHelper.ExecuteReader("pragma table_info(\"" + tableName + "\")");
            while (dr.Read())
            {
                dynamic eo = new ExpandoObject();
                eo.name = dr["name"];
                eo.type = dr["type"];
                eo.isnullable = dr["notnull"];
                list.Add(eo);
            }
            dr.Close();
            dr.Dispose();
            return list;
        }

        private List<dynamic> _tableInfos = null;
        private string _lastTableName = null;
        public string GetColumnType(SqlHelper sqlHelper, string dataBaseName, string tableName, string columnName)
        {
            if (tableName != _lastTableName || _tableInfos == null)
            {
                _tableInfos = new List<dynamic>();
                DbDataReader dr = sqlHelper.ExecuteReader("pragma table_info(\"" + tableName + "\")");
                while (dr.Read())
                {
                    string type = dr["type"].ToString().ToLower();
                    if (type == "integer")
                    {
                        type = "long";
                    }
                    else if (type == "real")
                    {
                        type = "double";
                    }
                    else if (type == "text")
                    {
                        type = "string";
                    }
                    else if (type == "blob")
                    {
                        type = "byte[]";
                    }
                    else
                    {
                        type = "string";
                    }
                    dynamic tableInfo = new ExpandoObject();
                    tableInfo.columnName = dr["name"].ToString();
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
            object pk = null;
            DbDataReader dr = sqlHelper.ExecuteReader("pragma table_info(\"" + tableName + "\")");
            while (dr.Read())
            {
                if (dr["pk"].ToString() == "1")
                {
                    pk = dr["name"];
                }
            }
            dr.Close();
            dr.Dispose();
            return pk;
        }

        public System.Data.Common.DbCommand GetCmd(Assembly driverAssembly)
        {
            return (DbCommand)driverAssembly.CreateInstance("System.Data.SQLite.SQLiteCommand");
        }

        public System.Data.Common.DbConnection GetConn(Assembly driverAssembly, string dbConnectionStr)
        {
            return
                    (DbConnection)
                    driverAssembly.CreateInstance("System.Data.SQLite.SQLiteConnection", true, BindingFlags.Default, null,
                                       new object[] { dbConnectionStr }, null, null);
        }

        public System.Data.IDataAdapter GetAdapter(Assembly driverAssembly, System.Data.Common.DbCommand cmd)
        {
            return
                    (System.Data.IDataAdapter)
                    driverAssembly.CreateInstance("System.Data.SQLite.SQLiteDataAdapter", true, BindingFlags.Default, null,
                                       new object[] { cmd }, null, null);
        }
        public string GetLastInsertIdSql(string tableName)
        {
            return "select last_insert_rowid();";
        }

        public string GetLeftEscape()
        {
            return "\"";
        }

        public string GetRightEscape()
        {
            return "\"";
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
                var cols = Warp.ShieldLogSql(() => GetColumns(sqlhelper, sqlhelper.DataBaseName, tableName));
                var needRebuild = false;
                foreach (var column in columns)
                {
                    var memberType = IntrospectionManager.GetMemberType(className, column.Key);
                    var columnType = IntrospectionManager.GetColumnType(className, column.Key);
                    var columnName = IntrospectionManager.GetColumnName(className, column.Key);
                    var length = IntrospectionManager.GetColumnLength(className, column.Key);
                    var isNullAble = IntrospectionManager.GetColumnIsNullAble(className, column.Key);
                    var isPk = IntrospectionManager.GetColumnIsPrimaryKey(className, column.Key);
                    var isRealPk = IsRealPkInDb(sqlhelper, tableName, columnName);

                    var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                    if (cols.Any(x => x.name.ToUpper() == columnName.ToUpper()))
                    {
                        if (!IsNeedModiy(cols, memberType, columnName, lastColumnType, length, isNullAble, isPk, isRealPk))
                        {
                            continue;
                        }
                        needRebuild = true;
                        break;
                    }
                    else
                    {
                        needRebuild = true;
                        break;
                    }
                }
                if (needRebuild)
                {
                    DbConnection conn = sqlhelper.GetConn();
                    conn.Open();
                    DbTransaction tran = conn.BeginTransaction();
                    try
                    {
                        var oldName = RenameTable(sqlhelper, tran, tableName);

                        CreateTable(sqlhelper, tran, className, tableName);

                        DropOldTable(sqlhelper, tran,className, tableName, oldName);

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
                return columnType + "(" + length + ")";
            }

            if (type == typeof(int) || type == typeof(uint))
            {
                return "integer";
            }
            else if (type == typeof(long) || type == typeof(ulong))
            {
                return "integer";
            }
            else if (type == typeof(short) || type == typeof(ushort))
            {
                return "integer";
            }
            else if (type == typeof(decimal))
            {
                return "real";
            }
            else if (type == typeof(double))
            {
                return "real";
            }
            else if (type == typeof(float))
            {
                return "real";
            }
            else if (type == typeof(bool))
            {
                return "integer";
            }
            else if (type == typeof(byte))
            {
                return "integer";
            }
            else if (type == typeof(byte[]))
            {
                return "blob";
            }
            else if (type.IsDigital())
            {
                return "integer";
            }
            else if (type == typeof(DateTime))
            {
                return "text";
            }
            else if (type == typeof(string) && length == int.MaxValue)
            {
                return "text";
            }
            else if (type == typeof(string))
            {
                return "text(" + length + ")";
            }
            return "text(" + length + ")";
        }

        private bool IsNeedModiy(IEnumerable<dynamic> cols, Type memberType, string columnName, string columnType,
            int length, bool isNullAble, bool isPk, bool isRealPk)
        {
            if (length == 0) length = 50;

            if (isPk != isRealPk)
            {
                return true;
            }

            if (isPk)
            {
                if (memberType.IsDigital())
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0]);
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType);
                }
            }
            else
            {
                if (memberType.IsDigital())
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType.Split('(')[0] && x.isnullable == isNullAble.To01());
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type == columnType && x.isnullable == isNullAble.To01());
                }
            }
        }

        private bool IsRealPkInDb(SqlHelper sqlhelper, string tableName, string columnName)
        {
            var getconstraint = Warp.ShieldLogSql(() =>GetPrimaryKey(sqlhelper, "", tableName));
            if (getconstraint == null) return false;
            return columnName.ToUpper()== getconstraint.ToString().ToUpper();
        }

        private bool IsNeedAddTable(SqlHelper sqlhelper, string tableName)
        {
            var tables = Warp.ShieldLogSql(() => GetTables(sqlhelper, sqlhelper.DataBaseName));
            return tables.Any(x => x.name.ToUpper() == tableName.ToUpper());
        }

        public void CreateTable(SqlHelper sqlhelper,DbTransaction tran,string className,string tableName)
        {
            var columns = IntrospectionManager.GetColumns(className);

            var columnsArr = columns.Select(x =>
            {
                var memberType = IntrospectionManager.GetMemberType(className, x.Key);
                var columnType = IntrospectionManager.GetColumnType(className, x.Key);
                var length = IntrospectionManager.GetColumnLength(className, x.Key);
                var isNullAble = IntrospectionManager.GetColumnIsNullAble(className, x.Key);
                var isPk = IntrospectionManager.GetColumnIsPrimaryKey(className, x.Key);
                var nullAbleSql = isNullAble ? "" : "not null";
                nullAbleSql = isPk ? "not null" : nullAbleSql;
                var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                if (isPk)
                {
                    return string.Format("\"{0}\" {1} primary key ", x.Value, lastColumnType);
                }
                return string.Format("\"{0}\" {1} {2}", x.Value, lastColumnType, nullAbleSql);
            });
            var columnsSql = string.Join(",", columnsArr);
            var createTable = "create table \"" + tableName + "\"(" + columnsSql + ")";
            if (tran == null)
            {
                sqlhelper.ExecuteNonQuery(createTable);
            }
            else
            {
                sqlhelper.ExecuteNonQuery(tran, createTable);
            }
        }

        public string RenameTable(SqlHelper sqlhelper, DbTransaction tran, string tableName)
        {
            string oldName = tableName + DateTime.Now.ToString("yyyyMMdd_HHmmss");
            sqlhelper.ExecuteNonQuery(tran , string.Format("alter table \"{0}\" rename to \"{1}\";", tableName, oldName));
            return oldName;
        }

        public string DropOldTable(SqlHelper sqlHelper, DbTransaction tran,string className, string tableName, string oldTableName)
        {
            var columns = IntrospectionManager.GetColumns(className);
            var columnsArr = string.Join(",", columns.Values.Select(x => "\"" + x + "\""));
            sqlHelper.ExecuteNonQuery(tran , string.Format("insert into \"{0}\"({2})  select {2} from \"{1}\"", tableName, oldTableName,columnsArr));
            sqlHelper.ExecuteNonQuery(tran , string.Format("drop table \"{0}\";", oldTableName));
            return "";
        }
    }
}
