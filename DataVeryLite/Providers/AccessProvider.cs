using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Data;
using System.Dynamic;
using System.Data.Common;
using DataVeryLite.Core;
using DataVeryLite.Util;

namespace DataVeryLite.Providers
{
    class AccessProvider:IProvider
    {
        public string ProviderName
        {
            get { return DataBaseNames.Access; }
        }

        public System.Reflection.Assembly GetAssembly(string path)
        {
            return Assembly.GetAssembly(typeof(System.Data.OleDb.OleDbConnection));
        }

        public string GetDataBaseName(System.Data.Common.DbConnectionStringBuilder builder)
        {
            object database = "";
            builder.TryGetValue("Data Source", out database);
            if (database == null || string.IsNullOrEmpty(database.ToString()))
            {
                throw new Exception(ProviderName+" Data Source error!");
            }
            database = Path.GetFileName(database.ToString()).Replace(".mdb", "");
            return database.ToString();
        }


        public List<dynamic> GetTables(SqlHelper sqlHelper, string dataBaseName)
        {
            var list = new List<dynamic>();
            var con = new System.Data.OleDb.OleDbConnection(sqlHelper.DbConnectionString);
            con.Open();
            DataTable dt = con.GetSchema("tables");
            foreach (DataRow dr in dt.Rows)
            {
                if (dr["TABLE_TYPE"].ToString() == "TABLE")
                {
                    dynamic eo = new ExpandoObject();
                    eo.name = dr["TABLE_NAME"].ToString().FirstLetterToUpper();
                    list.Add(eo);
                }
            }
            con.Close();
            con.Dispose();
            return list;
        }

        public List<dynamic> GetColumns(SqlHelper sqlHelper, string dataBaseName, string tableName)
        {
            var list = new List<dynamic>();
            var con = new System.Data.OleDb.OleDbConnection(sqlHelper.DbConnectionString);
            con.Open();
            DataTable dt = con.GetSchema("columns", new string[] { null, null, tableName });
            foreach (DataRow dr in dt.Rows)
            {
                dynamic eo = new ExpandoObject();
                eo.name = dr["COLUMN_NAME"].ToString().FirstLetterToUpper();
                eo.type = dr["data_type"].ToString();
                eo.length = dr["CHARACTER_MAXIMUM_LENGTH"].ToString();
                eo.isnullable = dr["is_nullable"].ToString();
                list.Add(eo);
            }
            con.Close();
            con.Dispose();
            return list;
        }

        private List<dynamic> _tableInfos = null;
        private string _lastTableName = null;
        public string GetColumnType(SqlHelper sqlHelper, string dataBaseName, string tableName, string columnName)
        {
            if (tableName != _lastTableName || _tableInfos == null)
            {
                _tableInfos = new List<dynamic>();
                var list = new List<dynamic>();
                var con = new System.Data.OleDb.OleDbConnection(sqlHelper.DbConnectionString);
                con.Open();
                DataTable dt = con.GetSchema("columns", new string[] { null, null, tableName });
                foreach (DataRow dr in dt.Rows)
                {
                    string type = dr["data_type"].ToString().ToLower();
                    //2 SmallInt,
                    if (type == "2")
                    {
                        type = "short";
                    }
                    //3 int,
                    else if (type == "3")
                    {
                        type = "int";
                    }
                    //4 real,
                    else if (type == "4")
                    {
                        type = "float";
                    }
                    //5 float
                    else if (type == "5")
                    {
                        type = "double";
                    }
                    //6 Money,131 Decimal
                    else if (type == "6" || type == "131")
                    {
                        type = "decimal";
                    }
                    //7 DateTime,13 TimeStamp,133 DateTime,135 SmallDateTime
                    else if (type == "7" || type == "13" || type == "133" || type == "135")
                    {
                        type = "DateTime";
                    }
                    //11 bit
                    else if (type == "11")
                    {
                        type = "bool";
                    }
                    //17 TinyInt
                    else if (type == "17")
                    {
                        type = "byte";
                    }
                    //128 Binary,204 Binary,205 Image
                    else if (type == "128" || type == "204" || type == "205")
                    {
                        type = "byte[]";
                    }//129 Char,130 NChar,200 VarChar,201 Text,202 VarChar,203 Text
                    else if (type == "129" || type == "130" || type == "200" || type == "201" || type == "202" || type == "203")
                    {
                        type = "string";
                    }
                    else
                    {
                        type = "string";
                    }
                    dynamic tableInfo = new ExpandoObject();
                    tableInfo.columnName = dr["column_name"].ToString();
                    tableInfo.type = type;
                    _tableInfos.Add(tableInfo);
                }
                con.Close();
                con.Dispose();
            }
            _lastTableName = tableName;
            foreach (dynamic item in _tableInfos)
            {
                if (item.columnName == columnName.ToLower())
                {
                    return item.type;
                }
            }
            return "string";
        }

        public object GetPrimaryKey(SqlHelper sqlHelper, string dataBaseName, string tableName)
        {
            var con = new System.Data.OleDb.OleDbConnection(sqlHelper.DbConnectionString);
            con.Open();
            var schemaTable = con.GetOleDbSchemaTable(System.Data.OleDb.OleDbSchemaGuid.Primary_Keys,
                                    new Object[] { null, null, tableName });
            con.Close();
            if (schemaTable == null||schemaTable.Rows.Count == 0)
            {
                return "";
            }
            object pk = null;
            pk = schemaTable.Rows[0].ItemArray[3].ToString().FirstLetterToUpper();
            return pk;
        }

        public DbCommand GetCmd(Assembly driverAssembly)
        {
            return (DbCommand)driverAssembly.CreateInstance("System.Data.OleDb.OleDbCommand");
        }

        public DbConnection GetConn(Assembly driverAssembly, string dbConnectionStr)
        {
            return
                    (DbConnection)
                    driverAssembly.CreateInstance("System.Data.OleDb.OleDbConnection", true, BindingFlags.Default, null,
                                       new object[] { dbConnectionStr }, null, null);
        }

        public IDataAdapter GetAdapter(Assembly driverAssembly, DbCommand cmd)
        {
            return
                     (IDataAdapter)
                     driverAssembly.CreateInstance("System.Data.OleDb.OleDbDataAdapter", true, BindingFlags.Default, null,
                                        new object[] { cmd }, null, null);
        }


        public string GetLastInsertIdSql(string tableName)
        {
            return "select @@identity;";
        }

        public string GetLeftEscape()
        {
            return "[";
        }

        public string GetRightEscape()
        {
            return "]";
        }


        public string GetPageSql(int page, int pageSize, bool asc,string talbeName,string idColumnName)
        {
            string _left = GetLeftEscape();
            string _right = GetRightEscape();
            string sql;
            if (asc)
            {
                sql = string.Format(
                        "select top {2} * from {0} where {1} not in(select top {3} {1} from {0} order by {1})order by {1}", _left + talbeName + _right,
                        _left + idColumnName + _right, pageSize, page * pageSize);
            }
            else
            {
                sql = string.Format(
                        "select top {2} * from {0} where {1} not in(select top {3} {1} from {0} order by {1} desc) order by {1} desc", _left + talbeName + _right,
                        _left + idColumnName + _right, pageSize, page * pageSize);
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
            get { return false; }
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
                    DbConnection conn = sqlhelper.GetConn();
                    conn.Open();
                    DbTransaction tran = conn.BeginTransaction();
                    try
                    {
                        var newTableName = tableName + DateTime.Now.ToString("yyyyMMdd_HHmmss");
                        ModiyTable(sqlhelper, tran, className, newTableName, tableName);
                        ModiyTable(sqlhelper, tran, className, tableName, newTableName);
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
                    return;
                }

                foreach (var column in columns)
                {
                    var memberType = IntrospectionManager.GetMemberType(className, column.Key);
                    var columnType = IntrospectionManager.GetColumnType(className, column.Key);
                    var columnName = IntrospectionManager.GetColumnName(className, column.Key);
                    var length = IntrospectionManager.GetColumnLength(className, column.Key);
                    var isAutoGrow = IntrospectionManager.GetColumnIsAutoGrow(className, column.Key);
                    //var isRealAutoGrow = IsRealAutoGrow(sqlhelper, tableName, columnName);
                    var isNullAble = IntrospectionManager.GetColumnIsNullAble(className, column.Key);
                    var isPk = IntrospectionManager.GetColumnIsPrimaryKey(className, column.Key);
                    var isRealPk = IsRealPkInDb(sqlhelper, tableName, columnName);

                    var autoGrowSql = isAutoGrow ? "autoincrement" : "";
                    var nullAbleSql = isNullAble ? " null" : " not null";
                    nullAbleSql = isPk ? " not null" : nullAbleSql;
                    var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                    if (cols.Any(x => x.name.ToUpper() == columnName.ToUpper()))
                    {
                        if (!IsNeedModiy(cols, memberType, columnName.ToUpper(), lastColumnType, length, isNullAble, isPk, isRealPk))
                        {
                            continue;
                        }
                        if (isPk != isRealPk)
                        {
                            var constrintName = GetPrimaryKeyContrantName(sqlhelper, "", tableName);
                            var dropconstraint = "alter table [" + tableName + "] drop constraint [" + constrintName + "]";
                            sqlhelper.ExecuteNonQuery(dropconstraint);
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
                        if (isPk)
                        {
                            var constrintName = GetPrimaryKeyContrantName(sqlhelper, "", tableName);
                            var dropconstraint = "alter table [" + tableName + "] drop constraint [" + constrintName+"]";
                            sqlhelper.ExecuteNonQuery(dropconstraint);
                        }
                        var alterSql = string.Format("alter table [{0}] add [{1}] {2} {3} {4}", tableName, columnName, lastColumnType, nullAbleSql, autoGrowSql);
                        sqlhelper.ExecuteNonQuery(alterSql);
                        if (isPk)
                        {
                            alterSql = string.Format("alter table [{0}] add constraint [{1}] primary key([{2}])", tableName, "pk_" + tableName + "_" + columnName, columnName);
                            sqlhelper.ExecuteNonQuery(alterSql);
                        }

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
                return "int";
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
                return "float";
            }
            else if (type == typeof(float))
            {
                return "real";
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
                return "varbinary";
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
                return "text(" + length + ")";
            }
            return "text(" + length + ")";
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
            int length, bool isNullAble, bool isPk, bool isRealPk)
        {
            if (length == 0) length = 50;

            if (isPk != isRealPk)
            {
                return true;
            }

            if (isPk || memberType == typeof(bool))
            {
                if (columnType.Split('(')[0] == "ntext")
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName && x.type == "130");
                }
                else if (_filterType.Contains(columnType.ToLower()))
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName && GetColumnTypeById(x.type) == columnType.Split('(')[0]);
                }
                else if (columnType.Split('(')[0] == "nchar" || columnType.Split('(')[0] == "nvarchar")
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName && x.type == "130" && x.length == length.ToString());
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName && GetColumnTypeById(x.type) == columnType.Split('(')[0] && x.length == length.ToString());
                }
            }
            else
            {
                
                if (columnType.Split('(')[0] == "ntext")
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName && x.type == "130" && x.isnullable == isNullAble.ToTrueFalse());
                }
                else if ( _filterType.Contains(columnType.ToLower()))
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName && GetColumnTypeById(x.type) == columnType.Split('(')[0] && x.isnullable == isNullAble.ToTrueFalse());
                }
                else if (columnType.Split('(')[0] == "nchar" || columnType.Split('(')[0] == "nvarchar" || columnType.Split('(')[0] == "varchar")
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName && x.type == "130" && x.length == length.ToString() && x.isnullable == isNullAble.ToTrueFalse());
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName && GetColumnTypeById(x.type) == columnType.Split('(')[0] && x.length == length.ToString() && x.isnullable == isNullAble.ToTrueFalse());
                }
            }
        }

        private bool IsRealPkInDb(SqlHelper sqlhelper, string tableName, string columnName)
        {
            var getconstraint = GetPrimaryKey(sqlhelper, "", tableName);
            return getconstraint.ToString().ToUpper() == columnName.ToUpper();
        }

        private bool IsRealAutoGrow(SqlHelper sqlHelper, string tableName, string columnName)
        {
            var con = new OleDbConnection(sqlHelper.DbConnectionString);
            con.Open();
            var adapter = new OleDbDataAdapter("select * from [" + tableName + "] where false", con);
            DataTable dtSchema = adapter.FillSchema(new DataTable(), SchemaType.Source);
            con.Close();
            con.Dispose();
            if (dtSchema == null)
            {
                return false;
            }
            for (int i = 0; i < dtSchema.Columns.Count; i++)
            {
                if (dtSchema.Columns[i].AutoIncrement && dtSchema.Columns[i].ColumnName.ToUpper()==columnName.ToUpper())
                {
                    return true;
                }
            }
            return false;
        }

        private bool IsNeedAddTable(SqlHelper sqlhelper, string tableName)
        {
            var tables = Warp.ShieldLogSql(() => GetTables(sqlhelper, sqlhelper.DataBaseName));
            return tables.Any(x => x.name.ToUpper() == tableName.ToUpper());
        }

        private string GetColumnTypeById(string type)
        {
            //2 SmallInt,
            if (type == "2")
            {
                type = "smallint";
            }
            //3 int,
            else if (type == "3")
            {
                type = "int";
            }
            //4 real,
            else if (type == "4")
            {
                type = "real";
            }
            //5 float
            else if (type == "5")
            {
                type = "float";
            }
            //6 Money
            else if (type == "6")
            {
                type = "money";
            }
            //131 Decimal
            else if (type == "131")
            {
                type = "decimal";
            }
            //7 DateTime
            else if (type == "7" )
            {
                type = "datetime";
            } //13 TimeStamp
            else if (type == "13")
            {
                type = "timestamp";
            } //133 DateTime
            else if (type == "133")
            {
                type = "datetime";
            } //135 SmallDateTime
            else if (type == "135")
            {
                type = "smalldatetime";
            }
            //11 bit
            else if (type == "11")
            {
                type = "bit";
            }
            //17 TinyInt
            else if (type == "17")
            {
                type = "tinyint";
            }
            //128 Binary
            else if (type == "128")
            {
                type = "varbinary";
            }
            //204 Binary
            else if (type == "204")
            {
                type = "binary";
            }
            //205 Image
            else if (type == "205")
            {
                type = "image";
            }//129 Char
            else if (type == "129")
            {
                type = "char";
            }//130 NChar
            else if (type == "130")
            {
                type = "text";
            }//200 VarChar
            else if (type == "200")
            {
                type = "varchar";
            }//201 Text
            else if (type == "201")
            {
                type = "text";
            }//202 VarChar
            else if (type == "202")
            {
                type = "varchar";
            }//203 Text
            else if (type == "203")
            {
                type = "text";
            }
            else
            {
                type = "text";
            }
            return type;
        }

        private object GetPrimaryKeyContrantName(SqlHelper sqlHelper, string dataBaseName, string tableName)
        {
            var con = new System.Data.OleDb.OleDbConnection(sqlHelper.DbConnectionString);
            con.Open();
            var schemaTable = con.GetOleDbSchemaTable(System.Data.OleDb.OleDbSchemaGuid.Primary_Keys,
                                    new Object[] { null, null, tableName });
            con.Close();
            object pk = null;
            pk = schemaTable == null ? null : schemaTable.Rows[0].ItemArray[7].ToString();
            return pk ?? "";
        }

        public void CreateTable(SqlHelper sqlhelper, DbTransaction tran, string className, string tableName)
        {
            var columns = IntrospectionManager.GetColumns(className);
            var columnsArr = columns.Select(x =>
            {
                var memberType = IntrospectionManager.GetMemberType(className, x.Key);
                var columnType = IntrospectionManager.GetColumnType(className, x.Key);
                var length = IntrospectionManager.GetColumnLength(className, x.Key);
                var isAutoGrow = IntrospectionManager.GetColumnIsAutoGrow(className, x.Key);
                var isNullAble = IntrospectionManager.GetColumnIsNullAble(className, x.Key);
                var isPk = IntrospectionManager.GetColumnIsPrimaryKey(className, x.Key);
                var autoGrowSql = isAutoGrow ? "autoincrement" : "";
                var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                lastColumnType = isAutoGrow ? "" : lastColumnType;
                var nullAbleSql = isNullAble ? "null" : "not null";
                nullAbleSql = isPk ? "not null" : nullAbleSql;
                if (isPk)
                {
                    return string.Format("[{0}] {1} {2} primary key {3}", x.Value, lastColumnType, autoGrowSql, nullAbleSql);
                }
                return string.Format("[{0}] {1} {2} {3}", x.Value, lastColumnType, autoGrowSql, nullAbleSql);
            });
            var columnsSql = string.Join(",", columnsArr);
            var createTable = "create table [" + tableName + "](" + columnsSql + ");";
            if (tran == null)
            {
                sqlhelper.ExecuteNonQuery(createTable);
            }
            else
            {
                sqlhelper.ExecuteNonQuery(tran, createTable);
            }
        }

        public void ModiyTable(SqlHelper sqlhelper, DbTransaction tran, string className, string newTableName,string oldTableName)
        {
            var columns = IntrospectionManager.GetColumns(className);
            var columnsArr = string.Join(",", columns.Values.Select(x => "[" + x + "]"));

            CreateTable(sqlhelper, tran, className, newTableName);
            var sql = string.Format("insert into [{0}]({1}) select {1} from [{2}];", newTableName, columnsArr,oldTableName);
            sqlhelper.ExecuteNonQuery(tran, sql);
            sqlhelper.ExecuteNonQuery(tran, "drop table [" + oldTableName + "]");

        }
    }
}
