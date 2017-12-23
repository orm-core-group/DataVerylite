using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using DataVeryLite.Core;
using DataVeryLite.Util;

namespace DataVeryLite.Providers
{
    class PostgreSqlProvider : IProvider
    {
        public string ProviderName
        {
            get { return DataBaseNames.PostgreSql; }
        }

        public System.Reflection.Assembly GetAssembly(string path)
        {
            Assembly.LoadFrom(path + "Mono.Security.dll");
            return Assembly.LoadFrom(path + "Npgsql.dll");
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
                throw new Exception(ProviderName + " must have database;"+builder.ConnectionString);
            }
            return database.ToString();
        }

        public List<dynamic> GetTables(Util.SqlHelper sqlHelper, string dataBaseName)
        {
            return
                sqlHelper.ExecuteDynamic(
                    "select tablename as name from pg_tables where tablename not like 'pg_%' and tablename not like 'sql_%' order by tablename");
        }

        public List<dynamic> GetColumns(Util.SqlHelper sqlHelper, string dataBaseName, string tableName)
        {
            return sqlHelper.ExecuteDynamic(string.Format("select column_name as name,data_type as type,character_maximum_length length,is_nullable as isnullable from  information_schema.columns where table_name='{0}'", tableName));
        }

        private List<dynamic> _tableInfos = null;
        private string _lastTableName = null;
        public string GetColumnType(Util.SqlHelper sqlHelper, string dataBaseName, string tableName, string columnName)
        {
            if (tableName != _lastTableName || _tableInfos == null)
            {
                _tableInfos = new List<dynamic>();
                DbDataReader dr =
                    sqlHelper.ExecuteReader(string.Format("SELECT format_type(a.atttypid,a.atttypmod) as data_type,a.attname as column_name from pg_class as c,pg_attribute as a where c.relname = '{0}' and a.attrelid = c.oid and a.attnum>0 ",tableName));
                while (dr.Read())
                {
                    string type = dr["data_type"].ToString().ToLower();

                    #region typedef
                    //cidr,inet =ipv4 or ipv6 / interval timespan
                    if (type == "macaddr" || type == "interval" || type == "inet" || type == "cidr" 
                        || type == "bit" || type == "bit varying" || type == "character" 
                        || type == "text" || type == "character varying")
                    {
                        type = "string";
                    }
                    else if (type == "real")
                    {
                        type = "float";
                    }
                    else if (type == "double precision")
                    {
                        type = "double";
                    }
                    else if (type.Contains("numeric") || type == "money")
                    {
                        type = "decimal";
                    }
                    else if (type == "bigint" || type == "bigserial")
                    {
                        type = "long";
                    }
                    else if (type == "integer" || type == "serial")
                    {
                        type = "int";
                    }
                    else if (type == "smallint")
                    {
                        type = "short";
                    }
                    else if (type == "boolean")
                    {
                        type = "bool";
                    }
                    else if (type == "date" || type.Contains("time"))
                    {
                        type = "DateTime";
                    }
                    else if (type == "bytea")
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

        public object GetPrimaryKey(Util.SqlHelper sqlHelper, string dataBaseName, string tableName)
        {
            return
                sqlHelper.ExecuteScalar(string.Format(
                    "select pg_attribute.attname as name from pg_constraint  inner join pg_class on pg_constraint.conrelid = pg_class.oid inner join pg_attribute on pg_attribute.attrelid = pg_class.oid and  pg_attribute.attnum = pg_constraint.conkey[1] inner join pg_type on pg_type.oid = pg_attribute.atttypid where pg_class.relname = '{0}' and pg_constraint.contype='p'",tableName));
        }

        public System.Data.Common.DbCommand GetCmd(System.Reflection.Assembly driverAssembly)
        {
            return (DbCommand)driverAssembly.CreateInstance("Npgsql.NpgsqlCommand");
        }

        public System.Data.Common.DbConnection GetConn(System.Reflection.Assembly driverAssembly, string dbConnectionStr)
        {
            return
                   (DbConnection)
                   driverAssembly.CreateInstance("Npgsql.NpgsqlConnection", true, BindingFlags.Default, null,
                                      new object[] { dbConnectionStr }, null, null);
        }

        public System.Data.IDataAdapter GetAdapter(System.Reflection.Assembly driverAssembly, System.Data.Common.DbCommand cmd)
        {
            return (System.Data.IDataAdapter)
                     driverAssembly.CreateInstance("Npgsql.NpgsqlDataAdapter", true, BindingFlags.Default, null,
                                        new object[] { cmd }, null, null);
        }
        public string GetLastInsertIdSql(string tableName)
        {
            return "select lastval();";
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
                sql = string.Format("select * from {0} LIMIT {2} OFFSET {1}", _left + talbeName + _right, (page - 1) * pageSize,
                                        pageSize);
            }
            else
            {
                sql = string.Format("select * from {0} order by {1} desc limit {3} OFFSET {2}", _left + talbeName + _right,
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
                #region Alter
                var columns = IntrospectionManager.GetColumnAttributes(className).OrderByDescending(x => x.Value.IsPrimaryKey);
                var cols = Warp.ShieldLogSql(() => GetColumns(sqlhelper, sqlhelper.DataBaseName, tableName));

                var needRebuild = false;

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

                    var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                    if (cols.Any(x => x.name.ToUpper() == columnName.ToUpper()))
                    {
                        if (!IsNeedModiy(cols, memberType, columnName, lastColumnType, length, isNullAble, isPk, isRealPk, isAutoGrow,isRealAutoGrow))
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

                    #endregion
                }
                if (needRebuild)
                {
                    ModiyTable(sqlhelper, className, tableName);
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

            if (type == typeof(int) || type == typeof(uint))
            {
                return "integer";
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
                return "numeric";
            }
            else if (type == typeof(double))
            {
                return "double precision";
            }
            else if (type == typeof(float))
            {
                return "real";
            }
            else if (type == typeof(bool))
            {
                return "boolean";
            }
            else if (type == typeof(byte))
            {
                return "smallint";
            }
            else if (type == typeof(byte[]))
            {
                return "bytea";
            }
            else if (type.IsDigital())
            {
                return "integer";
            }
            else if (type == typeof(DateTime))
            {
                return "date";
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
                "integer",
                "numeric",
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
                "varbinary","bytea","boolean","double precision"
            };

        private bool IsNeedModiy(IEnumerable<dynamic> cols, Type memberType, string columnName, string columnType,
            int length, bool isNullAble, bool isPk, bool isRealPk,bool isAutoGrow,bool isRealAutoGrow)
        {
            if (length == 0) length = 50;

            if (isAutoGrow != isRealAutoGrow)
            {
                return true;
            }

            if (isPk != isRealPk)
            {
                return true;
            }

            if (isPk || isAutoGrow)
            {
                if (_filterType.Contains(columnType.ToLower()))
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && (x.type == "character varying" ? "varchar" : x.type)== columnType.Split('(')[0]);
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && (x.type == "character varying" ? "varchar" : x.type) == columnType.Split('(')[0] && x.length == length.ToString());
                }
            }
            else
            {
                if (_filterType.Contains(columnType.ToLower()))
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && (x.type == "character varying" ? "varchar" : x.type) == columnType.Split('(')[0] && x.isnullable == isNullAble.ToYesNo());
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && (x.type == "character varying" ? "varchar" : x.type) == columnType.Split('(')[0] && x.length == length.ToString() && x.isnullable == isNullAble.ToYesNo());
                }
            }
        }

        private bool IsRealPkInDb(SqlHelper sqlhelper, string tableName, string columnName)
        {
            var pkName = Warp.ShieldLogSql(() =>GetPrimaryKey(sqlhelper, "", tableName));
            if (pkName == null) return false;
            return columnName == pkName.ToString();
        }

        private bool IsRealAutoGrow(SqlHelper sqlHelper, string tableName, string columnName)
        {
            var getconstraint = string.Format("select column_default from  information_schema.columns where table_name='{0}' and column_name='{1}'", tableName, columnName);
            var constraints = Warp.ShieldLogSql(() => sqlHelper.ExecuteDynamic(getconstraint));
            if (constraints.Any())
            {
                var columnDefault = constraints.FirstOrDefault().column_default;
                if (columnDefault == string.Format("nextval('\"{0}_{1}_seq\"'::regclass)", tableName, columnName))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        private bool IsNeedAddTable(SqlHelper sqlhelper, string tableName)
        {
            var tables = Warp.ShieldLogSql(() => GetTables(sqlhelper, sqlhelper.DataBaseName));
            return tables.Any(x => x.name.ToUpper() == tableName.ToUpper());
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
                var isPk = IntrospectionManager.GetColumnIsPrimaryKey(className, x.Key);
                var isNullAble = IntrospectionManager.GetColumnIsNullAble(className, x.Key);
                var nullAbleSql = isNullAble ? "" : "not null";
                nullAbleSql = isPk || isAutoGrow ? " not null" : nullAbleSql;
                var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                if (memberType == typeof (long) || memberType == typeof (ulong))
                {
                    lastColumnType = isAutoGrow ? "bigserial" : lastColumnType;
                }
                else
                {
                    lastColumnType = isAutoGrow ? "serial" : lastColumnType;
                }
                if (isPk)
                {
                    return string.Format("{0} {1} primary key", "\"" + x.Value + "\"", lastColumnType);
                }
                return "\"" + x.Value + "\" " + lastColumnType + " " + nullAbleSql;
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

        private void ModiyTable(SqlHelper sqlhelper, string className, string tableName)
        {
            var newTableName = tableName + DateTime.Now.ToString("yyyyMMdd_HHmmss");

            DbConnection conn = sqlhelper.GetConn();
            conn.Open();
            DbTransaction tran = conn.BeginTransaction();
            try
            {
                CreateTable(sqlhelper, tran, className, newTableName);
                //var columns = IntrospectionManager.GetColumns(className);
                //var columnsArr = string.Join(",", columns.Values.Select(x => "\"" + x + "\""));
                var cols = Warp.ShieldLogSql(() => GetColumns(sqlhelper, sqlhelper.DataBaseName, tableName));
                var columnsArr = string.Join(",", cols.Select(x => "\"" + x.name + "\""));
                sqlhelper.ExecuteNonQuery(tran, string.Format("insert into \"{0}\"({2}) select {2} from \"{1}\"", newTableName, tableName, columnsArr));
                sqlhelper.ExecuteNonQuery(tran, "drop table \"" + tableName + "\"");
                sqlhelper.ExecuteNonQuery(tran,string.Format("alter table \"{0}\" rename to \"{1}\"", newTableName, tableName));
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

            //var pkName = Warp.ShieldLogSql(() => GetPrimaryKey(sqlhelper, "", tableName));
            var autoGrowName= IntrospectionManager.GetAutoGrowColumnName(className);
            if (!string.IsNullOrWhiteSpace(autoGrowName))
            {
                sqlhelper.ExecuteNonQuery(string.Format("alter sequence \"{0}_{1}_seq\" rename to \"{2}_{1}_seq\"", newTableName, autoGrowName, tableName));
            }
        }
    }
}
