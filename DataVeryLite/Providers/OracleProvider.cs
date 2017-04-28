using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using DataVeryLite.Core;
using DataVeryLite.Util;

namespace DataVeryLite.Providers
{
    class OracleProvider:IProvider
    {
        public string ProviderName
        {
            get { return DataBaseNames.Oracle; }
        }

        public System.Reflection.Assembly GetAssembly(string path)
        {
            return Assembly.LoadFrom(path + "Oracle.ManagedDataAccess.dll");
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
                if (database != null)
                {
                    if (database.ToString().Contains("/"))
                    {
                        return database.ToString().Split('/')[1];
                    }
                }
            }
            if (database == null || string.IsNullOrEmpty(database.ToString()))
            {
                throw new Exception(ProviderName + " must have databse");
            }
            return database.ToString().FirstLetterToUpper();
        }

        private string getUser(string connStr)
        {
            var builder = new DbConnectionStringBuilder();
            builder.ConnectionString = connStr.ToUpper();
            object user = "";
            builder.TryGetValue("USER ID", out user);
            if (user == null || string.IsNullOrEmpty(user.ToString()))
            {
                builder.TryGetValue("UID", out user);
            }
            if (user == null || string.IsNullOrEmpty(user.ToString()))
            {
                throw new Exception(ProviderName + " must have User ID");
            }
            return user.ToString();
        }
        public List<dynamic> GetTables(Util.SqlHelper sqlHelper, string dataBaseName)
        {
            var list = new List<dynamic>();
            var dr = sqlHelper.ExecuteReader("select TABLE_NAME name from user_tables");
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

        public List<dynamic> GetColumns(Util.SqlHelper sqlHelper, string dataBaseName, string tableName)
        {
            var list = new List<dynamic>();
            var dr = sqlHelper.ExecuteReader("select COLUMN_NAME name, DATA_TYPE type,DATA_LENGTH length,NULLABLE isnullable from all_tab_columns where upper(table_name)=upper('" + tableName + "') and owner='" + getUser(sqlHelper.DbConnectionString) + "'");
            while (dr.Read())
            {
                dynamic eo = new ExpandoObject();
                eo.name = dr["name"].ToString().FirstLetterToUpper();
                eo.type = dr["type"].ToString();
                eo.length = dr["length"].ToString();
                eo.isnullable = dr["isnullable"].ToString();
                list.Add(eo);
            }
            dr.Close();
            dr.Dispose();
            return list;
        }

        private List<dynamic> _tableInfos = null;
        private string _lastTableName = null;
        public string GetColumnType(Util.SqlHelper sqlHelper, string dataBaseName, string tableName, string columnName)
        {

            if (tableName != _lastTableName || _tableInfos == null)
            {
                _tableInfos = new List<dynamic>();
                var dr =
                    sqlHelper.ExecuteReader("select column_name name,data_type typename from all_tab_columns where upper(table_name)=upper('" + tableName + "') and owner='" + getUser(sqlHelper.DbConnectionString) + "'");
                while (dr.Read())
                {
                    string type = dr["typename"].ToString().ToLower();

                    #region typedef

                    if (type == "varchar" || type == "char" || type == "varchar2")
                    {
                        type = "string";
                    }
                    else if (type == "float" || type == "integer"||type=="number")
                    {
                        type = "decimal";
                    }
                    else if (type == "interval year to month")
                    {
                        type = "int";
                    }
                    else if (type == "date"||type == "timestamp")
                    {
                        type = "DateTime";
                    }
                    else if (type == "interval day to second")
                    {
                        type = "TimeSpan";
                    }
                    else if (type == "bfile" || type == "blob" || type == "long raw" || type == "raw")
                    {
                        type = "byte[]";
                    }
                    else
                    {
                        type = "string";
                    }

                    #endregion

                    dynamic tableInfo = new ExpandoObject();
                    tableInfo.columnName = dr["name"].ToString().FirstLetterToUpper();
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
            object pk = sqlHelper.ExecuteScalar("select column_name from user_cons_columns where constraint_name=(select constraint_name from user_constraints where upper(table_name)=upper('"+tableName+"') and constraint_type='P')");
            return pk == null ? null : pk.ToString().FirstLetterToUpper();
        }

        public System.Data.Common.DbCommand GetCmd(System.Reflection.Assembly driverAssembly)
        {
            return (DbCommand)driverAssembly.CreateInstance("Oracle.ManagedDataAccess.Client.OracleCommand");
        }

        public System.Data.Common.DbConnection GetConn(System.Reflection.Assembly driverAssembly, string dbConnectionStr)
        {
            return
                   (DbConnection)
                   driverAssembly.CreateInstance("Oracle.ManagedDataAccess.Client.OracleConnection", true, BindingFlags.Default, null,
                                      new object[] { dbConnectionStr }, null, null);
        }

        public System.Data.IDataAdapter GetAdapter(System.Reflection.Assembly driverAssembly, System.Data.Common.DbCommand cmd)
        {
            return (System.Data.IDataAdapter)
                     driverAssembly.CreateInstance("Oracle.ManagedDataAccess.Client.OracleDataAdapter", true, BindingFlags.Default, null,
                                        new object[] { cmd }, null, null);
        }

        public string GetLastInsertIdSql(string tableName)
        {
            return "select \"SEQ_" + tableName + "\".currval from dual";
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
                sql = string.Format(
                        "select * from {0} where rowid in(select rid from (select rownum rn,rid from(select rowid rid,{1} from {0}  order by {1} ) where rownum<={3}) where rn>{2}) order by {1}", _left + talbeName + _right,
                        _left + idColumnName + _right, (page - 1) * pageSize, page * pageSize);
            }
            else
            {
                sql = string.Format(
                        "select * from {0} where rowid in(select rid from (select rownum rn,rid from(select rowid rid,{1} from {0}  order by {1} desc) where rownum<={3}) where rn>{2}) order by {1} desc", _left + talbeName + _right,
                        _left + idColumnName + _right, (page - 1) * pageSize, page * pageSize);
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
                sql = string.Format("select  * from {0} where rownum<={1} order by {2}", _left + talbeName + _right, top, _left + idColumnName + _right);
            }
            else
            {
                sql = string.Format("select * from {0} where rownum<={1} order by {2} desc", _left + talbeName + _right, top, _left + idColumnName + _right);
            }
            return sql;
        }

        public bool IsSupportBatch
        {
            get { return true; }
        }

        public string BatchBegin
        {
            get { return "Begin "; }
        }

        public string BatchEnd
        {
            get { return " End;"; }
        }


        public void SyncDbInfo(string key, string className, string tableName)
        {
            var sqlhelper = IntrospectionManager.GetSqlHelperByKey(key);


            if (IsNeedAddTable(sqlhelper, tableName))
            {
                #region Alter
                var columns = IntrospectionManager.GetColumnAttributes(className).OrderByDescending(x => x.Value.IsPrimaryKey);
                var cols = Warp.ShieldLogSql(() => GetColumns(sqlhelper, sqlhelper.DataBaseName, tableName));

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

                    var nullAbleSql = isNullAble ? " null" : " not null";
                    nullAbleSql = isPk || isRealAutoGrow ? " not null" : nullAbleSql;
                    var lastColumnType = GetColumnTypeByMebmberType(memberType, columnType.ToLower(), length);
                    if (cols.Any(x => x.name.ToUpper() == columnName.ToUpper()))
                    {
                        if (!IsNeedModiy(cols, memberType, columnName, lastColumnType, length, isNullAble, isPk, isRealPk, isAutoGrow,isRealAutoGrow))
                        {
                            continue;
                        }
                        if (isPk != isRealPk)
                        {
                            var getconstraint = string.Format("select constraint_name name from user_cons_columns where constraint_name=(select constraint_name from user_constraints where upper(table_name)=upper('{0}') and constraint_type='P')", tableName);
                            var constraints = Warp.ShieldLogSql(() => sqlhelper.ExecuteDynamic(getconstraint));
                            foreach (var constraint in constraints)
                            {
                                var dropconstraint = "alter table \"" + tableName + "\" drop constraint \"" + constraint.NAME + "\"";
                                sqlhelper.ExecuteNonQuery(dropconstraint);
                            }
                        }

                        if (isPk && isPk != isRealPk)
                        {
                            var alterSql = string.Format("alter table \"{0}\" modify(\"{1}\" {2})", tableName, columnName, lastColumnType);
                            sqlhelper.ExecuteNonQuery(alterSql);
                       
                            alterSql = string.Format("alter table \"{0}\" add constraint \"{1}\" primary key(\"{2}\")", tableName, "pk_" + tableName + "_" + columnName, columnName);
                            sqlhelper.ExecuteNonQuery(alterSql);
                        }
                        else
                        {
                            var colsNew = Warp.ShieldLogSql(() => GetColumns(sqlhelper, sqlhelper.DataBaseName, tableName));
                            var isNullableChange = !colsNew.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.isnullable == isNullAble.ToYN());

                            var alterSql = string.Format("alter table \"{0}\" modify(\"{1}\" {2})", tableName, columnName, lastColumnType);
                            sqlhelper.ExecuteNonQuery(alterSql);
                            if (!isPk&&isNullableChange)
                            {
                                alterSql = string.Format("alter table \"{0}\" modify(\"{1}\" {2})", tableName, columnName, nullAbleSql);
                                sqlhelper.ExecuteNonQuery(alterSql);
                            }
                        }

                        if (isAutoGrow != isRealAutoGrow)
                        {
                            var getconstraint =string.Format(
                                    "select sequence_name,increment_by,last_number from user_sequences where upper(sequence_name)=upper('{0}')",
                                    "SEQ_" + tableName);
                            var constraints = Warp.ShieldLogSql(() => sqlhelper.ExecuteDynamic(getconstraint));
                            if (!constraints.Any())
                            {
                                var addconstraint = string.Format("create sequence \"SEQ_{0}\" increment by 1 start with 1 nomaxvalue nocycle", tableName);
                                sqlhelper.ExecuteNonQuery(addconstraint);
                            }
                            var gettriggers = string.Format("select trigger_name from user_triggers where table_name='{0}' and trigger_name='{1}'", tableName, "TRIGGER_" + tableName + "_" + columnName);
                            var triggers = Warp.ShieldLogSql(() => sqlhelper.ExecuteDynamic(gettriggers));
                            if (isAutoGrow &&!triggers.Any())
                            {
                                var createtriggers = string.Format("create or replace trigger \"TRIGGER_{0}_{1}\" before insert on \"{0}\" for each row " +
                                                                   "declare newid number(18,0);" +
                                                                   " begin " +
                                                                   "select \"{2}\".nextval into newid from dual; " +
                                                                   ":new.\"{1}\" := newId; " +
                                                                   "end;", tableName, columnName, "SEQ_" + tableName);
                                sqlhelper.ExecuteNonQuery(createtriggers);
                            }
                            else if (!isAutoGrow && triggers.Any())
                            {
                                var createtriggers = string.Format("drop trigger \"TRIGGER_{0}_{1}\" ", tableName,columnName);
                                sqlhelper.ExecuteNonQuery(createtriggers);
                            }

                        }
                    }
                    else
                    {
                        if (isPk != isRealPk)
                        {
                            var getconstraint = string.Format("select constraint_name name from user_cons_columns where constraint_name=(select constraint_name from user_constraints where upper(table_name)=upper('{0}') and constraint_type='P')", tableName);
                            var constraints = Warp.ShieldLogSql(() => sqlhelper.ExecuteDynamic(getconstraint));
                            foreach (var constraint in constraints)
                            {
                                var dropconstraint = "alter table \"" + tableName + "\" drop constraint \"" + constraint.NAME+"\"";
                                sqlhelper.ExecuteNonQuery(dropconstraint);
                            }
                        }
                        var alterSql = string.Format("alter table \"{0}\" add (\"{1}\" {2} {3} )", tableName, columnName, lastColumnType, nullAbleSql);
                        sqlhelper.ExecuteNonQuery(alterSql);
                        if (isPk != isRealPk)
                        {
                            alterSql = string.Format("alter table \"{0}\" add constraint \"{1}\" primary key(\"{2}\")", tableName, "pk_" + tableName + "_" + columnName, columnName);
                            sqlhelper.ExecuteNonQuery(alterSql);
                        }

                    }

                    #endregion
                }
                #endregion
            }
            else
            {
                CreateTable(sqlhelper, className, tableName);
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
                return "number";
            }
            else if (type == typeof(long) || type == typeof(ulong))
            {
                return "number";
            }
            else if (type == typeof(short) || type == typeof(ushort))
            {
                return "number";
            }
            else if (type == typeof(decimal))
            {
                return "number";
            }
            else if (type == typeof(double))
            {
                return "number";
            }
            else if (type == typeof(float))
            {
                return "number";
            }
            else if (type == typeof(bool))
            {
                return "number";
            }
            else if (type == typeof(byte))
            {
                return "number";
            }
            else if (type == typeof(byte[]))
            {
                return "blob";
            }
            else if (type.IsDigital())
            {
                return "number";
            }
            else if (type == typeof(DateTime))
            {
                return "date";
            }
            else if (type == typeof(string) && length == int.MaxValue)
            {
                return "clob";
            }
            else if (type == typeof(string))
            {
                return "varchar2(" + length + ")";
            }
            return "varchar2(" + length + ")";
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
                "clob",
                "nblob",
                "nclob",
                "number"
            };

        private bool IsNeedModiy(IEnumerable<dynamic> cols, Type memberType, string columnName, string columnType,
            int length, bool isNullAble, bool isPk, bool isRealPk ,bool isAutoGrow,bool isRealAutoGrow)
        {
            if (length == 0) length = 50;

            if (isPk != isRealPk)
            {
                return true;
            }
            if (isAutoGrow != isRealAutoGrow)
            {
                return true;
            }
            if (isPk || isAutoGrow)
            {
                if (_filterType.Contains(columnType.ToLower()))
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type.ToUpper() == columnType.Split('(')[0].ToUpper());
                }
                else if (columnType.Split('(')[0] == "nvarchar2" || columnType.Split('(')[0] == "nchar")
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type.ToUpper() == columnType.Split('(')[0].ToUpper() && x.length == (2 * length).ToString());
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type.ToUpper() == columnType.Split('(')[0].ToUpper() && x.length == length.ToString());
                }
            }
            else
            {
                if (_filterType.Contains(columnType.ToLower()))
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type.ToUpper() == columnType.Split('(')[0].ToUpper() && x.isnullable == isNullAble.ToYN());
                }
                else if (columnType.Split('(')[0] == "nvarchar2" || columnType.Split('(')[0] == "nchar")
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type.ToUpper() == columnType.Split('(')[0].ToUpper() && x.length == (2 * length).ToString() && x.isnullable == isNullAble.ToYN());
                }
                else
                {
                    return !cols.Any(x => x.name.ToUpper() == columnName.ToUpper() && x.type.ToUpper() == columnType.Split('(')[0].ToUpper() && x.length == length.ToString() && x.isnullable == isNullAble.ToYN());
                }
            }
        }

        private bool IsRealPkInDb(SqlHelper sqlhelper, string tableName, string columnName)
        {
            var pkName = Warp.ShieldLogSql(() =>GetPrimaryKey(sqlhelper, "", tableName));
            if (pkName == null)
            {
                return false;
            }
            return pkName.ToString().ToUpper() == columnName.ToUpper();
        }

        private bool IsRealAutoGrow(SqlHelper sqlHelper, string tableName, string columnName)
        {
            var gettriggers = string.Format("select trigger_name from user_triggers where table_name='{0}' and trigger_name='{1}'", tableName, "TRIGGER_" + tableName + "_" + columnName);
            var triggers = Warp.ShieldLogSql(() => sqlHelper.ExecuteDynamic(gettriggers));
            return triggers.Any();
        }

        private bool IsNeedAddTable(SqlHelper sqlhelper, string tableName)
        {
            var tables = Warp.ShieldLogSql(() => GetTables(sqlhelper, sqlhelper.DataBaseName));
            return tables.Any(x => x.name.ToUpper() == tableName.ToUpper());
        }

        public void CreateTable(SqlHelper sqlhelper,string className, string tableName)
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
                if (isPk)
                {
                    return string.Format("\"{0}\" {1} primary key", x.Value, lastColumnType);
                }
                return string.Format("\"{0}\" {1} {2}", x.Value, lastColumnType, nullAbleSql);
            });
            var columnsSql = string.Join(",", columnsArr);
            var createTable = "create table \"" + tableName + "\"(" + columnsSql + ")";
            sqlhelper.ExecuteNonQuery(createTable);
           
            var autoGrowName = IntrospectionManager.GetAutoGrowColumnName(className);
            if (!string.IsNullOrWhiteSpace(autoGrowName))
            {
                var getconstraint = string.Format(
                                   "select sequence_name,increment_by,last_number from user_sequences where upper(sequence_name)=upper('{0}')",
                                   "SEQ_" + tableName);
                var constraints = Warp.ShieldLogSql(() => sqlhelper.ExecuteDynamic(getconstraint));
                if (!constraints.Any())
                {
                    var addconstraint = string.Format("create sequence \"SEQ_{0}\" increment by 1 start with 1 nomaxvalue nocycle", tableName);
                    sqlhelper.ExecuteNonQuery(addconstraint);
                }

                var createtriggers = string.Format(
                        "create or replace trigger \"TRIGGER_{0}_{1}\" before insert on \"{0}\" for each row " +
                        "declare newid number(18,0);" +
                        " begin " +
                        "select \"{2}\".nextval into newid from dual; " +
                        ":new.\"{1}\" := newid; " +
                        "end;", tableName, autoGrowName, "SEQ_" + tableName);
                sqlhelper.ExecuteNonQuery(createtriggers);
            }
        }
    }
}
