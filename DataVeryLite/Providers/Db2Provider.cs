using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using DataVeryLite.Core;
using DataVeryLite.Util;

namespace DataVeryLite.Providers
{
    class Db2Provider:IProvider
    {
        public string ProviderName
        {
            get { return  DataBaseNames.Db2; }
        }
        
        public System.Reflection.Assembly GetAssembly(string path)
        {
            if (Environment.Is64BitProcess)
            {
                return Assembly.LoadFrom(path + @"IBM.Data.DB2.dll");
            }
            else
            {
                return Assembly.LoadFrom(path + @"32bit\IBM.Data.DB2.dll");
            }
        }
        private string getUser(string connStr)
        {
            var builder=new DbConnectionStringBuilder();
            builder.ConnectionString = connStr;
            object user = "";
            builder.TryGetValue("User ID", out user);
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
            return database.ToString().FirstLetterToUpper();
        }

        public List<dynamic> GetTables(Util.SqlHelper sqlHelper, string dataBaseName)
        {

            var list = new List<dynamic>();
            var dr = sqlHelper.ExecuteReader("select name from sysibm.systables where type='T' and upper(creator) = upper('" + getUser(sqlHelper.DbConnectionString) + "')");
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
            var dr = sqlHelper.ExecuteReader("select name from sysibm.syscolumns where upper(tbname)=upper('" + tableName + "')");
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

        private List<dynamic> _tableInfos = null;
        private string _lastTableName = null;
        public string GetColumnType(Util.SqlHelper sqlHelper, string dataBaseName, string tableName, string columnName)
        {
            if (tableName != _lastTableName || _tableInfos == null)
            {
                _tableInfos = new List<dynamic>();
                var dr =
                    sqlHelper.ExecuteReader("select name,typename from sysibm.syscolumns where upper(tbname)=upper('" + tableName + "')");
                while (dr.Read())
                {
                    string type = dr["typename"].ToString().ToLower();

                    #region typedef

                    if (type == "varchar" || type == "char" || type == "character")
                    {
                        type = "string";
                    }
                    else if (type == "real")
                    {
                        type = "float";
                    }
                    else if (type == "double" || type == "double precision" || type == "float")
                    {
                        type = "double";
                    }
                    else if (type == "decimal" || type == "numeric" || type == "num" || type == "dec" || type == "decfloat")
                    {
                        type = "decimal";
                    }
                    else if (type == "bigint")
                    {
                        type = "long";
                    }
                    else if (type == "integer")
                    {
                        type = "int";
                    }
                    else if (type == "smallint")
                    {
                        type = "short";
                    }
                    else if (type == "date" || type == "time" || type == "timestamp")
                    {
                        type = "DateTime";
                    }
                    else if (type=="xml"||type == "varbinary" || type == "binary" || type == "char for bit data"||type=="long varchar for bit data"||type=="blob"||type=="rowid")
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
            object pk=sqlHelper.ExecuteScalar("select name from sysibm.syscolumns where upper(tbname) = upper('" + tableName + "')  and keyseq > 0  order by keyseq asc");
            return pk==null?null:pk.ToString().FirstLetterToUpper();

        }

        public System.Data.Common.DbCommand GetCmd(System.Reflection.Assembly driverAssembly)
        {
            return (DbCommand)driverAssembly.CreateInstance("IBM.Data.DB2.DB2Command");
        }

        public System.Data.Common.DbConnection GetConn(System.Reflection.Assembly driverAssembly, string dbConnectionStr)
        {
            return
                   (DbConnection)
                   driverAssembly.CreateInstance("IBM.Data.DB2.DB2Connection", true, BindingFlags.Default, null,
                                      new object[] { dbConnectionStr }, null, null);
        }

        public System.Data.IDataAdapter GetAdapter(System.Reflection.Assembly driverAssembly, System.Data.Common.DbCommand cmd)
        {
            return (System.Data.IDataAdapter)
                     driverAssembly.CreateInstance("IBM.Data.DB2.DB2DataAdapter", true, BindingFlags.Default, null,
                                        new object[] { cmd }, null, null);
        }

        public string GetLastInsertIdSql(string tableName)
        {
            return "select sysibm.identity_val_local() from sysibm.dual;";
        }

        public string GetLeftEscape()
        {
            return "";
        }

        public string GetRightEscape()
        {
            return "";
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
                        "select * from (select {0}.*,rownumber() over(order by {1} asc ) as rowid  from {0} ) as a where a.rowid >= {2} and a.rowid < {3}",
                        _left + talbeName + _right,
                        _left + idColumnName + _right, (page - 1)*pageSize + 1, page*pageSize + 1);
            }
            else
            {
                sql =
                    string.Format(
                        "select * from (select {0}.*,rownumber() over(order by {1} desc ) as rowid  from {0} ) as a where a.rowid >= {2} and a.rowid < {3} order by {1} desc",
                        _left + talbeName + _right,
                        _left + idColumnName + _right, (page - 1)*pageSize + 1, page*pageSize + 1);
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
                sql = string.Format("select * from {0}  fetch first {1} rows only", _left + talbeName + _right, top);
            }
            else
            {
                sql = string.Format("select * from {0} order by {1} desc  fetch first {2} rows only", _left + talbeName + _right, _left + idColumnName + _right, top);
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
           // throw new NotImplementedException();
        }


        public string GetColumnTypeByMebmberType(Type type, string columnType, int length)
        {
          //  throw new NotImplementedException();
            return "";
        }
    }
}
