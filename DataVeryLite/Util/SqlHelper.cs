using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using DataVeryLite.Core;
using DataVeryLite.Exceptions;
using Microsoft.CSharp;

namespace DataVeryLite.Util
{
    /// <summary>
    /// Sql CRUD provider and helper
    /// </summary>
    public class SqlHelper
    {
        private SqlHelper()
        {
        }
        /// <summary>
        /// Sql CRUD provider and helper
        /// </summary>
        /// <param name="dbConnectionString">ConnectionString</param>
        /// <param name="providerName">ConnectionString provider name in config</param>
        /// <param name="driverPath">Your driver path,the default path is strat up dir</param>
        public SqlHelper(string dbConnectionString, string providerName,string driverPath)
        {
            _dbConnectionString = dbConnectionString;
            _providerName = providerName;

            setDatabaseType();
            setDatabaseName();
            setDriverAssembly(driverPath);
            if (_dataBaseType == DataBaseNames.Sqlite)
            {
                var builder = new DbConnectionStringBuilder();
                builder.ConnectionString = _dbConnectionString;
                object database = "";
                builder.TryGetValue("Data Source", out database);
                if (!System.IO.File.Exists(database.ToString()))
                {
                    string accessPath = driverPath + @"bin\debug\"+database.ToString();
                    if (System.IO.File.Exists(accessPath))
                    {
                        _dbConnectionString = "Data Source=" + accessPath;
                    }
                    else
                    {
                        //if web dir
                        accessPath = driverPath + database.ToString();
                        if (System.IO.File.Exists(accessPath))
                        {
                            _dbConnectionString = "Data Source=" + accessPath;
                        }
                    }
                }
            }
        }
        /// <summary>
        /// Sql CRUD provider and helper
        /// </summary>
        /// <param name="key">ConnectionString key in config</param>
        /// <param name="driverPath">Your driver path,the default path is strat up dir</param>
        public SqlHelper(string key, string driverPath = null)
        {
            if (!IntrospectionManager.IsExistsKey(key))
            {
                var exception = new ConnectionStringKeyNotExists(key);
                throw exception;
            }
            _key = key;
            _dbConnectionString = IntrospectionManager.GetConnectionString(key);
            _providerName = IntrospectionManager.GetProviderName(key);

            setDatabaseType();
            setDatabaseName();
            if (driverPath == null)
            {
                setDriverAssembly(AppDomain.CurrentDomain.BaseDirectory);
            }
            else
            {
                setDriverAssembly(driverPath);
            }
            if (_dataBaseType == DataBaseNames.Sqlite)
            {
                var builder = new DbConnectionStringBuilder();
                builder.ConnectionString = _dbConnectionString;
                object database = "";
                builder.TryGetValue("Data Source", out database);
                if (!System.IO.File.Exists(database.ToString()))
                {
                    string accessPath=AppDomain.CurrentDomain.BaseDirectory+database.ToString();
                    if (System.IO.File.Exists(accessPath))
                    {
                        _dbConnectionString = "Data Source=" + accessPath;
                    }
                }
            }
            if (_driverAssembly != null)
            {
                string info = string.Format("[Prvider:'{0}'][Key:'{1}'][Db:'{2}'] {3}", _dataBaseType,_key,_dataBaseName, "Create sqlhelper instance sucessed!");
                LogHelper.LogInfo(info);
            }
            else
            {
                string info = string.Format("[Prvider:'{0}'][Key:'{1}'][Db:'{2}'] {3}", _dataBaseType, _key,_dataBaseName, "Create sqlhelper instance fail!");
                LogHelper.LogError(info);
            }
        }

        #region DBConnectionString

        private readonly string _dbConnectionString;

        public string DbConnectionString
        {
            get { return _dbConnectionString; }
        }

        private string _key;

        public string Key
        {
            get { return _key; }
        }

        private string _dataBaseType;

        public string DataBaseType
        {
            get { return _dataBaseType; }
        }

        private string _dataBaseName;

        public string DataBaseName
        {
            get { return _dataBaseName; }
        }


        private Assembly _driverAssembly;
        private readonly string _providerName;


        #endregion

        #region Util

        private void setDatabaseType()
        {
            if (ProviderManager.Providers.Keys.Contains(_providerName))
            {
                _dataBaseType = _providerName;
            }
            else
            {
                throw new ConnectionStringProviderNameNotExists(_dbConnectionString);
            }
        }

        private void setDriverAssembly(string path)
        {
            var driver = IntrospectionManager.GetDriver(_providerName);
            if (driver != null)
            {
                _driverAssembly = driver;
                return;
            }
            try
            {
                _driverAssembly = ProviderManager.GetProvider(_dataBaseType).GetAssembly(path + "\\Drivers\\");
            }
            catch (Exception)
            {
                string info = string.Format("[Provider:'{0}'][Key:'{1}'][Db:'{2}'] {3}", _dataBaseType,_key,_dataBaseName, "Can't found driver!");
                LogHelper.LogWarning(info);
            }
        }

        private void setDatabaseName()
        {
            var builder = new DbConnectionStringBuilder();
            builder.ConnectionString = _dbConnectionString;
            _dataBaseName = ProviderManager.GetProvider(_dataBaseType).GetDataBaseName(builder);
        }
        /// <summary>
        /// Get all table names
        /// </summary>
        /// <returns></returns>
        public List<dynamic> GetTables()
        {
            return ProviderManager.GetProvider(_dataBaseType).GetTables(this, _dataBaseName);
        }
        /// <summary>
        /// Get all column names from table
        /// </summary>
        /// <param name="tableName">A table name</param>
        /// <returns></returns>
        public List<dynamic> GetColumns(string tableName)
        {
            return ProviderManager.GetProvider(_dataBaseType).GetColumns(this, _dataBaseName, tableName);
        }
        /// <summary>
        /// Get column type 
        /// </summary>
        /// <param name="tableName">A table name</param>
        /// <param name="columnName">A column name</param>
        /// <returns></returns>
        public string GetColumnType(string tableName, string columnName)
        {
            return ProviderManager.GetProvider(_dataBaseType).GetColumnType(this, _dataBaseName, tableName, columnName);
        }
        /// <summary>
        /// Get primary key column name
        /// </summary>
        /// <param name="tableName">A table name</param>
        /// <returns></returns>
        public object GetPrimaryKey(string tableName)
        {
            return ProviderManager.GetProvider(_dataBaseType).GetPrimaryKey(this, _dataBaseName, tableName);
        }
        /// <summary>
        /// Get DbCommand
        /// </summary>
        /// <returns></returns>
        public  DbCommand GetCmd()
         {
             return ProviderManager.GetProvider(_dataBaseType).GetCmd(_driverAssembly);
         }
        /// <summary>
        /// Get DbConnection
        /// </summary>
        /// <returns></returns>
        public  DbConnection GetConn()
        {
            return GetConn(_dbConnectionString);
        }
        /// <summary>
        /// Get DbConnection
        /// </summary>
        /// <param name="dbConnectionStr">Connection string</param>
        /// <returns></returns>
        public DbConnection GetConn(string dbConnectionStr)
        {
            return ProviderManager.GetProvider(_dataBaseType).GetConn(_driverAssembly, dbConnectionStr);
        }
        /// <summary>
        /// Get IDataAdapter
        /// </summary>
        /// <param name="cmd">DbCommand</param>
        /// <returns></returns>
        public  IDataAdapter GetAdapter(DbCommand cmd)
         {
             return ProviderManager.GetProvider(_dataBaseType).GetAdapter(_driverAssembly, cmd);
         }
        #endregion

        #region PrepareCommand

        /// <summary>

        /// Command prepare deal

        /// </summary>

        /// <param name="conn">Connection</param>

        /// <param name="trans">Transaction</param>

        /// <param name="cmd">Command</param>

        /// <param name="cmdType">CommandType</param>

        /// <param name="cmdText">Sql text</param>

        /// <param name="cmdParms">Parameters</param>

        private  void PrepareCommand(DbConnection conn, DbTransaction trans, DbCommand cmd, CommandType cmdType, string cmdText, DbParameter[] cmdParms)
        {
            if (Configure.EnableLogSql)
            {
                string info = string.Format("[Provider:'{0}'][Key:'{1}'][Db:'{2}'] {3}", _dataBaseType, _key,_dataBaseName, cmdText);
                if (Configure.LogSqlLevel == TraceLevel.Verbose)
                {
                    LogHelper.LogVerbose(info);
                }
                else if (Configure.LogSqlLevel == TraceLevel.Info)
                {
                    LogHelper.LogInfo(info);
                }
                else if (Configure.LogSqlLevel == TraceLevel.Warning)
                {
                    LogHelper.LogWarning(info);
                }
                else if (Configure.LogSqlLevel == TraceLevel.Error)
                {
                    LogHelper.LogError(info);
                }
            }
            if (conn.State != ConnectionState.Open)

                conn.Open();

            cmd.Connection = conn;

            cmd.CommandText = cmdText;

            if (trans != null)

                cmd.Transaction = trans;

            cmd.CommandType = cmdType;

            if (cmdParms != null)
            {

                foreach (DbParameter parm in cmdParms)

                    cmd.Parameters.Add(parm);

            }
            //cmd.Prepare();
        }

        #endregion

        #region ExecuteNonQuery
        /// <summary>
        /// Return the affected rows 
        /// </summary>
        /// <param name="connectionString">Connection String</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The affected rows</returns>
        public int ExecuteNonQuery(string connectionString, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {

                DbCommand cmd = GetCmd();

                using (DbConnection conn = GetConn(connectionString))
                {

                    PrepareCommand(conn, null, cmd, cmdType, cmdText, cmdParms);

                    int val = cmd.ExecuteNonQuery();

                    cmd.Parameters.Clear();

                    return val;

                }
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return the affected rows
        /// </summary>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The affected rows</returns>
        public int ExecuteNonQuery(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            return ExecuteNonQuery(_dbConnectionString, cmdText, cmdType, cmdParms);
        }
        /// <summary>
        /// Return the affected rows
        /// </summary>
        /// <param name="conn">DbConnection</param>
        /// <param name="cmdText">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdType">DbParameter</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The affected rows</returns>
        public int ExecuteNonQuery(DbConnection conn, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                PrepareCommand(conn, null, cmd, cmdType, cmdText, cmdParms);

                int val = cmd.ExecuteNonQuery();

                cmd.Parameters.Clear();

                return val;
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return the affected rows
        /// </summary>
        /// <param name="trans">DbTransaction</param>
        /// <param name="cmdText">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdType">DbParameter</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The affected rows</returns>
        public int ExecuteNonQuery(DbTransaction trans, string cmdText, CommandType cmdType = CommandType.Text,params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                PrepareCommand(trans.Connection, trans, cmd, cmdType, cmdText, cmdParms);

                int val = cmd.ExecuteNonQuery();

                cmd.Parameters.Clear();

                return val;
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }

        #endregion

        #region ExecuteScalar
        /// <summary>
        /// Return the single value
        /// </summary>
        /// <param name="connectionString">Connection String</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The single value</returns>
        public object ExecuteScalar(string connectionString, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                using (DbConnection connection = GetConn(connectionString))
                {

                    PrepareCommand(connection, null, cmd, cmdType, cmdText, cmdParms);

                    object val = cmd.ExecuteScalar();

                    cmd.Parameters.Clear();

                    return val;

                }
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return the single value
        /// </summary>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The single value</returns>
        public object ExecuteScalar(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            return ExecuteScalar(DbConnectionString, cmdText, cmdType, cmdParms);
        }
        /// <summary>
        /// Return the single value
        /// </summary>
        /// <param name="conn">DbConnection</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The single value</returns>
        public object ExecuteScalar(DbConnection conn, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                PrepareCommand(conn, null, cmd, cmdType, cmdText, cmdParms);

                object val = cmd.ExecuteScalar();

                cmd.Parameters.Clear();

                return val;
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return the single value
        /// </summary>
        /// <param name="trans">DbTransaction</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The single value</returns>
        public object ExecuteScalar(DbTransaction trans, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                PrepareCommand(trans.Connection, trans, cmd, cmdType, cmdText, cmdParms);

                object val = cmd.ExecuteScalar();

                cmd.Parameters.Clear();

                return val;
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        #endregion

        #region ExecuteReader
        /// <summary>
        /// Return the DataReader
        /// </summary>
        /// <param name="connectionString">Connection String</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The DataReader</returns>
        public DbDataReader ExecuteReader(string connectionString, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            DbCommand cmd = GetCmd();

            DbConnection conn = GetConn(connectionString);

            try
            {
                PrepareCommand(conn, null, cmd, cmdType, cmdText, cmdParms);

                DbDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                //使用下面的返回参数会被清空
                //cmd.Parameters.Clear();

                return dr;
            }
            catch
            {
                conn.Close();
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return the DataReader
        /// </summary>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The DataReader</returns>
        public DbDataReader ExecuteReader(string cmdText, CommandType cmdType = CommandType.Text,params DbParameter[] cmdParms)
        {
            return ExecuteReader(_dbConnectionString, cmdText, cmdType, cmdParms);
        }
        /// <summary>
        /// Return the DataReader
        /// </summary>
        /// <param name="conn">DbConnection</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The DataReader</returns>
        public DbDataReader ExecuteReader(DbConnection conn, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();
                PrepareCommand(conn, null, cmd, cmdType, cmdText, cmdParms);

                DbDataReader dr = cmd.ExecuteReader();

                cmd.Parameters.Clear();

                return dr;
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return the DataReader
        /// </summary>
        /// <param name="trans">DbTransaction</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The DataReader</returns>
        public DbDataReader ExecuteReader(DbTransaction trans, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();
                PrepareCommand(trans.Connection, trans, cmd, cmdType, cmdText, cmdParms);

                DbDataReader dr = cmd.ExecuteReader();

                cmd.Parameters.Clear();

                return dr;
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }

        #endregion

        #region ExecuteDataSet
        /// <summary>
        /// Return the DataSet
        /// </summary>
        /// <param name="connectionString">Connection String</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The DataSet</returns>
        public DataSet ExecuteDataSet(string connectionString, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                using (DbConnection conn = GetConn(connectionString))
                {

                    PrepareCommand(conn, null, cmd, cmdType, cmdText, cmdParms);

                    IDataAdapter da = GetAdapter(cmd);

                    var ds = new DataSet();

                    da.Fill(ds);

                    conn.Close();

                    cmd.Parameters.Clear();

                    return ds;

                }
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return the DataSet
        /// </summary>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The DataSet</returns>
        public DataSet ExecuteDataSet(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            return ExecuteDataSet(_dbConnectionString, cmdText, cmdType, cmdParms);
        }
        /// <summary>
        /// Return the DataSet
        /// </summary>
        /// <param name="conn">DbConnection</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>The DataSet</returns>
        public DataSet ExecuteDataSet(DbConnection conn , string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                PrepareCommand(conn, null, cmd, cmdType, cmdText, cmdParms);

                IDataAdapter da = GetAdapter(cmd);

                var ds = new DataSet();

                da.Fill(ds);

                cmd.Parameters.Clear();

                return ds;
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return the DataSet
        /// </summary>
        /// <param name="trans">DbTransaction</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns></returns>
        public DataSet ExecuteDataSet(DbTransaction trans, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                PrepareCommand(trans.Connection, trans, cmd, cmdType, cmdText, cmdParms);

                IDataAdapter da = GetAdapter(cmd);

                var ds = new DataSet();

                da.Fill(ds);

                cmd.Parameters.Clear();

                return ds;
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return anything you want
        /// </summary>
        /// <param name="connectionString">Connection String</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>Anything you want</returns>
        public List<dynamic> ExecuteDynamic(string connectionString, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                using (DbConnection conn = GetConn(connectionString))
                {

                    PrepareCommand(conn, null, cmd, cmdType, cmdText, cmdParms);

                    IDataAdapter da = GetAdapter(cmd);

                    var dataSet = new DataSet();

                    da.Fill(dataSet);

                    conn.Close();

                    cmd.Parameters.Clear();

                    var list = new List<dynamic>();
                    if (dataSet.Tables.Count != 0)
                    {
                        if (dataSet.Tables[0].Rows.Count != 0)
                        {
                            var columnNames = new List<string>();
                            foreach (DataColumn dc in dataSet.Tables[0].Columns)
                            {
                                columnNames.Add(dc.ColumnName);
                            }
                            foreach (DataRow dr in dataSet.Tables[0].Rows)
                            {
                                var obj = new Dynamic();
                                foreach (var columnName in columnNames)
                                {
                                    obj.SetProperty(columnName, dr[columnName]);
                                }
                                list.Add(obj);
                            }
                        }
                    }
                    return list;

                }
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return anything you want
        /// </summary>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>Anything you want</returns>
        public List<dynamic> ExecuteDynamic(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            return ExecuteDynamic(_dbConnectionString, cmdText, cmdType, cmdParms);
        }
        /// <summary>
        /// Return anything you want
        /// </summary>
        /// <param name="conn">DbConnection</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>Anything you want</returns>
        public List<dynamic> ExecuteDynamic(DbConnection conn, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                var dataSet = new DataSet();

                PrepareCommand(conn, null, cmd, cmdType, cmdText, cmdParms);

                IDataAdapter da = GetAdapter(cmd);

                da.Fill(dataSet);

                cmd.Parameters.Clear();

                var list = new List<dynamic>();
                if (dataSet.Tables.Count != 0)
                {
                    if (dataSet.Tables[0].Rows.Count != 0)
                    {
                        var columnNames = new List<string>();
                        foreach (DataColumn dc in dataSet.Tables[0].Columns)
                        {
                            columnNames.Add(dc.ColumnName);
                        }
                        foreach (DataRow dr in dataSet.Tables[0].Rows)
                        {
                            var obj = new Dynamic();
                            foreach (var columnName in columnNames)
                            {
                                obj.SetProperty(columnName, dr[columnName]);
                            }
                            list.Add(obj);
                        }
                    }
                }
                return list;
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        /// <summary>
        /// Return anything you want
        /// </summary>
        /// <param name="trans">DbTransaction</param>
        /// <param name="cmdText">Sql</param>
        /// <param name="cmdType">CommandType,default value is CommandType.Text</param>
        /// <param name="cmdParms">DbParameter</param>
        /// <returns>Anything you want</returns>
        public List<dynamic> ExecuteDynamic(DbTransaction trans, string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            try
            {
                DbCommand cmd = GetCmd();

                var dataSet = new DataSet();


                PrepareCommand(trans.Connection, trans, cmd, cmdType, cmdText, cmdParms);

                IDataAdapter da = GetAdapter(cmd);

                da.Fill(dataSet);

                cmd.Parameters.Clear();

                var list = new List<dynamic>();
                if (dataSet.Tables.Count != 0)
                {
                    if (dataSet.Tables[0].Rows.Count != 0)
                    {
                        var columnNames = new List<string>();
                        foreach (DataColumn dc in dataSet.Tables[0].Columns)
                        {
                            columnNames.Add(dc.ColumnName);
                        }
                        foreach (DataRow dr in dataSet.Tables[0].Rows)
                        {
                            var obj = new Dynamic();
                            foreach (var columnName in columnNames)
                            {
                                obj.SetProperty(columnName, dr[columnName]);
                            }
                            list.Add(obj);
                        }
                    }
                }
                return list;
            }
            catch (Exception)
            {
                LogHelper.LogError("When execute this sql =>" + cmdText);
                throw;
            }
        }
        #endregion
        ///<summary>
        /// Create new assembly
        ///</summary>
        [Obsolete]
        public static Assembly NewAssembly(List<string> properties)
        {
            //创建编译器实例。  
            var provider = new CSharpCodeProvider();
            //设置编译参数。  
            var paras = new CompilerParameters();
            paras.GenerateExecutable = false;
            paras.GenerateInMemory = true;
            //创建动态代码。  
            var classSource = new StringBuilder();
            classSource.Append("public   class   DynamicClass \n");
            classSource.Append("{\n");
            //创建属性。  
            foreach (var property in properties)
            {
                classSource.Append(propertyString(property));
            }
            classSource.Append("}");
            
            //编译代码。  
            CompilerResults result = provider.CompileAssemblyFromSource(paras, classSource.ToString());
            //获取编译后的程序集。  
            Assembly assembly = result.CompiledAssembly;
            return assembly;
        }
        //Concat property
        [Obsolete]
        private static string propertyString(string propertyName)
        {
            var sbProperty = new StringBuilder();
            sbProperty.Append(" private   string   _" + propertyName + "   ;\n");
            sbProperty.Append(" public   string   " + "" + propertyName + "\n");
            sbProperty.Append(" {\n");
            sbProperty.Append(" get{   return   _" + propertyName + ";}   \n");
            sbProperty.Append(" set{   _" + propertyName + "   =   value;   }\n");
            sbProperty.Append(" }");
            return sbProperty.ToString();
        }
        ///<summary>
        /// Set property value
        ///</summary>
        [Obsolete]
        public static void ReflectionSetProperty(object objClass, string propertyName, object value)
        {
            PropertyInfo[] infos = objClass.GetType().GetProperties();
            foreach (PropertyInfo info in infos)
            {
                if (info.Name == propertyName && info.CanWrite)
                {
                    info.SetValue(objClass, value, null);
                }
            }
        }
        ///<summary>
        /// Get property value
        ///</summary>
        [Obsolete]
        public static string ReflectionGetProperty(object objClass, string propertyName)
        {
            PropertyInfo[] infos = objClass.GetType().GetProperties();
            foreach (PropertyInfo info in infos)
            {
                if (info.Name == propertyName && info.CanRead)
                {
                    object value = info.GetValue(objClass, null);
                    return value == null ? null : value.ToString();
                }
            }
            throw new Exception("no propertyName:" + propertyName + " at class " + objClass.GetType().Name);
        }
        [Obsolete]
        public static object ExpressionGetProperty(object objClass, string propertyName)
        {
            PropertyInfo[] infos = objClass.GetType().GetProperties();
            foreach (PropertyInfo info in infos)
            {
                var attr = info.GetCustomAttributes(false).Where(x => x is ColumnAttribute).FirstOrDefault() as ColumnAttribute;
                var fieldName = string.IsNullOrEmpty(attr.Name) ? info.Name : attr.Name;

                if (fieldName == propertyName && info.CanRead)
                {
                    object value = info.GetValue(objClass, null);
                    return value;
                }
            }
            throw new Exception("no propertyName:" + propertyName + " at class " + objClass.GetType().Name);
        }
        ///<summary>
        /// Get property type
        ///</summary>
        [Obsolete]
        public static Type ReflectionGetPropertyType(object objClass, string propertyName)
        {
            PropertyInfo[] infos = objClass.GetType().GetProperties();
            foreach (PropertyInfo info in infos)
            {
                if (info.Name == propertyName && info.CanRead)
                {
                    return info.PropertyType;
                }
            }
            throw new Exception("no propertyName:" + propertyName + " at class " + objClass.GetType().Name);
        }
    }
}
