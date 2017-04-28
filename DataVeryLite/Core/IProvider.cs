using System;
using System.Collections.Generic;
using System.Reflection;
using System.Data.Common;
using System.Data;
using DataVeryLite.Util;

namespace DataVeryLite.Core
{
    public interface  IProvider
    {
        string ProviderName { get;}
        Assembly GetAssembly(string path);
        string GetDataBaseName(DbConnectionStringBuilder builder);
        List<dynamic> GetTables(SqlHelper sqlHelper,string dataBaseName);
        List<dynamic> GetColumns(SqlHelper sqlHelper, string dataBaseName, string tableName);
        string GetColumnType(SqlHelper sqlHelper, string dataBaseName, string tableName, string columnName);
        object GetPrimaryKey(SqlHelper sqlHelper, string dataBaseName, string tableName);
        DbCommand GetCmd(Assembly driverAssembly);
        DbConnection GetConn(Assembly driverAssembly, string dbConnectionStr);
        IDataAdapter GetAdapter(Assembly driverAssembly, DbCommand cmd);
        string GetLastInsertIdSql(string tableName);
        string GetLeftEscape();
        string GetRightEscape();
        string GetPageSql(int page, int pageSize, bool asc,string talbeName,string idColumnName);
        string GetTopSql(int top, bool asc, string talbeName, string idColumnName);
        bool IsSupportBatch { get; }
        string BatchBegin { get; }
        string BatchEnd { get; }
        void SyncDbInfo(string key, string className, string tableName);
        string GetColumnTypeByMebmberType(Type type, string columnType, int length);
    }
}
