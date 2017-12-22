using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using DataVeryLite.Aop;
using DataVeryLite.Bys;
using DataVeryLite.Exceptions;
using DataVeryLite.Util;

namespace DataVeryLite.Core
{
    /// <summary>
    /// Database table entity 
    /// </summary>
    public abstract class Entity 
    {
        protected string _entityStatus = "ready";
        internal string TableName { get; private set; }
        //public string IdColumnName { get; protected set; }
        internal string TableFullName { get; set; }
        protected SqlHelper _sqlHelper
        {
            get
            {
                var sqlHelper = IntrospectionManager.GetSqlHelperByKey(Key);
                if (sqlHelper == null)
                {
                    throw new SqlHelperCanNotExists(Key);
                }
                else
                {
                    return sqlHelper;
                }
            }
        }

        protected Dictionary<string, object> Resources;

        internal string LeftEscape
        {
            get { return ProviderManager.GetProvider(ProviderName).GetLeftEscape(); }
        }

        protected string RightEscape
        {
            get { return ProviderManager.GetProvider(ProviderName).GetRightEscape(); }
        }

        protected Entity()
        {
            TableFullName = GetType().FullName;
            Key = IntrospectionManager.GetTableKey(TableFullName);
            if (string.IsNullOrWhiteSpace(Key) && Configure.SetKey != null)
            {
                Key = Configure.SetKey.Invoke(this);
            }
            TableName = IntrospectionManager.GetTableName(TableFullName);
            Resources=new Dictionary<string, object>();
        }

        /// <summary>
        /// A provider name in ConnectionStirng
        /// </summary>
        internal virtual string ProviderName { get { return _sqlHelper.DataBaseType; } }

        /// <summary>
        /// Connection string key.
        /// </summary>
        internal string Key;

        public string GetKey()
        {
            return Key;
        }

        /// <summary>
        /// Load data for this entity
        /// </summary>
        public virtual void Load(By by)
        {
            if (by is BySql)
            {
                #region BySql

                var tmp = by as BySql;
                var trans = tmp.Tran;
                var cmdText = tmp.CmdText;
                var cmdType = tmp.CmdType;
                var cmdParms = tmp.CmdParms;

                _entityStatus = "loaded";

                var columns = IntrospectionManager.GetColumns(TableFullName);
                DbDataReader dataReader;
                if (trans == null)
                {
                    dataReader = _sqlHelper.ExecuteReader(cmdText, cmdType, cmdParms);
                }
                else
                {
                    dataReader = _sqlHelper.ExecuteReader(trans, cmdText, cmdType, cmdParms);
                }
                if (dataReader.Read())
                {
                    foreach (var propertyName in columns.Keys)
                    {
                        var fieldName = columns[propertyName];
                        var schemaTable = dataReader.GetSchemaTable();
                        if (schemaTable != null && schemaTable.Select("columnname='" + fieldName + "'").Length > 0)
                        {
                            object columnValue = dataReader[fieldName];
                            SetValue(propertyName, columnValue.ChangeType(columnValue.GetType()));
                        }
                            //if not exists this filed
                        else
                        {
                            SetValue(propertyName, null);
                        }
                    }
                }
                else
                {
                    foreach (var propertyName in columns.Keys)
                    {
                        SetValue(propertyName, null);
                    }
                }
                dataReader.Close();
                dataReader.Dispose();

                #endregion

                return;
            }
            else if (by is ById)
            {
                var tmp = by as ById;
                var idValue = tmp.IdValue;
                var tran = tmp.Tran;
                var pk = IntrospectionManager.GetPrimaryColumnName(TableFullName);
                string sql = string.Format("select * from {0} where {1}", Escape(TableName),GetWhereSqlByInputValue(pk,idValue));
                Load(By.Sql(sql, tran));
                return;
            }
            else if (by is ByAttach)
            {
                var pk = IntrospectionManager.GetPrimaryColumnName(TableFullName);
                if (!pk.Any())
                {
                    throw new TableNotHavePrimaryKeyException(TableFullName);
                }
                var tmp = by as ByAttach;
                var idValue = tmp.IdValue;
                var filedsArr = tmp.Fields.Select(Escape);
                var fields = tmp.Fields.Any() ? string.Join(",", filedsArr) : "*";
                string sql = string.Format("select {2} from {0} where {1}", Escape(TableName), GetWhereSqlByInputValue(pk, idValue), fields);
                Load(By.Sql(sql));
                return;
            }
            throw new NotSupportThisByException(by == null ? "null" : by.ToString());
        }

        public virtual void Load(Partly partly, Condition condition, DbTransaction tran = null)
        {
            string fields = "*";
            string where = "";
            if (partly != null)
            {
                var filedsArr = partly.Fields.Select(Escape);
                fields = partly.Fields.Any() ? string.Join(",", filedsArr) : "*";
            }
            if (condition != null)
            {
                var filedsArr =condition.Fields.Select(GetWhereSql);
                where = string.Join(" and ", filedsArr);
            }
            else
            {
                throw new Exception("Condition can't be null.");
            }

            string sql = string.Format("select {2} from {0} where {1}", Escape(TableName), where, fields);
            Load(By.Sql(sql, tran));
        }

        public virtual void Load(Condition condition, DbTransaction tran = null)
        {
            Load(null, condition, tran);
        }

        /// <summary>
        /// Delete record from database
        /// </summary>
        ///<param name="tran">Transaction</param>
        public virtual void Del(DbTransaction tran = null)
        {
            Del(null, tran);
        }
        /// <summary>
        ///  Delete record from database
        /// </summary>
        /// <param name="condition">Condition</param>
        /// <param name="tran">Transaction</param>
        public virtual void Del(Condition condition, DbTransaction tran = null)
        {
            _entityStatus = "deleted";
            string whereSql = "";
            if (condition != null)
            {
                var filedsArr = condition.Fields.Select(GetWhereSql);
                whereSql = string.Join(" and ", filedsArr);
            }
            else
            {
                var pk = IntrospectionManager.GetPrimaryMemberName(TableFullName);
                whereSql = GetWhereSql(pk);
            }
            string sql = string.Format("delete from {0} where {1}", Escape(TableName), whereSql);
            if (tran == null)
            {
                _sqlHelper.ExecuteNonQuery(sql);
            }
            else
            {
                _sqlHelper.ExecuteNonQuery(tran, sql);
            }
        }

        /// <summary>
        /// Update record to database
        /// </summary>
        /// <param name="tran">Transaction</param>
        public virtual void Update(DbTransaction tran = null)
        {
            Update(null, null, tran);
        }

        public virtual void Update(Partly partly, Condition condition, DbTransaction tran = null)
        {
            _entityStatus = "updated";
            
            string setSql = "";
            string whereSql = "";
            if (partly != null)
            {
                var filedsArr = partly.Fields.Where(x => Resources.Keys.Contains(x)).Select(GetSetSql);
                setSql = partly.Fields.Any() ? string.Join(",", filedsArr) : "*";
            }
            else
            {
                setSql = GetUpdateSetSql();
            }

            if (condition != null)
            {
                var filedsArr = condition.Fields.Select(GetWhereSql);
                whereSql = string.Join(" and ", filedsArr);
            }
            else
            {
                var pk = IntrospectionManager.GetPrimaryMemberName(TableFullName);
                whereSql = GetWhereSql(pk);
            }

            string sql = string.Format("update  {0} set {1} where {2}", Escape(TableName), setSql, whereSql);
            if (tran == null)
            {
                _sqlHelper.ExecuteNonQuery(sql);
            }
            else
            {
                _sqlHelper.ExecuteNonQuery(tran, sql);
            }
        }

        public virtual void Update(Partly partly, DbTransaction tran = null)
        {
            Update(partly, null, tran);
        }

        public virtual void Update(Condition condition, DbTransaction tran = null)
        {
            Update(null, condition, tran);
        }

        /// <summary>
        /// Insert entity to database
        /// </summary>
        /// <param name="isIdAutoGrow">Id is or isn't auto grow </param>
        /// <param name="transaction">Transaction</param>
        public virtual void Save(bool isIdAutoGrow = true, DbTransaction transaction = null)
        {
            _entityStatus = "saved";
            var isNeedCommit = false;
            var isSupportBatch = ProviderManager.GetProvider(ProviderName).IsSupportBatch;
            var pk = IntrospectionManager.GetPrimaryMemberName(TableFullName);
            var sql = "";
            if (isSupportBatch && ProviderName != DataBaseNames.Oracle)
            {
                sql+=ProviderManager.GetProvider(ProviderName).BatchBegin;
            }
            else if (transaction==null)
            {
                DbConnection conn = _sqlHelper.GetConn();
                conn.Open();
                transaction = conn.BeginTransaction();
                isNeedCommit = true;
            }

            if (isSupportBatch && ProviderName != DataBaseNames.Oracle)
            {
                sql += GetSaveSql(isIdAutoGrow);
                if (isIdAutoGrow)
                {
                    sql += GetLastInsertIdSql();
                }
            }
            else
            {
                var sqlT = GetSaveSql(isIdAutoGrow);
                if (ProviderName == DataBaseNames.Oracle)
                {
                    sqlT = sqlT.Remove(sqlT.LastIndexOf(";"), 1);
                }
                _sqlHelper.ExecuteNonQuery(transaction, sqlT);
                if (isIdAutoGrow)
                {
                    var idValue = _sqlHelper.ExecuteScalar(transaction, GetLastInsertIdSql());

                    if (idValue != null)
                    {
                        var pkType = IntrospectionManager.GetPrimaryKeyType(TableFullName);
                        SetValue(pk, idValue.ChangeType(pkType));
                    }
                }
            }

            if (isSupportBatch && ProviderName != DataBaseNames.Oracle)
            {
                sql += ProviderManager.GetProvider(ProviderName).BatchEnd;
            }

            if (isSupportBatch && ProviderName != DataBaseNames.Oracle)
            {
                object idValue;
                if (transaction == null)
                {
                    idValue = _sqlHelper.ExecuteScalar(sql);
                }
                else
                {
                    idValue = _sqlHelper.ExecuteScalar(transaction, sql);
                }

                if (isIdAutoGrow && idValue != null)
                {
                    var pkType = IntrospectionManager.GetPrimaryKeyType(TableFullName);
                    SetValue(pk, idValue.ChangeType(pkType));
                }
            }

            if (!isSupportBatch && isNeedCommit)
            {
                var conn = transaction.Connection;
                transaction.Commit();
                transaction.Dispose();
                conn.Close();
                conn.Dispose();
            }
        }

        /// <summary>
        /// Insert entity to database,isIdAutoGrow = true
        /// </summary>
        /// <param name="transaction">Transaction</param>
        public virtual void Save(DbTransaction transaction)
        {
            Save(true, transaction);
        }

        public virtual Entity Clone()
        {
            var target = (Entity) Activator.CreateInstance(this.GetType());
            foreach (var property in target.GetType().GetProperties().Where(x => x.CanRead && x.CanWrite))
            {
                property.SetValue(target, this.GetType().GetProperty(property.Name).GetValue(this, null), null);
            }
            return target;
        }

        internal string GetColumnName(string memberName)
        {
            return IntrospectionManager.GetColumns(TableFullName)[memberName];
        }

        internal string GetSqlValue(string memberName)
        {
            //var columnName = IntrospectionManager.GetColumns(TableFullName)[memberName];
            var value = GetValue(memberName);
            if (value == null)
            {
                return "null";
            }
            else if (value.GetType().IsDigital())
            {
                return value.ToString();
            }
            else if (value.GetType().IsByteArrary())
            {
                return "'" + Encoding.Default.GetString((byte[])value) + "'";
            }
            else if (value is DateTime&&(DateTime)value==DateTime.MinValue)
            {
                return "null";
            }
            else if (value is Guid && (Guid)value == Guid.Empty)
            {
                return "null";
            }
            else
            {
                return "'" + value + "'";
            }
        }

        internal string GetUpdateSetSql()
        {
            var infos = IntrospectionManager.GetColumns(TableFullName);
            var pk = IntrospectionManager.GetPrimaryMemberName(TableFullName);
            var filedsArr = infos.Keys.Where(x => !pk.Contains(x) && Resources.Keys.Contains(x)).Select(GetSetSql);
            var setSql = string.Join(",", filedsArr);
            return setSql;
        }

        internal string GetSetSql(string memberName)
        {
            var value = GetValue(memberName);
            var columnName = IntrospectionManager.GetColumns(TableFullName)[memberName];
            if (value == null)
            {
                return Escape(columnName) + "=null";
            }
            else if (value.GetType().IsDigital())
            {
                return Escape(columnName) + "=" + value;
            }
            else if (value.GetType().IsByteArrary())
            {
                return Escape(columnName) + "='" + Encoding.Default.GetString((byte[])value) + "'";
            }
            else if (value is DateTime && (DateTime)value == DateTime.MinValue)
            {
                return Escape(columnName) + "=null";
            }
            else if (value is Guid && (Guid)value == Guid.Empty)
            {
                return Escape(columnName) + "=null";
            }
            else
            {
                return Escape(columnName) + "='" + value + "'";
            }
        }

        internal string GetWhereSql(string memberName)
        {
            var value = GetValue(memberName);
            var columnName = IntrospectionManager.GetColumns(TableFullName)[memberName];
            return GetWhereSqlByInputValue(columnName, value);
        }

        internal string GetWhereSqlByInputValue(string columnName, object value)
        {
            if (value == null)
            {
                return Escape(columnName) + "is null";
            }
            else if (value.GetType().IsDigital())
            {
                return Escape(columnName) + "=" + value;
            }
            else if (value.GetType().IsByteArrary())
            {
                return Escape(columnName) + "='" + Encoding.Default.GetString((byte[])value) + "'";
            }
            else
            {
                return Escape(columnName) + "='" + value + "'";
            }
        }

        internal string GetSaveSql(bool isIdAutoGrow)
        {
            var pk = IntrospectionManager.GetPrimaryMemberName(TableFullName);
            var infos = IntrospectionManager.GetColumns(TableFullName);
            string valueSql = "";
            string columnSql = "";
            if (isIdAutoGrow)
            {
                var filedsArr = infos.Keys.Where(x => !pk.Contains(x)).Select(GetSqlValue);
                valueSql = string.Join(",", filedsArr);
                var columnsArr = infos.Keys.Where(x => !pk.Contains(x)).Select(x => Escape(GetColumnName(x)));
                columnSql = string.Join(",", columnsArr);
            }
            else
            {
                var filedsArr = infos.Keys.Select(GetSqlValue);
                valueSql = string.Join(" , ", filedsArr);
                var columnsArr = infos.Keys.Select(x => Escape(GetColumnName(x)));
                columnSql = string.Join(",", columnsArr);
            }

            string sql = string.Format("insert into  {0}({1}) values ({2});", Escape(TableName), columnSql, valueSql);
            return sql;
        }

        internal string GetUpdateSql()
        {
            var pk = IntrospectionManager.GetPrimaryMemberName(TableFullName);
            string setSql = GetUpdateSetSql();
            var sql = string.Format("update  {0} set {1} where {2};", Escape(TableName), setSql, GetWhereSql(pk));
            return sql;
        }

        internal string GetDelSql()
        {
            var pk = IntrospectionManager.GetPrimaryMemberName(TableFullName);
            var sql = string.Format("delete from {0} where {1};", Escape(TableName), GetWhereSql(pk));
            return sql;
        }

        internal string GetLastInsertIdSql()
        {
            if (ProviderName == DataBaseNames.Oracle)
            {
                return ProviderManager.GetProvider(ProviderName).GetLastInsertIdSql(TableName);
            }
            else
            {
                return ProviderManager.GetProvider(ProviderName).GetLastInsertIdSql(Escape(TableName));
            }
        }

        internal string Escape(string value)
        {
            return LeftEscape + value + RightEscape;
        }

        public object GetValue(string memberName)
        {
            return IntrospectionManager.GetGetDelegate(TableFullName)(this, memberName);
        }

        public void SetValue(string memberName,object value)
        {
            InvokeSetProperty(memberName, value);
            IntrospectionManager.GetSetDelegate(TableFullName)(this, memberName, value);
        }

        public  void InvokeSetProperty(string name, object value)
        {
            if (Resources.Keys.Contains(name))
            {
                Resources[name] = value;
            }
            else
            {
                Resources.Add(name, value);
            }
        }
    }
}
