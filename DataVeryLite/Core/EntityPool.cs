using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using DataVeryLite.Bys;
using DataVeryLite.Exceptions;
using DataVeryLite.Util;

namespace DataVeryLite.Core
{
    /// <summary>
    /// Database table entity pool
    /// </summary>
    public abstract class EntityPool
    {
       /*static EntityPool()
        {
            var entityPoolType = typeof(TSelfReferenceType);
            var attributes = entityPoolType.GetCustomAttributes(false).Where(x => x is DataBaseAttribute);
            if (attributes.Any())
            {
                var dBAttribute = (DataBaseAttribute)attributes.FirstOrDefault();
                NativeSql = IntrospectionManager.GetSqlHelperByKey(dBAttribute.Key);
            }
            else
            {
                throw new ClassNotHaveDataBaseAttributeException(entityPoolType.Name);
            }
        }  */

        public EntityPool()
        {
            Key = IntrospectionManager.GetDbKey(GetType().FullName);
            if (string.IsNullOrWhiteSpace(Key) && Configure.SetKey != null)
            {
                Key = Configure.SetKey.Invoke(this);
            }
        }

        public string Key { get; internal set; }

        public  SqlHelper NativeSql
        {
            get { return Sql(Key); }
        }

        public static SqlHelper Sql(string key)
        {
            var sqlHelper = IntrospectionManager.GetSqlHelperByKey(key);
            if (sqlHelper == null)
            {
                throw new SqlHelperCanNotExists(key);
            }
            else
            {
                return sqlHelper;
            }
        }
      
        protected TransactionStatus _tranactionStatus = TransactionStatus.Normal;
        protected DbTransaction _dbTransaction = null;
        protected DbConnection _dbConnection = null;

        /// <summary>
        /// A provider name in ConnectionStirng
        /// </summary>
        public virtual string ProviderName { get { return NativeSql.DataBaseType; }}

        /// <summary>
        /// Easy to access database,you can get any data that you want.
        /// </summary>
        /// <param name="sql">Any sql text</param>
        /// <param name="trans">Transaction</param>
        /// <returns>Any data list</returns>
        public List<dynamic> List(string sql, DbTransaction trans = null)
        {
            if (trans == null)
            {
                return NativeSql.ExecuteDynamic(sql);
            }
            else
            {
                return NativeSql.ExecuteDynamic(trans, sql);
            }
        }

        /// <summary>
        /// Easy to access database,you can get any data that you want.
        /// </summary>
        /// <param name="cmdText">Any sql text</param>
        /// <param name="trans">Transaction</param>
        /// <returns>Any data list</returns>
        public List<dynamic> List(string cmdText, DbTransaction trans, CommandType cmdType = CommandType.Text,
            params DbParameter[] cmdParms)
        {
            if (trans == null)
            {
                return NativeSql.ExecuteDynamic(cmdText, cmdType, cmdParms);
            }
            else
            {
                return NativeSql.ExecuteDynamic(trans, cmdText, cmdType, cmdParms);
            }
        }
        /// <summary>
        /// Easy to access database,you can get any data that you want.
        /// </summary>
        /// <param name="cmdText">Any sql text</param>
        /// <param name="trans">Transaction</param>
        /// <returns>Any data list</returns>
        public List<dynamic> List(string cmdText,CommandType cmdType = CommandType.Text,
            params DbParameter[] cmdParms)
        {
            return NativeSql.ExecuteDynamic(cmdText, cmdType, cmdParms);
        }
        /// <summary>
        ///  Load data  list
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="by">Sql,All,Top,Page,Between</param>
        /// <returns></returns>
        public List<T> List<T>(By by) where T :  new()
        {
            #region BySql
            if (by is BySql)
            {
                var bySql = by as BySql;
                var trans = bySql.Tran;
                var cmdText = bySql.CmdText;
                var cmdType = bySql.CmdType;
                var cmdParms = bySql.CmdParms;

                var list = new List<T>();

                Type type = typeof(T);
                var typeName = type.FullName;
                var infos = IntrospectionManager.GetColumns(typeName);
                DbDataReader dr = null;
                if (trans == null)
                {
                    dr = NativeSql.ExecuteReader(cmdText, cmdType, cmdParms);
                }
                else
                {
                    dr = NativeSql.ExecuteReader(trans, cmdText, cmdType, cmdParms);
                }
                while (dr.Read())
                {
                    T entity = new T();
                    foreach (var propertyName in infos.Keys)
                    {
                        var fieldName = infos[propertyName];
                        if (dr.GetSchemaTable().Select("columnname='" + fieldName + "'").Length > 0)
                        {
                            object columnValue = dr[fieldName];
                            if (columnValue is System.DBNull)
                            {
                                columnValue = null;
                            }
                            IntrospectionManager.GetSetDelegate(typeName)(entity, propertyName, columnValue);
                        }
                        
                       /* object columnValue = dr[fieldName];
                        if (columnValue is DBNull)
                        {
                            columnValue = null;
                        }
                       IntrospectionManager.GetSetDelegate(typeName)(entity, propertyName, columnValue);*/
                    }
                    list.Add(entity);
                }
                dr.Close();
                dr.Dispose();
                return list;
            } 
            #endregion
            #region ByAll
            else if (by is ByAll)
            {
                var byAll = by as ByAll;
                var asc = byAll.Asc;
                var trans = byAll.Trans;

                string _left = ProviderManager.GetProvider(ProviderName).GetLeftEscape();
                string _right = ProviderManager.GetProvider(ProviderName).GetRightEscape();
                string sql;
                var typeName = typeof(T).FullName;
                var tableName = IntrospectionManager.GetTableName(typeName);
                var pk = IntrospectionManager.GetPrimaryColumnName(typeName);
                if (asc)
                {
                    sql = string.Format("select * from {0}", _left + tableName + _right);
                }
                else
                {
                    sql = string.Format("select * from {0} order by {1} desc", _left + tableName + _right, _left + pk + _right);
                }
                return List<T>(By.Sql(sql, trans));
            } 
            #endregion
            #region ByTop
            else if (by is ByTop)
            {
                var byTop = by as ByTop;
                var top = byTop.Top;
                var asc = byTop.Asc;
                var trans = byTop.Trans;

                var typeName = typeof(T).FullName;
                var pk = IntrospectionManager.GetPrimaryColumnName(typeName);
                var tableName = IntrospectionManager.GetTableName(typeName);
                string sql = ProviderManager.GetProvider(ProviderName).GetTopSql(top, asc, tableName, pk);

                return List<T>(By.Sql(sql, trans));
            } 
            #endregion
            #region ByPage
            else if (by is ByPage)
            {
                var byPage = by as ByPage;
                var page = byPage.Page;
                var pageSize = byPage.PageSize;
                var asc = byPage.Asc;
                var trans = byPage.Trans;

                var typeName = typeof(T).FullName;
                var pk = IntrospectionManager.GetPrimaryColumnName(typeName);
                var tableName = IntrospectionManager.GetTableName(typeName);
                string sql = ProviderManager.GetProvider(ProviderName).GetPageSql(page, pageSize, asc, tableName, pk);
                return List<T>(By.Sql(sql, trans));
            } 
            #endregion
            #region ByBetween
            else if (by is ByBetween)
            {
                var byBetween = by as ByBetween;
                var from = byBetween.From;
                var to = byBetween.To;
                var asc = byBetween.Asc;
                var trans = byBetween.Trans;

                string _left = ProviderManager.GetProvider(ProviderName).GetLeftEscape();
                string _right = ProviderManager.GetProvider(ProviderName).GetRightEscape();
                var typeName = typeof(T).FullName;
                var pk = IntrospectionManager.GetPrimaryColumnName(typeName);
                var tableName = IntrospectionManager.GetTableName(typeName);
                string sql;
                if (asc)
                {
                    sql = string.Format("select * from {0} where {1} between {2} and {3}", _left + tableName + _right,
                                        _left + pk + _right, from, to);
                }
                else
                {
                    sql = string.Format("select * from {0} where {1} between {2} and {3} order by {1}", _left + tableName + _right,
                                        _left + pk + _right, from, to);
                }
                return List<T>(By.Sql(sql, trans));
            } 
            #endregion
            else
            {
                throw new NotSupportThisByException(by == null ? "null" : by.ToString());
            }
        }
        /// <summary>
        /// Betch delete
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="list">Entity list</param>
        /// <param name="trans">Transaction</param>
        public virtual void Del<T>(List<T> list,DbTransaction trans=null) where T : Entity
        {
            var sql = new StringBuilder();
            var isNeedCommit = false;
            var isSupportBatch = ProviderManager.GetProvider(ProviderName).IsSupportBatch;
            if (isSupportBatch)
            {
                sql.Append(ProviderManager.GetProvider(ProviderName).BatchBegin);
            }
            else if (trans == null)
            {
                var conn = NativeSql.GetConn();
                conn.Open();
                trans = conn.BeginTransaction();
                isNeedCommit = true;
            }
            foreach (var item in list)
            {
                item.Key = Key;
                var sqlT = item.GetDelSql();
                
                if (isSupportBatch)
                {
                    sql.Append(sqlT);
                }
                else
                {
                    NativeSql.ExecuteNonQuery(trans, sqlT);
                }
            }
            if (isSupportBatch)
            {
                sql.Append(ProviderManager.GetProvider(ProviderName).BatchEnd);

                if (trans == null)
                {
                    NativeSql.ExecuteNonQuery(sql.ToString());

                }
                else
                {
                    NativeSql.ExecuteNonQuery(trans, sql.ToString());
                }
            }
            if (!isSupportBatch && isNeedCommit)
            {
                var conn = trans.Connection;
                trans.Commit();
                trans.Dispose();
                conn.Close();
                conn.Dispose();
            }
        }
        /// <summary>
        /// Betch delete
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="trans">Transaction</param>
        /// <param name="arr">Entity params</param>
        public virtual void Del<T>(DbTransaction trans,params T[] arr) where T : Entity
        {
            Del(arr.ToList(), trans);
        }
        /// <summary>
        /// Betch delete
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="arr">Entity params</param>
        public virtual void Del<T>(params T[] arr) where T : Entity
        {
            Del(arr.ToList());
        }
        /// <summary>
        /// Betch update
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="list">Entity list</param>
        /// <param name="trans">Transaction</param>
        public virtual void Update<T>(List<T> list,DbTransaction trans=null) where T : Entity
        {
            var sql = new StringBuilder();
            var isNeedCommit = false;
            var isSupportBatch = ProviderManager.GetProvider(ProviderName).IsSupportBatch;
            if (isSupportBatch)
            {
                sql.Append(ProviderManager.GetProvider(ProviderName).BatchBegin);
            }
            else if (trans == null)
            {
                var conn = NativeSql.GetConn();
                conn.Open();
                trans = conn.BeginTransaction();
                isNeedCommit = true;
            }

            foreach (var item in list)
            {
                item.Key = Key;
                var sqlT = item.GetUpdateSql();

                if (isSupportBatch)
                {
                    sql.Append(sqlT);
                }
                else
                {
                    NativeSql.ExecuteNonQuery(trans, sqlT);
                }
            }

            if (isSupportBatch)
            {
                sql.Append(ProviderManager.GetProvider(ProviderName).BatchEnd);

                if (trans == null)
                {
                    NativeSql.ExecuteNonQuery(sql.ToString());
                }
                else
                {
                    NativeSql.ExecuteNonQuery(trans, sql.ToString());
                }
            }
            if (!isSupportBatch && isNeedCommit)
            {
                var conn = trans.Connection;
                trans.Commit();
                trans.Dispose();
                conn.Close();
                conn.Dispose();
                
            }
        }
        /// <summary>
        /// Betch update
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="trans">Transaction</param>
        /// <param name="arr">Entity params</param>
        public virtual void Update<T>(DbTransaction trans,params T[] arr) where T : Entity
        {
            Update(arr.ToList(), trans);
        }
        /// <summary>
        /// Betch update
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="arr">Entity params</param>
        public virtual void Update<T>(params T[] arr) where T : Entity
        {
            Update(arr.ToList());
        }
        /// <summary>
        /// Betch insert
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="list">Entity list</param>
        /// <param name="isIdAutoGrow">Is id auto grow?</param>
        /// <param name="trans">Transaction</param>
        public virtual void Save<T>(List<T> list, bool isIdAutoGrow = true, DbTransaction trans=null) where T : Entity
        {
            var sql = new StringBuilder();
            var typeName = typeof(T).FullName;
            var pk = IntrospectionManager.GetPrimaryMemberName(typeName);
            var setValue = IntrospectionManager.GetSetDelegate(typeName);

            var isNeedCommit = false;
            var isSupportBatch = ProviderManager.GetProvider(ProviderName).IsSupportBatch;
            if (isSupportBatch && ProviderName != DataBaseNames.Oracle)
            {
                sql.Append(ProviderManager.GetProvider(ProviderName).BatchBegin);
            }
            else if (trans == null)
            {
                var conn = NativeSql.GetConn();
                conn.Open();
                trans = conn.BeginTransaction();
                isNeedCommit = true;
            }

            foreach (var item in list)
            {
                item.Key = Key;
                var sqlT = item.GetSaveSql(isIdAutoGrow);
                if (isSupportBatch && ProviderName != DataBaseNames.Oracle)
                {
                    sql.Append(sqlT);
                    if (isIdAutoGrow)
                    {
                        sql.Append(item.GetLastInsertIdSql());
                    }
                }
                else
                {
                    if (ProviderName == DataBaseNames.Oracle)
                    {
                        sqlT = sqlT.Remove(sqlT.LastIndexOf(";"), 1);
                    }
                    NativeSql.ExecuteNonQuery(trans, sqlT);
                    if (isIdAutoGrow)
                    {
                        object idValue = NativeSql.ExecuteScalar(trans, item.GetLastInsertIdSql());
                        if (idValue != null)
                        {
                            var pkType = IntrospectionManager.GetPrimaryKeyType(typeName);
                            setValue(item, pk, idValue.ChangeType(pkType));
                        }
                    }
                }
                
            }

            if (isSupportBatch && ProviderName != DataBaseNames.Oracle)
            {
                sql.Append(ProviderManager.GetProvider(ProviderName).BatchEnd);

                DataSet ds;
                if (trans == null)
                {
                    ds = NativeSql.ExecuteDataSet(sql.ToString());

                }
                else
                {
                    ds = NativeSql.ExecuteDataSet(trans, sql.ToString());
                }

                if (isIdAutoGrow)
                {
                    var pkType = IntrospectionManager.GetPrimaryKeyType(typeName);
                    for (int i = 0; i < list.Count; i++)
                    {
                        object idValue = ds.Tables[i].Rows[0][0];
                        setValue(list[i], pk, idValue.ChangeType(pkType));
                    }
                }
            }

            if (!isSupportBatch && isNeedCommit)
            {
                var conn = trans.Connection;
                trans.Commit();
                trans.Dispose();
                conn.Close();
                conn.Dispose();
            }
        }

        /// <summary>
        /// Betch insert,isIdAutoGrow = true
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="trans">Transaction</param>
        /// <param name="arr">Entity params</param>
        public virtual void Save<T>(DbTransaction trans,params T[] arr) where T : Entity
        {
            Save(arr.ToList(),true,trans);
        }
        /// <summary>
        /// Betch insert,isIdAutoGrow = true
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="trans">Transaction</param>
        /// <param name="list">Entity list</param>
        public virtual void Save<T>(DbTransaction trans, List<T> list) where T : Entity
        {
            Save(trans, list.ToArray());
        }
        /// <summary>
        /// Betch insert,isIdAutoGrow = true
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="arr">Entity params</param>
        public virtual void Save<T>(params T[] arr) where T : Entity
        {
            Save(arr.ToList());
        }
        /// <summary>
        /// Betch insert
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="isIdAutoGrow">Is id auto grow?</param>
        /// <param name="arr">Entity params</param>
        public virtual void Save<T>(bool isIdAutoGrow, params T[] arr) where T : Entity
        {
            Save(arr.ToList(), isIdAutoGrow);
        }
        /// <summary>
        /// Betch insert
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="isIdAutoGrow">Is id auto grow?</param>
        /// <param name="trans">Transaction</param>
        /// <param name="arr">Entity params</param>
        public virtual void Save<T>(bool isIdAutoGrow ,DbTransaction trans, params T[] arr) where T : Entity
        {
            Save(arr.ToList(), isIdAutoGrow, trans);
        }

        /// <summary>
        /// Betch insert,don't return id value
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="isIdAutoGrow">Is id auto grow?</param>
        /// <param name="list">Entity list</param>
        public virtual void SaveOnly<T>(List<T> list, bool isIdAutoGrow = true, DbTransaction trans = null)
            where T : Entity
        {
            var sql = new StringBuilder();
            var isNeedCommit = false;
            var isSupportBatch = ProviderManager.GetProvider(ProviderName).IsSupportBatch;
            if (isSupportBatch)
            {
                sql.Append(ProviderManager.GetProvider(ProviderName).BatchBegin);
            }
            else if (trans == null)
            {
                var conn = NativeSql.GetConn();
                conn.Open();
                trans = conn.BeginTransaction();
                isNeedCommit = true;
            }

            foreach (var item in list)
            {
                item.Key = Key;
                var sqlT = item.GetSaveSql(isIdAutoGrow);
                if (isSupportBatch)
                {
                    sql.Append(sqlT);
                }
                else
                {
                    NativeSql.ExecuteNonQuery(trans, sqlT);
                }
            }

            if (isSupportBatch)
            {
                sql.Append(ProviderManager.GetProvider(ProviderName).BatchEnd);

                if (trans == null)
                {
                    NativeSql.ExecuteNonQuery(sql.ToString());
                }
                else
                {
                    NativeSql.ExecuteNonQuery(trans, sql.ToString());
                }
            }

            if (!isSupportBatch && isNeedCommit)
            {
                var conn = trans.Connection;
                trans.Commit();
                trans.Dispose();
                conn.Close();
                conn.Dispose();
            }
        }
        /// <summary>
        /// Betch insert,isIdAutoGrow = true,don't return id value
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="trans">Transaction</param>
        /// <param name="arr">Entity params</param>
        public virtual void SaveOnly<T>(DbTransaction trans, params T[] arr) where T : Entity
        {
            SaveOnly(arr.ToList(), true, trans);
        }
        /// <summary>
        /// Betch insert,isIdAutoGrow = true,don't return id value
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="trans">Transaction</param>
        /// <param name="list">Entity list</param>
        public virtual void SaveOnly<T>(DbTransaction trans, List<T> list) where T : Entity
        {
            SaveOnly(trans,list.ToArray());
        }
        /// <summary>
        /// Betch insert,don't return id value
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="isIdAutoGrow">Is id auto grow?</param>
        /// <param name="trans">Transaction</param>
        /// <param name="arr">Entity params</param>
        public virtual void SaveOnly<T>(bool isIdAutoGrow, DbTransaction trans, params T[] arr) where T : Entity
        {
            SaveOnly(arr.ToList(), isIdAutoGrow, trans);
        }
        /// <summary>
        /// Betch insert,isIdAutoGrow = true,don't return id value
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="arr">Entity params</param>
        public virtual void SaveOnly<T>(params T[] arr) where T : Entity
        {
            SaveOnly(arr.ToList());
        }
        /// <summary>
        /// Betch insert,don't return id value
        /// </summary>
        /// <typeparam name="T">Entity</typeparam>
        /// <param name="isIdAutoGrow">Is id auto grow?</param>
        /// <param name="arr">Entity params</param>
        public virtual void SaveOnly<T>(bool isIdAutoGrow, params T[] arr) where T : Entity
        {
            SaveOnly(arr.ToList(), isIdAutoGrow);
        }

        /// <summary>
        /// Table record count
        /// </summary>
        public static int Count<T>() where T : Entity, new()
        {
            var t = new T();
            string sql = "select count(1) from " + t.Escape(t.TableName);
            return int.Parse(Sql(t.GetKey()).ExecuteScalar(sql).ToString());
        }

        /// <summary>
        /// Start transaction
        /// </summary>
        /// <param name="isolationLevel">transaction level</param>
        /// <returns></returns>
        public virtual DbTransaction BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            throwTransactionIsRunningException();
            _tranactionStatus = TransactionStatus.Running;
            _dbConnection = NativeSql.GetConn();
            _dbConnection.Open();
            _dbTransaction = _dbConnection.BeginTransaction(isolationLevel);
            return _dbTransaction;
        }
        /// <summary>
        /// Commit transaction
        /// </summary>
        public virtual void Commit()
        {
            throwTransactionIsNormalException();
            _tranactionStatus = TransactionStatus.Normal;
            _dbTransaction.Commit();
            if (_dbConnection.State == ConnectionState.Open)
            {
                _dbConnection.Close();
                _dbConnection.Dispose();
            }
        }
        /// <summary>
        /// Rollback transaction
        /// </summary>
        public virtual void Rollback()
        {
            throwTransactionIsNormalException();
            _tranactionStatus = TransactionStatus.Normal;
            _dbTransaction.Rollback();
            if (_dbConnection.State == ConnectionState.Open)
            {
                _dbConnection.Close();
                _dbConnection.Dispose();
            }
        }

        private  void throwTransactionIsRunningException()
        {
            if (_tranactionStatus == TransactionStatus.Running)
            {
                throw new Exception("Your entityPool tranaction is running!Please commit entityPool tranaction first!");
            }
        }
        private  void throwTransactionIsNormalException()
        {
            if (_tranactionStatus == TransactionStatus.Normal)
            {
                throw new Exception("Must BeginTransaction First!");
            }
        }
    }

    public enum TransactionStatus
    {
        Normal=0,Running
    }

    public abstract class EntityPool<T> : EntityPool where T : EntityPool,new ()
    {
        static EntityPool()
        {
            var entityPoolType = typeof(T);
            var attributes = entityPoolType.GetCustomAttributes(false).Where(x => x is DataBaseAttribute);
            if (attributes.Any())
            {
                var dBAttribute = (DataBaseAttribute)attributes.FirstOrDefault();
                _instance = new T();
                _instance.Key = dBAttribute.Key;
            }
            else
            {
                throw new ClassNotHaveDataBaseAttributeException(entityPoolType.Name);
            }
        }

        private static T _instance = null;
        public static T Instance
        {
            get { return _instance; }
        }

        public List<X> Query<X>(string cmdText, DbTransaction tran, CommandType cmdType = CommandType.Text,
            params DbParameter[] cmdParms) where X : new()
        {
            return List<X>(By.Sql(cmdText, tran, cmdType));
        }

        public List<X> Query<X>(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms) where X : new()
        {
            return List<X>(By.Sql(cmdText, cmdType));
        }

        public X QueryFirstOrDefault<X>(string cmdText, DbTransaction tran, CommandType cmdType = CommandType.Text,
            params DbParameter[] cmdParms) where X : Entity, new()
        {
            return Query<X>(cmdText, tran, cmdType, cmdParms).FirstOrDefault();
        }

        public X QueryFirstOrDefault<X>(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms) where X :  new()
        {
            return Query<X>(cmdText, cmdType, cmdParms).FirstOrDefault();
        }

        public List<dynamic> Query(string cmdText, DbTransaction tran, CommandType cmdType = CommandType.Text,
            params DbParameter[] cmdParms)
        {
            return List(cmdText, tran, cmdType, cmdParms);
        }

        public List<dynamic> Query(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            return List(cmdText, cmdType, cmdParms);
        }

        public dynamic QueryFirstOrDefault(string cmdText, DbTransaction tran, CommandType cmdType = CommandType.Text,
            params DbParameter[] cmdParms)
        {
            return Query(cmdText, tran, cmdType, cmdParms).FirstOrDefault();
        }

        public dynamic QueryFirstOrDefault(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            return Query(cmdText, cmdType, cmdParms).FirstOrDefault();
        }

        public X ExecuteScalar<X>(string cmdText, DbTransaction tran, CommandType cmdType = CommandType.Text,
            params DbParameter[] cmdParms) where X :new()
        {
            object result;
            if (tran != null)
            {
                 result = NativeSql.ExecuteScalar(tran, cmdText, cmdType, cmdParms);
            }
            else
            {
                 result = NativeSql.ExecuteScalar(cmdText, cmdType, cmdParms);
            }
            
            
            return (X)Convert.ChangeType(result, typeof(X));
        }

        public X ExecuteScalar<X>(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms) where X : new()
        {
            return ExecuteScalar<X>(cmdText, null, cmdType, cmdParms);
        }

        public int ExecuteNonQuery(string cmdText, DbTransaction tran, CommandType cmdType = CommandType.Text,
            params DbParameter[] cmdParms)
        {
            if (tran != null)
            {
               return NativeSql.ExecuteNonQuery(tran, cmdText, cmdType, cmdParms);
            }
            else
            {
                return NativeSql.ExecuteNonQuery(cmdText, cmdType, cmdParms);
            }
        }

        public int ExecuteNonQuery(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            return ExecuteNonQuery(cmdText, null, cmdType, cmdParms);
        }
    }
}
