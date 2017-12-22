using System.Data;
using System.Data.Common;
using DataVeryLite.Bys;

namespace DataVeryLite.Core
{
    public abstract class By
    {
        /// <summary>
        /// Condition for this entity by any sql text,can use transaction
        /// </summary>
        /// <param name="cmdText">Any sql text</param>
        /// <param name="tran">Transaction</param>
        /// <param name="cmdType">Sql text type</param>
        /// <param name="cmdParms">Parameter</param>
        /// <returns></returns>
        public static BySql Sql(string cmdText, DbTransaction tran, CommandType cmdType = CommandType.Text,params DbParameter[] cmdParms)
        {
            return new BySql {CmdText = cmdText, Tran = tran, CmdType = cmdType, CmdParms = cmdParms};
        }
        /// <summary>
        /// Condition for this entity by any sql text
        /// </summary>
        /// <param name="cmdText">Any sql text</param>
        /// <param name="cmdType">Sql text type</param>
        /// <param name="cmdParms">Parameter</param>
        /// <returns></returns>
        public static BySql Sql(string cmdText, CommandType cmdType = CommandType.Text, params DbParameter[] cmdParms)
        {
            return Sql(cmdText, null, cmdType, cmdParms);
        }
        /// <summary>
        /// Condition for this entity
        /// </summary>
        /// <param name="idValue">A id value in your table,suppurt long,int & short etc...</param>
        /// <param name="tran">Transaction</param>
        /// <returns></returns>
        public static ById Id(object idValue, DbTransaction tran = null)
        {
            return new ById {IdValue = idValue, Tran = tran};
        }
        public static ByAttach Attach(object idValue, params string[] fields)
        {
            return new ByAttach {IdValue = idValue, Fields = fields};
        }

        /// <summary>
        ///  Load all data
        /// </summary>
        /// <param name="asc">Order by</param>
        /// <param name="trans">Transaction</param>
        /// <returns></returns>
        public static ByAll All(bool asc = true, DbTransaction trans = null)
        {
            return new ByAll {Asc = asc, Trans = trans};
        }
        /// <summary>
        /// Load data top
        /// </summary>
        /// <param name="top">Top value</param>
        /// <param name="asc">Order by</param>
        /// <param name="trans">Transaction</param>
        /// <returns></returns>
        public static ByTop Top(int top, bool asc = true, DbTransaction trans = null)
        {
            return new ByTop {Top = top, Asc = asc, Trans = trans};
        }
        /// <summary>
        /// Load data page
        /// </summary>
        /// <param name="page">Page value</param>
        /// <param name="pageSize">Page size</param>
        /// <param name="asc">Order by</param>
        /// <param name="trans">Transaction</param>
        /// <returns></returns>
        public static ByPage Page(int page, int pageSize, bool asc = true, DbTransaction trans = null)
        {
            return new ByPage {Page = page, PageSize = pageSize, Asc = asc, Trans = trans};
        }
        /// <summary>
        /// Load data bettween
        /// </summary>
        /// <param name="from">From value</param>
        /// <param name="to">To value</param>
        /// <param name="asc">Order by</param>
        /// <param name="trans">Transaction</param>
        /// <returns></returns>
        public static ByBetween Between(int from, int to, bool asc = true, DbTransaction trans = null)
        {
            return new ByBetween {From = from, To = to, Asc = asc, Trans = trans};
        }
        /* public static ByAttach Partly(DbTransaction transaction, params string[] fields)
        {
            return new ByAttach() {Fields = fields};
        }

        public static ByAttach Partly(params string[] fields)
        {
            return new ByAttach() {  Fields = fields };
        }

        public static void Detach(object idValue, params string[] fields)
        {
            throw new NotImplementedException();
        }*/
    }
}
