using System.Data;
using System.Data.Common;
using DataVeryLite.Core;

namespace DataVeryLite.Bys
{
    public class BySql : By
    {
        public string CmdText { get; set; }
        public DbTransaction Tran { get; set; }
        public CommandType CmdType { get; set; }
        public DbParameter[] CmdParms { get; set; }
    }
}
