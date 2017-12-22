using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using DataVeryLite.Core;

namespace DataVeryLite.Util
{
    public static class ModelHelper
    {
        public static string Generate()
        {
            var sqlHelpers = IntrospectionManager.GetSqlHelpers();
            var csharpCode = new StringBuilder();
            csharpCode.Append("using System;"+Environment.NewLine);
            csharpCode.Append("using DataVeryLite.Util;" + Environment.NewLine);
            var defaultNameSpace = Assembly.GetCallingAssembly().FullName.Split(',')[0];
            csharpCode.AppendFormat("namespace {0}.{1}" + Environment.NewLine, defaultNameSpace, "Model");
            csharpCode.Append("{"+ Environment.NewLine);
            foreach (var sqlHelper in sqlHelpers.Values)
            {
                var dataBaseName = sqlHelper.DataBaseName;
                var dataBaseType = sqlHelper.DataBaseType;
                var key = sqlHelper.Key;
                csharpCode.AppendFormat("namespace {0}" + Environment.NewLine, dataBaseName);
                csharpCode.Append("{" + Environment.NewLine);
                csharpCode.AppendFormat("[DataVeryLite.DataBase(Name = \"{0}\",Key = \"{1}\")]"+ Environment.NewLine, dataBaseName, key);
                csharpCode.AppendFormat("public class {0}:{1}<{2}>"+ Environment.NewLine,dataBaseName, ProviderManager.GetEntityPool(dataBaseType),dataBaseName);
                csharpCode.Append("{" + Environment.NewLine);
                csharpCode.Append("}" + Environment.NewLine);
                List<dynamic> tables = sqlHelper.GetTables();
                foreach (var table in tables)
                {
                    var idValue = sqlHelper.GetPrimaryKey(table.name);
                    csharpCode.AppendFormat("[DataVeryLite.Table(Name = \"{0}\",EntityPool = typeof({1}))]"+ Environment.NewLine, table.name,dataBaseName);
                    csharpCode.AppendFormat("public partial class {0} : {1}"+ Environment.NewLine, table.name,ProviderManager.GetEntity(dataBaseType));
                    csharpCode.Append("{" + Environment.NewLine);
                    List<dynamic> columns = sqlHelper.GetColumns(table.name);
                    foreach (var column in columns)
                    {
                        if (column.name == idValue)
                        {
                            csharpCode.AppendFormat("[DataVeryLite.Column(Name = \"{0}\",IsPrimaryKey = true)]"+ Environment.NewLine,column.name);
                        }
                        else
                        {
                            csharpCode.AppendFormat("[DataVeryLite.Column(Name = \"{0}\")]"+ Environment.NewLine, column.name);
                        }
                        csharpCode.AppendFormat("public {0} {1} {2} get; set; {3}",sqlHelper.GetColumnType(table.name, column.name), column.name,"{","}");
                    }
                    csharpCode.Append("}" + Environment.NewLine);
                }
                csharpCode.Append("}" + Environment.NewLine);
            }
            csharpCode.Append("}" + Environment.NewLine);
            return csharpCode.ToString();
        }
    }
}
