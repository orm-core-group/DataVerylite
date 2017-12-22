using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace DataVeryLite.Core
{
    /// <summary>
    /// Goble setting
    /// </summary>
    public static class Configure
    {
        public static Delegates.SetKeyHander SetKey;

        public static bool EnableLog = true;

        public static TraceLevel LogLevel = TraceLevel.Warning;

        public static bool EnableSync = true;

        internal static bool EnableLogSql = true;

        internal static TraceLevel LogSqlLevel = TraceLevel.Verbose;
    }
}
