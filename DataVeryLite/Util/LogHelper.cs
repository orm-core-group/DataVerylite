using System;
using System.Diagnostics;
using DataVeryLite.Core;

namespace DataVeryLite.Util
{
    public class LogHelper
    {
        static LogHelper()
        {
            TraceSwitch.Level = Configure.LogLevel;
        }

        private static readonly TraceSwitch TraceSwitch = new TraceSwitch("DataVeryLite", "In the Config file");

        public static void LogVerbose(string msg)
        {
            if (Configure.EnableLog && TraceSwitch.TraceVerbose)
            {
                Trace.WriteLineIf(TraceSwitch.TraceVerbose, string.Format("{{[Verbose][{0}] {1}}}", DateTime.Now, msg));
                Trace.Flush();
            }
        }

        public static void LogInfo(string msg)
        {
            if (Configure.EnableLog && TraceSwitch.TraceInfo)
            {
                Trace.WriteLineIf(TraceSwitch.TraceInfo, string.Format("{{[Info][{0}] {1}}}", DateTime.Now, msg));
                Trace.Flush();
            }
            
        }

        public static void LogWarning(string msg)
        {
            if (Configure.EnableLog && TraceSwitch.TraceWarning)
            {
                Trace.WriteLineIf(TraceSwitch.TraceWarning, string.Format("{{[Warning][{0}] {1}}}", DateTime.Now, msg));
                Trace.Flush();
            }
        }

        public static void LogError(string msg)
        {
            if (Configure.EnableLog && TraceSwitch.TraceError)
            {
                Trace.WriteLineIf(TraceSwitch.TraceError, string.Format("{{[Error][{0}] {1}}}", DateTime.Now, msg));
                Trace.Flush();
            }
        }
    }
}