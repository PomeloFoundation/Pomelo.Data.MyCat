// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if NET451
using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Reflection;


namespace Pomelo.Data.MyCat
{
    public sealed partial class MyCatTrace  
    {
        private static TraceSource source = new TraceSource("mysql");
        //protected static string qaHost;
        private static bool qaEnabled = false;

        static MyCatTrace()
        {

            foreach (TraceListener listener in source.Listeners)
            {
                if (listener.GetType().ToString().Contains("MyCat.EMTrace.EMTraceListener"))
                {
                    qaEnabled = true;
                    break;
                }
            }
        }

        public static TraceListenerCollection Listeners
        {
            get { return source.Listeners; }
        }

        public static SourceSwitch Switch
        {
            get { return source.Switch; }
            set { source.Switch = value; }
        }

        public static bool QueryAnalysisEnabled
        {
            get { return qaEnabled; }
        }

        public static void EnableQueryAnalyzer(string host, int postInterval)
        {
            if (qaEnabled) return;
            // create a EMTraceListener and add it to our source
            TraceListener l = (TraceListener)Activator.CreateInstance("MyCat.EMTrace",
                "MyCat.EMTrace.EMTraceListener", false, BindingFlags.CreateInstance,
                null, new object[] { host, postInterval }, null, null).Unwrap();
            if (l == null)
                throw new MyCatException(Resources.UnableToEnableQueryAnalysis);
            source.Listeners.Add(l);
            Switch.Level = SourceLevels.All;
        }

        public static void DisableQueryAnalyzer()
        {
            qaEnabled = false;
            foreach (TraceListener l in source.Listeners)
                if (l.GetType().ToString().Contains("EMTraceListener"))
                {
                    source.Listeners.Remove(l);
                    break;
                }
        }

        internal static TraceSource Source
        {
            get { return source; }
        }

        internal static void LogInformation(int id, string msg)
        {
            Source.TraceEvent(TraceEventType.Information, id, msg, MyCatTraceEventType.NonQuery, -1);
            Trace.TraceInformation(msg);
        }

        internal static void LogWarning(int id, string msg)
        {
            Source.TraceEvent(TraceEventType.Warning, id, msg, MyCatTraceEventType.NonQuery, -1);
            Trace.TraceWarning(msg);
        }

        internal static void LogError(int id, string msg)
        {
            Source.TraceEvent(TraceEventType.Error, id, msg, MyCatTraceEventType.NonQuery, -1);
            Trace.TraceError(msg);
        }

        internal static void TraceEvent(TraceEventType eventType,
            MyCatTraceEventType mysqlEventType, string msgFormat, params object[] args)
        {
            Source.TraceEvent(eventType, (int)mysqlEventType, msgFormat, args);
        }
    }

    public enum UsageAdvisorWarningFlags
    {
        NoIndex = 1,
        BadIndex,
        SkippedRows,
        SkippedColumns,
        FieldConversion
    }

    public enum MyCatTraceEventType : int
    {
        ConnectionOpened = 1,
        ConnectionClosed,
        QueryOpened,
        ResultOpened,
        ResultClosed,
        QueryClosed,
        StatementPrepared,
        StatementExecuted,
        StatementClosed,
        NonQuery,
        UsageAdvisorWarning,
        Warning,
        Error,
        QueryNormalized
    }
}
#endif