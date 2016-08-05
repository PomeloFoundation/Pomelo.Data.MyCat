// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if !NET451
using System;

namespace Pomelo.Data.MyCat
{
    public class MyCatTrace
    {
        /*private static string qaHost;
        private static bool qaEnabled = false;*/

        public static TraceListenerCollection Listeners
        {
            get { throw new NotImplementedException(); }
        }

        public static SourceSwitch Switch
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public static bool QueryAnalysisEnabled
        {
            get { throw new NotImplementedException(); }
        }

        public static void EnableQueryAnalyzer(string host, int postInterval)
        {
            throw new NotImplementedException();
        }

        public static void DisableQueryAnalyzer()
        {
            throw new NotImplementedException();
        }

        public static TraceSource Source
        {
            get { throw new NotImplementedException(); }
        }

        public static void LogInformation(int id, string msg)
        {
          //TODO implement
        }

        public static void LogWarning(int id, string msg)
        {
          //TODO implement
        }

        public static void LogError(int id, string msg)
        {
            //TODO implement
        }

        public static void TraceEvent(TraceEventType eventType, MyCatTraceEventType mysqlEventType, string msgFormat, params object[] args)
        {
            throw new NotImplementedException();
        }
    }

    // These types are just placeholders to keep same api for MyCatTrace in both .NET & RT (ie. they supply missing types in .NET for RT).
    public enum TraceEventType
    {
        Critical = 1,
        Error = 2,
        Warning = 3,
        Information = 4,
        Verbose = 5
    }

    public class TraceListenerCollection
    {
    }

    public class TraceSource
    {
        public void TraceEvent(TraceEventType type, int id, string msg, MyCatTraceEventType type2, int code)
        {
            throw new NotImplementedException();
        }

        public void TraceEvent(TraceEventType type, int id, string msg, params object[] args)
        {
            throw new NotImplementedException();
        }
    }

    public class SourceSwitch
    {

    }

    internal static class Trace
    {
        internal static void TraceInformation(string msg)
        {
            throw new NotImplementedException();
        }

        internal static void TraceWarning(string msg)
        {
            throw new NotImplementedException();
        }

        internal static void TraceError(string msg)
        {
            throw new NotImplementedException();
        }

        internal static void TraceEven(string msg)
        {
            throw new NotImplementedException();
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