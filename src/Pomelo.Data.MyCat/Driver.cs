// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.Globalization;
using System.Text;
using Pomelo.Data.Common;
using Pomelo.Data.Types;
using System.Diagnostics;
using System.Collections.Generic;
using System.Security;

namespace Pomelo.Data.MyCat
{
    /// <summary>
    /// Summary description for BaseDriver.
    /// </summary>
    internal class Driver : IDisposable
    {
        protected Encoding encoding;
        protected MyCatConnectionStringBuilder connectionString;
        protected bool isOpen;
        protected DateTime creationTime;
        protected string serverCharSet;
        protected int serverCharSetIndex;
        protected Dictionary<string, string> serverProps;
        protected Dictionary<int, string> charSets;
        protected long maxPacketSize;
        internal int timeZoneOffset;
        private DateTime idleSince;

#if !NETSTANDARD1_3
        protected MyCatPromotableTransaction currentTransaction;
        protected bool inActiveUse;
#endif
        protected MyCatPool pool;
        private bool firstResult;
        protected IDriver handler;
        internal List<MyCatDataReader> reader = new List<MyCatDataReader>();
        private bool disposeInProgress;
        private bool disposeComplete;
        internal bool isFabric;

        /// <summary>
        /// For pooled connections, time when the driver was
        /// put into idle queue
        /// </summary>
        public DateTime IdleSince
        {
            get { return idleSince; }
            set { idleSince = value; }
        }

        public Driver(MyCatConnectionStringBuilder settings)
        {
            encoding = Encoding.UTF8;
            if (encoding == null)
                throw new MyCatException(Resources.DefaultEncodingNotFound);
            connectionString = settings;
            serverCharSet = "utf8";
            serverCharSetIndex = -1;
            maxPacketSize = 1024;
            handler = new NativeDriver(this);
        }

        ~Driver()
        {
            Dispose(false);
        }

        #region Properties

        public int ThreadID
        {
            get { return handler.ThreadId; }
        }

        public DBVersion Version
        {
            get { return handler.Version; }
        }

        public MyCatConnectionStringBuilder Settings
        {
            get { return connectionString; }
            set { connectionString = value; }
        }

        public Encoding Encoding
        {
            get { return encoding; }
            set { encoding = value; }
        }

#if !NETSTANDARD1_3
        public MyCatPromotableTransaction CurrentTransaction
        {
            get { return currentTransaction; }
            set { currentTransaction = value; }
        }

        public bool IsInActiveUse
        {
            get { return inActiveUse; }
            set { inActiveUse = value; }
        }
#endif
        public bool IsOpen
        {
            get { return isOpen; }
        }

        public MyCatPool Pool
        {
            get { return pool; }
            set { pool = value; }
        }

        public long MaxPacketSize
        {
            get { return maxPacketSize; }
        }

        internal int ConnectionCharSetIndex
        {
            get { return serverCharSetIndex; }
            set { serverCharSetIndex = value; }
        }

        internal Dictionary<int, string> CharacterSets
        {
            get { return charSets; }
        }

        public bool SupportsOutputParameters
        {
            get { return Version.isAtLeast(5, 5, 0); }
        }

        public bool SupportsBatch
        {
            get { return (handler.Flags & ClientFlags.MULTI_STATEMENTS) != 0; }
        }

        public bool SupportsConnectAttrs
        {
            get { return (handler.Flags & ClientFlags.CONNECT_ATTRS) != 0; }
        }

        public bool SupportsPasswordExpiration
        {
            get { return (handler.Flags & ClientFlags.CAN_HANDLE_EXPIRED_PASSWORD) != 0; }
        }

        public bool IsPasswordExpired { get; internal set; }

        #endregion

        public string Property(string key)
        {
            return (string)serverProps[key];
        }

        public bool ConnectionLifetimeExpired()
        {
            TimeSpan ts = DateTime.Now.Subtract(creationTime);
            if (Settings.ConnectionLifeTime != 0 &&
              ts.TotalSeconds > Settings.ConnectionLifeTime)
                return true;
            return false;
        }

        public static Driver Create(MyCatConnectionStringBuilder settings)
        {
            Driver d = null;
#if !NETSTANDARD1_3
            try
            {
                if (MyCatTrace.QueryAnalysisEnabled || settings.Logging || settings.UseUsageAdvisor)
                    d = new TracingDriver(settings);
            }
            catch (TypeInitializationException ex)
            {
                if (!(ex.InnerException is SecurityException))
                    throw ex;
                //Only rethrow if InnerException is not a SecurityException. If it is a SecurityException then 
                //we couldn't initialize MyCatTrace because we don't have unmanaged code permissions. 
            }
#else
      if (settings.Logging || settings.UseUsageAdvisor)
      {
        throw new NotImplementedException( "Logging not supported in this WinRT release." );
      }
#endif
            if (d == null)
                d = new Driver(settings);

            //this try was added as suggested fix submitted on MyCat Bug 72025, socket connections are left in CLOSE_WAIT status when connector fails to open a new connection.
            //the bug is present when the client try to get more connections that the server support or has configured in the max_connections variable.
            try
            {
                d.Open();
            }
            catch (Exception ex)
            {
                d.Dispose();
                throw ex;
            }
            return d;
        }

        public bool HasStatus(ServerStatusFlags flag)
        {
            return (handler.ServerStatus & flag) != 0;
        }

        public virtual void Open()
        {
            creationTime = DateTime.Now;
            handler.Open();
            isOpen = true;
        }

        public virtual void Close()
        {
            Dispose();
        }

        public virtual void Configure(MyCatConnection connection)
        {
            bool firstConfigure = false;

            // if we have not already configured our server variables
            // then do so now
            if (serverProps == null)
            {
                firstConfigure = true;

                // if we are in a pool and the user has said it's ok to cache the
                // properties, then grab it from the pool
                try
                {
                    if (Pool != null && Settings.CacheServerProperties)
                    {
                        if (Pool.ServerProperties == null)
                            Pool.ServerProperties = LoadServerProperties(connection);
                        serverProps = Pool.ServerProperties;
                    }
                    else
                        serverProps = LoadServerProperties(connection);

                    LoadCharacterSets(connection);
                }
                catch (MyCatException ex)
                {
                    // expired password capability
                    if (ex.Number == 1820)
                    {
                        IsPasswordExpired = true;
                        return;
                    }
                    throw;
                }
            }


#if AUTHENTICATED
      string licenseType = serverProps["license"];
      if (licenseType == null || licenseType.Length == 0 || 
        licenseType != "commercial") 
        throw new MyCatException( "This client library licensed only for use with commercially-licensed MySQL servers." );
#endif
            // if the user has indicated that we are not to reset
            // the connection and this is not our first time through,
            // then we are done.
            if (!Settings.ConnectionReset && !firstConfigure) return;

            string charSet = connectionString.CharacterSet;
            if (charSet == null || charSet.Length == 0)
            {
                if (serverCharSetIndex >= 0 && charSets.ContainsKey(serverCharSetIndex))
                    charSet = (string)charSets[serverCharSetIndex];
                else
                    charSet = serverCharSet;
            }

            if (serverProps.ContainsKey("max_allowed_packet"))
                maxPacketSize = Convert.ToInt64(serverProps["max_allowed_packet"]);

            // now tell the server which character set we will send queries in and which charset we
            // want results in
            MyCatCommand charSetCmd = new MyCatCommand("SET character_set_results=NULL",
                              connection);
            charSetCmd.InternallyCreated = true;

            string clientCharSet;
            serverProps.TryGetValue("character_set_client", out clientCharSet);
            string connCharSet;
            serverProps.TryGetValue("character_set_connection", out connCharSet);
            if ((clientCharSet != null && clientCharSet.ToString() != charSet) ||
              (connCharSet != null && connCharSet.ToString() != charSet))
            {
                MyCatCommand setNamesCmd = new MyCatCommand("SET NAMES " + charSet, connection);
                setNamesCmd.InternallyCreated = true;
                setNamesCmd.ExecuteNonQuery();
            }
            charSetCmd.ExecuteNonQuery();

            if (charSet != null)
                Encoding = CharSetMap.GetEncoding(Version, charSet);
            else
                Encoding = CharSetMap.GetEncoding(Version, "utf8");

            handler.Configure();
        }

        /// <summary>
        /// Loads the properties from the connected server into a hashtable
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        private Dictionary<string, string> LoadServerProperties(MyCatConnection connection)
        {
            // load server properties
            Dictionary<string, string> hash = new Dictionary<string, string>();
            MyCatCommand cmd = new MyCatCommand("SHOW VARIABLES", connection);
            try
            {
                using (MyCatDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string key = reader.GetString(0);
                        string value = reader.GetString(1);
                        hash[key] = value;
                    }
                }
                // Get time zone offset as numerical value
                timeZoneOffset = TimeZoneInfo.Local.BaseUtcOffset.Hours;
                return hash;
            }
            catch (Exception ex)
            {
                MyCatTrace.LogError(ThreadID, ex.Message);
                throw;
            }
        }

        private int GetTimeZoneOffset(MyCatConnection con)
        {
            MyCatCommand cmd = new MyCatCommand("select timediff( curtime(), utc_time() )", con);
            string s = cmd.ExecuteScalar() as string;
            if (s == null) s = "0:00";

            return int.Parse(s.Substring(0, s.IndexOf(':')));
        }

        /// <summary>
        /// Loads all the current character set names and ids for this server 
        /// into the charSets hashtable
        /// </summary>
        private void LoadCharacterSets(MyCatConnection connection)
        {
            MyCatCommand cmd = new MyCatCommand("SHOW COLLATION", connection);

            // now we load all the currently active collations
            try
            {
                using (MyCatDataReader reader = cmd.ExecuteReader())
                {
                    charSets = new Dictionary<int, string>();
                    while (reader.Read())
                    {
                        charSets[Convert.ToInt32(reader["id"], NumberFormatInfo.InvariantInfo)] =
                          reader.GetString(reader.GetOrdinal("charset"));
                    }
                }
            }
            catch (Exception ex)
            {
                MyCatTrace.LogError(ThreadID, ex.Message);
                throw;
            }
        }

        public virtual List<MyCatError> ReportWarnings(MyCatConnection connection)
        {
            List<MyCatError> warnings = new List<MyCatError>();

            MyCatCommand cmd = new MyCatCommand("SHOW WARNINGS", connection);
            cmd.InternallyCreated = true;
            using (MyCatDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    warnings.Add(new MyCatError(reader.GetString(0),
                                  reader.GetInt32(1), reader.GetString(2)));
                }
            }

            MyCatInfoMessageEventArgs args = new MyCatInfoMessageEventArgs();
            args.errors = warnings.ToArray();
            if (connection != null)
                connection.OnInfoMessage(args);
            return warnings;
        }

        public virtual void SendQuery(MyCatPacket p)
        {
            handler.SendQuery(p);
            firstResult = true;
        }

        public virtual ResultSet NextResult(int statementId, bool force)
        {
            if (!force && !firstResult && !HasStatus(ServerStatusFlags.AnotherQuery | ServerStatusFlags.MoreResults))
                return null;
            firstResult = false;

            var affectedRows = -1L;
            long insertedId = -1;
            var fieldCount = GetResult(statementId, ref affectedRows, ref insertedId);
            if (fieldCount == -1)
                return null;
            if (fieldCount > 0)
                return new ResultSet(this, statementId, fieldCount);
            else
                return new ResultSet(affectedRows, insertedId);
        }

        protected virtual long GetResult(int statementId, ref long affectedRows, ref long insertedId)
        {
            return handler.GetResult(ref affectedRows, ref insertedId);
        }

        public virtual bool FetchDataRow(int statementId, int columns)
        {
            return handler.FetchDataRow(statementId, columns);
        }

        public virtual bool SkipDataRow()
        {
            return FetchDataRow(-1, 0);
        }

        public virtual void ExecuteDirect(string sql)
        {
            MyCatPacket p = new MyCatPacket(Encoding);
            p.WriteString(sql);
            SendQuery(p);
            NextResult(0, false);
        }

        public MyCatField[] GetColumns(long count)
        {
            MyCatField[] fields = new MyCatField[count];
            for (int i = 0; i < count; i++)
                fields[i] = new MyCatField(this);
            handler.GetColumnsData(fields);

            return fields;
        }

        public virtual int PrepareStatement(string sql, ref MyCatField[] parameters)
        {
            return handler.PrepareStatement(sql, ref parameters);
        }

        public IMyCatValue ReadColumnValue(int index, MyCatField field, IMyCatValue value)
        {
            return handler.ReadColumnValue(index, field, value);
        }

        public void SkipColumnValue(IMyCatValue valObject)
        {
            handler.SkipColumnValue(valObject);
        }

        public void ResetTimeout(int timeoutMilliseconds)
        {
            handler.ResetTimeout(timeoutMilliseconds);
        }

        public bool Ping()
        {
            return handler.Ping();
        }

        public virtual void SetDatabase(string dbName)
        {
            handler.SetDatabase(dbName);
        }

        public virtual void ExecuteStatement(MyCatPacket packetToExecute)
        {
            handler.ExecuteStatement(packetToExecute);
        }


        public virtual void CloseStatement(int id)
        {
            handler.CloseStatement(id);
        }

        public virtual void Reset()
        {
            handler.Reset();
        }

        public virtual void CloseQuery(MyCatConnection connection, int statementId)
        {
            if (handler.WarningCount > 0)
                ReportWarnings(connection);
        }

        #region IDisposable Members

        protected virtual void Dispose(bool disposing)
        {
            // Avoid cyclic calls to Dispose.
            if (disposeInProgress || disposeComplete)
                return;

            disposeInProgress = true;

            try
            {
                ResetTimeout(1000);
                if (disposing)
                    handler.Close(isOpen);
                // if we are pooling, then release ourselves
                if (connectionString.Pooling)
                    MyCatPoolManager.RemoveConnection(this);
            }
            catch (Exception)
            {
                if (disposing)
                    throw;
            }
            finally
            {
                if (reader != null)
                {
                    foreach (var x in reader)
                        x.Dispose();
                    reader.Clear();
                }
                isOpen = false;
                disposeComplete = true;
                disposeInProgress = false;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }

    internal interface IDriver
    {
        int ThreadId { get; }
        DBVersion Version { get; }
        ServerStatusFlags ServerStatus { get; }
        ClientFlags Flags { get; }
        void Configure();
        void Open();
        void SendQuery(MyCatPacket packet);
        void Close(bool isOpen);
        bool Ping();
        long GetResult(ref long affectedRows, ref long insertedId);
        bool FetchDataRow(int statementId, int columns);
        int PrepareStatement(string sql, ref MyCatField[] parameters);
        void ExecuteStatement(MyCatPacket packet);
        void CloseStatement(int statementId);
        void SetDatabase(string dbName);
        void Reset();
        IMyCatValue ReadColumnValue(int index, MyCatField field, IMyCatValue valObject);
        void SkipColumnValue(IMyCatValue valueObject);
        void GetColumnsData(MyCatField[] columns);
        void ResetTimeout(int timeout);
        int WarningCount { get; }
    }
}
