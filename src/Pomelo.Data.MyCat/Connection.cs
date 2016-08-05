// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
#if NET451
using System.Transactions;
using IsolationLevel = System.Data.IsolationLevel;
#endif
using Pomelo.Data.Common;

#if SUPPORT_REPLICATION
using Pomelo.Data.MyCat.Replication;
#endif
using System.Threading.Tasks;
using System.Threading;

namespace Pomelo.Data.MyCat
{
    public partial class MyCatConnection : IDisposable
    {
        internal ConnectionState connectionState;
        internal Driver driver;
        internal bool hasBeenOpen;
        private SchemaProvider schemaProvider;
        private ProcedureCache procedureCache;
        private bool isInUse;
        private PerformanceMonitor perfMonitor;
#if !NETSTANDARD1_3
        private ExceptionInterceptor exceptionInterceptor;
        internal CommandInterceptor commandInterceptor;
#endif
        private bool isKillQueryConnection;
        private string database;
        private int commandTimeout;

        /// <include file='docs/MyCatConnection.xml' path='docs/InfoMessage/*'/>
        public event MyCatInfoMessageEventHandler InfoMessage;

        private static Cache<string, MyCatConnectionStringBuilder> connectionStringCache =
          new Cache<string, MyCatConnectionStringBuilder>(0, 25);

        /// <include file='docs/MyCatConnection.xml' path='docs/DefaultCtor/*'/>
        public MyCatConnection()
        {
            //TODO: add event data to StateChange docs
            Settings = new MyCatConnectionStringBuilder();
            database = String.Empty;
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/Ctor1/*'/>
        public MyCatConnection(string connectionString)
          : this()
        {
            ConnectionString = connectionString;
        }

        #region Destructor
        ~MyCatConnection()
        {
#if !RT
            Dispose(false);
#else
      Dispose();
#endif
        }
        #endregion

        #region Interal Methods & Properties
        
        internal PerformanceMonitor PerfMonitor
        {
            get { return perfMonitor; }
        }

        internal ProcedureCache ProcedureCache
        {
            get { return procedureCache; }
        }

        internal MyCatConnectionStringBuilder Settings { get; private set; }

        public List<MyCatDataReader> Reader
        {
            get
            {
                if (driver == null)
                    return null;
                if (driver.reader == null)
                    driver.reader = new List<MyCatDataReader>();
                return driver.reader;
            }
            set
            {
                driver.reader = value;
                isInUse = driver.reader != null;
            }
        }

        internal void OnInfoMessage(MyCatInfoMessageEventArgs args)
        {
            if (InfoMessage != null)
            {
                InfoMessage(this, args);
            }
        }

        internal bool SoftClosed
        {
            get
            {
#if !NETSTANDARD1_3
                return (State == ConnectionState.Closed) &&
                  driver != null &&
                  driver.CurrentTransaction != null;
#else
                return false;
#endif
            }
        }

        internal bool IsInUse
        {
            get { return isInUse; }
            set { isInUse = value; }
        }

        #endregion

        #region Properties

        /// <summary>
        /// Returns the id of the server thread this connection is executing on
        /// </summary>
        [Browsable(false)]
        public int ServerThread
        {
            get { return driver.ThreadID; }
        }

        /// <summary>
        /// Gets the name of the MySQL server to which to connect.
        /// </summary>
        [Browsable(true)]
        public override string DataSource
        {
            get { return Settings.Server; }
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/ConnectionTimeout/*'/>
        [Browsable(true)]
        public override int ConnectionTimeout
        {
            get { return (int)Settings.ConnectionTimeout; }
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/Database/*'/>
        [Browsable(true)]
        public override string Database
        {
            get { return database; }
        }

        /// <summary>
        /// Indicates if this connection should use compression when communicating with the server.
        /// </summary>
        [Browsable(false)]
        public bool UseCompression
        {
            get { return Settings.UseCompression; }
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/State/*'/>
        [Browsable(false)]
        public override ConnectionState State
        {
            get { return connectionState; }
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/ServerVersion/*'/>
        [Browsable(false)]
        public override string ServerVersion
        {
            get { return driver.Version.ToString(); }
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/ConnectionString/*'/>
        // [Editor("Pomelo.Data.MyCat.Design.ConnectionStringTypeEditor,MyCatClient.Design", typeof(UITypeEditor))]
        [Browsable(true)]
        [Category("Data")]
        [Description(
          "Information used to connect to a DataSource, such as 'Server=xxx;UserId=yyy;Password=zzz;Database=dbdb'.")]
        public override string ConnectionString
        {
            get
            {
                // Always return exactly what the user set.
                // Security-sensitive information may be removed.
                return Settings.GetConnectionString(!hasBeenOpen || Settings.PersistSecurityInfo);
            }
            set
            {
                if (State != ConnectionState.Closed)
                    Throw(new MyCatException(
                      "Not allowed to change the 'ConnectionString' property while the connection (state=" + State + ")."));

                MyCatConnectionStringBuilder newSettings;
                lock (connectionStringCache)
                {
                    if (value == null)
                        newSettings = new MyCatConnectionStringBuilder();
                    else
                    {
                        newSettings = (MyCatConnectionStringBuilder)connectionStringCache[value];
                        if (null == newSettings)
                        {
                            newSettings = new MyCatConnectionStringBuilder(value);
                            connectionStringCache.Add(value, newSettings);
                        }
                    }
                }

                Settings = newSettings;

                if (Settings.Database != null && Settings.Database.Length > 0)
                    this.database = Settings.Database;

                if (driver != null)
                    driver.Settings = newSettings;
            }
        }

        public bool IsPasswordExpired { get { return driver.IsPasswordExpired; } }

        #endregion

        partial void AssertPermissions();

        #region Transactions

#if NET451
        /// <summary>
        /// Enlists in the specified transaction. 
        /// </summary>
        /// <param name="transaction">
        /// A reference to an existing <see cref="System.Transactions.Transaction"/> in which to enlist.
        /// </param>
        public override void EnlistTransaction(Transaction transaction)
        {
            // enlisting in the null transaction is a noop
            if (transaction == null)
                return;

            // guard against trying to enlist in more than one transaction
            if (driver.CurrentTransaction != null)
            {
                if (driver.CurrentTransaction.BaseTransaction == transaction)
                    return;

                Throw(new MyCatException("Already enlisted"));
            }

            // now see if we need to swap out drivers.  We would need to do this since
            // we have to make sure all ops for a given transaction are done on the
            // same physical connection.
            Driver existingDriver = DriverTransactionManager.GetDriverInTransaction(transaction);
            if (existingDriver != null)
            {
                // we can't allow more than one driver to contribute to the same connection
                if (existingDriver.IsInActiveUse)
                    Throw(new NotSupportedException(Resources.MultipleConnectionsInTransactionNotSupported));

                // there is an existing driver and it's not being currently used.
                // now we need to see if it is using the same connection string
                string text1 = existingDriver.Settings.ConnectionString;
                string text2 = Settings.ConnectionString;
                if (String.Compare(text1, text2, true) != 0)
                    Throw(new NotSupportedException(Resources.MultipleConnectionsInTransactionNotSupported));

                // close existing driver
                // set this new driver as our existing driver
                CloseFully();
                driver = existingDriver;
            }

            if (driver.CurrentTransaction == null)
            {
                MyCatPromotableTransaction t = new MyCatPromotableTransaction(this, transaction);
                if (!transaction.EnlistPromotableSinglePhase(t))
                    Throw(new NotSupportedException(Resources.DistributedTxnNotSupported));

                driver.CurrentTransaction = t;
                DriverTransactionManager.SetDriverInTransaction(driver);
                driver.IsInActiveUse = true;
            }
        }
#endif

        /// <include file='docs/MyCatConnection.xml' path='docs/BeginTransaction/*'/>
        public new MyCatTransaction BeginTransaction()
        {
            return BeginTransaction(IsolationLevel.RepeatableRead);
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/BeginTransaction1/*'/>
        public new MyCatTransaction BeginTransaction(IsolationLevel iso)
        {
            //TODO: check note in help
            if (State != ConnectionState.Open)
                Throw(new InvalidOperationException(Resources.ConnectionNotOpen));

            // First check to see if we are in a current transaction
            if (driver.HasStatus(ServerStatusFlags.InTransaction))
                Throw(new InvalidOperationException(Resources.NoNestedTransactions));

            MyCatTransaction t = new MyCatTransaction(this, iso);

            MyCatCommand cmd = new MyCatCommand("", this);

            cmd.CommandText = "SET SESSION TRANSACTION ISOLATION LEVEL ";
            switch (iso)
            {
                case IsolationLevel.ReadCommitted:
                    cmd.CommandText += "READ COMMITTED";
                    break;
                case IsolationLevel.ReadUncommitted:
                    cmd.CommandText += "READ UNCOMMITTED";
                    break;
                case IsolationLevel.RepeatableRead:
                    cmd.CommandText += "REPEATABLE READ";
                    break;
                case IsolationLevel.Serializable:
                    cmd.CommandText += "SERIALIZABLE";
                    break;
                case IsolationLevel.Chaos:
                    Throw(new NotSupportedException(Resources.ChaosNotSupported));
                    break;
                case IsolationLevel.Snapshot:
                    Throw(new NotSupportedException(Resources.SnapshotNotSupported));
                    break;
            }

            cmd.ExecuteNonQuery();

            cmd.CommandText = "BEGIN";
            cmd.ExecuteNonQuery();

            return t;
        }

        #endregion

        /// <include file='docs/MyCatConnection.xml' path='docs/ChangeDatabase/*'/>
        public override void ChangeDatabase(string databaseName)
        {
            if (databaseName == null || databaseName.Trim().Length == 0)
                Throw(new ArgumentException(Resources.ParameterIsInvalid, "databaseName"));

            if (State != ConnectionState.Open)
                Throw(new InvalidOperationException(Resources.ConnectionNotOpen));

            // This lock  prevents promotable transaction rollback to run
            // in parallel
            lock (driver)
            {
#if !NETSTANDARD1_3
                if (Transaction.Current != null &&
                  Transaction.Current.TransactionInformation.Status == TransactionStatus.Aborted)
                {
                    Throw(new TransactionAbortedException());
                }
#endif
                // We use default command timeout for SetDatabase
                using (new CommandTimer(this, (int)Settings.DefaultCommandTimeout))
                {
                    driver.SetDatabase(databaseName);
                }
            }
            this.database = databaseName;
        }

        internal void SetState(ConnectionState newConnectionState, bool broadcast)
        {
            if (newConnectionState == connectionState && !broadcast)
                return;
            ConnectionState oldConnectionState = connectionState;
            connectionState = newConnectionState;
            if (broadcast)
                OnStateChange(new StateChangeEventArgs(oldConnectionState, connectionState));
        }

        /// <summary>
        /// Ping
        /// </summary>
        /// <returns></returns>
        public bool Ping()
        {
            if (Reader != null)
                Throw(new MyCatException(Resources.DataReaderOpen));
            if (driver != null && driver.Ping())
                return true;
            driver = null;
            SetState(ConnectionState.Closed, true);
            return false;
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/Open/*'/>
        public override void Open()
        {
            if (State == ConnectionState.Open)
                Throw(new InvalidOperationException(Resources.ConnectionAlreadyOpen));

#if !NETSTANDARD1_3
            // start up our interceptors
            exceptionInterceptor = new ExceptionInterceptor(this);
            commandInterceptor = new CommandInterceptor(this);
#endif

            SetState(ConnectionState.Connecting, true);

            AssertPermissions();

#if !NETSTANDARD1_3
            // if we are auto enlisting in a current transaction, then we will be
            // treating the connection as pooled
            if (Settings.AutoEnlist && Transaction.Current != null)
            {
                driver = DriverTransactionManager.GetDriverInTransaction(Transaction.Current);
                if (driver != null &&
                  (driver.IsInActiveUse ||
                  !driver.Settings.EquivalentTo(this.Settings)))
                    Throw(new NotSupportedException(Resources.MultipleConnectionsInTransactionNotSupported));
            }
#endif

            try
            {
                MyCatConnectionStringBuilder currentSettings = Settings;
#if SUPPORT_REPLICATION

        // Load balancing 
        if (ReplicationManager.IsReplicationGroup(Settings.Server))
        {
          if (driver == null)
          {
            ReplicationManager.GetNewConnection(Settings.Server, false, this);
          }
          else
            currentSettings = driver.Settings;
        }
#endif
#if !NETSTANDARD1_3
                if (Settings.Pooling)
                {
                    MyCatPool pool = MyCatPoolManager.GetPool(currentSettings);
                    if (driver == null || !driver.IsOpen)
                        driver = pool.GetConnection();
                    procedureCache = pool.ProcedureCache;

                }
                else
                {
                    if (driver == null || !driver.IsOpen)
                        driver = Driver.Create(currentSettings);
                    procedureCache = new ProcedureCache((int)Settings.ProcedureCacheSize);
                }
#else
                if (driver == null || !driver.IsOpen)
                        driver = Driver.Create(currentSettings);
                    procedureCache = new ProcedureCache((int)Settings.ProcedureCacheSize);
#endif
            }
            catch (Exception ex)
            {
                SetState(ConnectionState.Closed, true);
                throw ex;
            }

            SetState(ConnectionState.Open, false);
            driver.Configure(this);

            if (!(driver.SupportsPasswordExpiration && driver.IsPasswordExpired))
            {
                if (Settings.Database != null && Settings.Database != String.Empty)
                    ChangeDatabase(Settings.Database);
            }

            // setup our schema provider
            schemaProvider = new ISSchemaProvider(this);
            perfMonitor = new PerformanceMonitor(this);

            // if we are opening up inside a current transaction, then autoenlist
            // TODO: control this with a connection string option
#if !NETSTANDARD1_3
            if (Transaction.Current != null && Settings.AutoEnlist)
                EnlistTransaction(Transaction.Current);
#endif

            hasBeenOpen = true;
            SetState(ConnectionState.Open, true);
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/CreateCommand/*'/>
        public new MyCatCommand CreateCommand()
        {
            // Return a new instance of a command object.
            MyCatCommand c = new MyCatCommand();
            c.Connection = this;
            return c;
        }

        /// <summary>
        /// Creates a new MyCatConnection object with the exact same ConnectionString value
        /// </summary>
        /// <returns>A cloned MyCatConnection object</returns>
        public object Clone()
        {
            MyCatConnection clone = new MyCatConnection();
            string connectionString = Settings.ConnectionString;
            if (connectionString != null)
                clone.ConnectionString = connectionString;
            return clone;
        }

        internal void Abort()
        {
            try
            {
                driver.Close();
            }
            catch (Exception ex)
            {
                MyCatTrace.LogWarning(ServerThread, String.Concat("Error occurred aborting the connection. Exception was: ", ex.Message));
            }
            finally
            {
                this.isInUse = false;
            }
            SetState(ConnectionState.Closed, true);
        }

        internal void CloseFully()
        {
            if (Settings.Pooling && driver.IsOpen)
            {
                // if we are in a transaction, roll it back
                if (driver.HasStatus(ServerStatusFlags.InTransaction))
                {
                    MyCatTransaction t = new MyCatTransaction(this, IsolationLevel.Unspecified);
                    t.Rollback();
                }

                MyCatPoolManager.ReleaseConnection(driver);
            }
            driver.Close();
            driver = null;
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/Close/*'/>
        public override void Close()
        {
            if (driver != null)
                driver.IsPasswordExpired = false;

            if (State == ConnectionState.Closed) return;

            if (Reader != null)
#if !NETSTANDARD1_3
                foreach (var x in Reader)
                    x.Close();
#else
                foreach (var x in Reader)
                    x.Dispose();
#endif

            // if the reader was opened with CloseConnection then driver
            // will be null on the second time through
            if (driver != null)
            {
#if !NETSTANDARD1_3
                if (driver.CurrentTransaction == null)
#endif
                CloseFully();
#if !NETSTANDARD1_3
                else
                    driver.IsInActiveUse = false;
#endif
            }

            SetState(ConnectionState.Closed, true);
        }

        internal string CurrentDatabase()
        {
            if (Database != null && Database.Length > 0)
                return Database;
            MyCatCommand cmd = new MyCatCommand("SELECT database()", this);
            return cmd.ExecuteScalar().ToString();
        }



        internal void HandleTimeoutOrThreadAbort(Exception ex)
        {
            bool isFatal = false;

            if (isKillQueryConnection)
            {
                // Special connection started to cancel a query.
                // Abort will prevent recursive connection spawning
                Abort();
                if (ex is TimeoutException)
                {
                    Throw(new MyCatException(Resources.Timeout, true, ex));
                }
                else
                {
                    return;
                }
            }

            try
            {

                // Do a fast cancel.The reason behind small values for connection
                // and command timeout is that we do not want user to wait longer
                // after command has already expired.
                // Microsoft's SqlClient seems to be using 5 seconds timeouts 
                // here as well.
                // Read the  error packet with "interrupted" message.
                CancelQuery(5);
                driver.ResetTimeout(5000);
                if (Reader != null)
                {
#if NETSTANDARD1_3
                    foreach(var x in Reader)
                        x.Dispose();
#else
                    foreach (var x in Reader)
                        x.Close();
#endif
                    Reader = null;
                }
            }
            catch (Exception ex2)
            {
                MyCatTrace.LogWarning(ServerThread, "Could not kill query, " +
                  " aborting connection. Exception was " + ex2.Message);
                Abort();
                isFatal = true;
            }
            if (ex is TimeoutException)
            {
                Throw(new MyCatException(Resources.Timeout, isFatal, ex));
            }
        }

        public void CancelQuery(int timeout)
        {
            MyCatConnectionStringBuilder cb = new MyCatConnectionStringBuilder(
              Settings.ConnectionString);
            cb.Pooling = false;
            cb.AutoEnlist = false;
            cb.ConnectionTimeout = (uint)timeout;

            using (MyCatConnection c = new MyCatConnection(cb.ConnectionString))
            {
                c.isKillQueryConnection = true;
                c.Open();
                string commandText = "KILL QUERY " + ServerThread;
                MyCatCommand cmd = new MyCatCommand(commandText, c);
                cmd.CommandTimeout = timeout;
                cmd.ExecuteNonQuery();
            }
        }

#region Routines for timeout support.

        // Problem description:
        // Sometimes, ExecuteReader is called recursively. This is the case if
        // command behaviors are used and we issue "set sql_select_limit" 
        // before and after command. This is also the case with prepared 
        // statements , where we set session variables. In these situations, we 
        // have to prevent  recursive ExecuteReader calls from overwriting 
        // timeouts set by the top level command.

        // To solve the problem, SetCommandTimeout() and ClearCommandTimeout() are 
        // introduced . Query timeout here is  "sticky", that is once set with 
        // SetCommandTimeout, it only be overwritten after ClearCommandTimeout 
        // (SetCommandTimeout would return false if it timeout has not been 
        // cleared).

        // The proposed usage pattern of there routines is following: 
        // When timed operations starts, issue SetCommandTimeout(). When it 
        // finishes, issue ClearCommandTimeout(), but _only_ if call to 
        // SetCommandTimeout() was successful.


        /// <summary>
        /// Sets query timeout. If timeout has been set prior and not
        /// yet cleared ClearCommandTimeout(), it has no effect.
        /// </summary>
        /// <param name="value">timeout in seconds</param>
        /// <returns>true if </returns>
        internal bool SetCommandTimeout(int value)
        {
            if (!hasBeenOpen)
                // Connection timeout is handled by driver
                return false;

            if (commandTimeout != 0)
                // someone is trying to set a timeout while command is already
                // running. It could be for example recursive call to ExecuteReader
                // Ignore the request, as only top-level (non-recursive commands)
                // can set timeouts.
                return false;

            if (driver == null)
                return false;

            commandTimeout = value;
            driver.ResetTimeout(commandTimeout * 1000);
            return true;
        }

        /// <summary>
        /// Clears query timeout, allowing next SetCommandTimeout() to succeed.
        /// </summary>
        internal void ClearCommandTimeout()
        {
            if (!hasBeenOpen)
                return;
            commandTimeout = 0;
            if (driver != null)
            {
                driver.ResetTimeout(0);
            }
        }
#endregion

        public MyCatSchemaCollection GetSchemaCollection(string collectionName, string[] restrictionValues)
        {
            if (collectionName == null)
                collectionName = SchemaProvider.MetaCollection;

            string[] restrictions = schemaProvider.CleanRestrictions(restrictionValues);
            MyCatSchemaCollection c = schemaProvider.GetSchema(collectionName, restrictions);
            return c;
        }

#region Pool Routines

        /// <include file='docs/MyCatConnection.xml' path='docs/ClearPool/*'/>
        public static void ClearPool(MyCatConnection connection)
        {
            MyCatPoolManager.ClearPool(connection.Settings);
        }

        /// <include file='docs/MyCatConnection.xml' path='docs/ClearAllPools/*'/>
        public static void ClearAllPools()
        {
            MyCatPoolManager.ClearAllPools();
        }

#endregion

        internal void Throw(Exception ex)
        {
#if !NETSTANDARD1_3
            if (exceptionInterceptor == null)
                throw ex;
            exceptionInterceptor.Throw(ex);
#else
            throw ex;
#endif
        }

#if !RT
        public new void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
#else
    public void Dispose()
    {
      if (State == ConnectionState.Open)
        Close();

      GC.SuppressFinalize(this);
    }
#endif

#if NET_40_OR_GREATER
#region Async
    /// <summary>
    /// Async version of BeginTransaction
    /// </summary>
    /// <returns>An object representing the new transaction.</returns>
    public Task<MyCatTransaction> BeginTransactionAsync()
    {
      return BeginTransactionAsync(IsolationLevel.RepeatableRead, CancellationToken.None);
    }

    public Task<MyCatTransaction> BeginTransactionAsync(CancellationToken cancellationToken)
    {
      return BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);
    }

    /// <summary>
    /// Async version of BeginTransaction
    /// </summary>
    /// <param name="iso">The isolation level under which the transaction should run. </param>
    /// <returns>An object representing the new transaction.</returns>
    public Task<MyCatTransaction> BeginTransactionAsync(IsolationLevel iso)
    {
      return BeginTransactionAsync(iso, CancellationToken.None);
    }

    public Task<MyCatTransaction> BeginTransactionAsync(IsolationLevel iso, CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<MyCatTransaction>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          MyCatTransaction tranResult = BeginTransaction(iso);
          result.SetResult(tranResult);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      else
      {
        result.SetCanceled();
      }

      return result.Task;
    }

    public Task ChangeDataBaseAsync(string databaseName)
    {
      return ChangeDataBaseAsync(databaseName, CancellationToken.None);
    }

    /// <summary>
    /// Async version of ChangeDataBase
    /// </summary>
    /// <param name="databaseName">The name of the database to use.</param>
    /// <returns></returns>
    public Task ChangeDataBaseAsync(string databaseName, CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<bool>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          ChangeDatabase(databaseName);
          result.SetResult(true);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      return result.Task;
    }

    ///// <summary>
    ///// Async version of Open
    ///// </summary>
    ///// <returns></returns>
    //public Task OpenAsync()
    //{
    //  return Task.Run(() =>
    //  {
    //    Open();
    //  });
    //}

    /// <summary>
    /// Async version of Close
    /// </summary>
    /// <returns></returns>
    public Task CloseAsync()
    {
      return CloseAsync(CancellationToken.None);
    }

    public Task CloseAsync(CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<bool>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          Close();
          result.SetResult(true);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      else 
      {
        result.SetCanceled();
      }
      return result.Task;
    }

    /// <summary>
    /// Async version of ClearPool
    /// </summary>
    /// <param name="connection">The connection associated with the pool to be cleared.</param>
    /// <returns></returns>
    public Task ClearPoolAsync(MyCatConnection connection)
    {
      return ClearPoolAsync(connection, CancellationToken.None);
    }

    public Task ClearPoolAsync(MyCatConnection connection, CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<bool>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          ClearPool(connection);
          result.SetResult(true);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      else
      {
        result.SetCanceled();
      }
      return result.Task;
    }

    /// <summary>
    /// Async version of ClearAllPools
    /// </summary>
    /// <returns></returns>
    public Task ClearAllPoolsAsync()
    {
      return ClearAllPoolsAsync(CancellationToken.None);
    }

    public Task ClearAllPoolsAsync(CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<bool>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          ClearAllPools();
          result.SetResult(true);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      else
      {
        result.SetCanceled();
      }
      return result.Task;
    }
    /// <summary>
    /// Async version of GetSchemaCollection
    /// </summary>
    /// <param name="collectionName">Name of the collection</param>
    /// <param name="restrictionValues">Values to restrict</param>
    /// <returns>A schema collection</returns>
    public Task<MyCatSchemaCollection> GetSchemaCollectionAsync(string collectionName, string[] restrictionValues)
    {
      return GetSchemaCollectionAsync(collectionName, restrictionValues, CancellationToken.None);
    }

    public Task<MyCatSchemaCollection> GetSchemaCollectionAsync(string collectionName, string[] restrictionValues, CancellationToken cancellationToken)
    {
      var result = new TaskCompletionSource<MyCatSchemaCollection>();
      if (cancellationToken == CancellationToken.None || !cancellationToken.IsCancellationRequested)
      {
        try
        {
          var schema = GetSchemaCollection(collectionName, restrictionValues);
          result.SetResult(schema);
        }
        catch (Exception ex)
        {
          result.SetException(ex);
        }
      }
      else
      {
        result.SetCanceled();
      }
      return result.Task;
    }
#endregion
#endif
    }

    /// <summary>
    /// Represents the method that will handle the <see cref="MyCatConnection.InfoMessage"/> event of a 
    /// <see cref="MyCatConnection"/>.
    /// </summary>
    public delegate void MyCatInfoMessageEventHandler(object sender, MyCatInfoMessageEventArgs args);

    /// <summary>
    /// Provides data for the InfoMessage event. This class cannot be inherited.
    /// </summary>
    public class MyCatInfoMessageEventArgs : EventArgs
    {
        /// <summary>
        /// 
        /// </summary>
        public MyCatError[] errors;
    }

    /// <summary>
    /// IDisposable wrapper around SetCommandTimeout and ClearCommandTimeout
    /// functionality
    /// </summary>
    internal class CommandTimer : IDisposable
    {
        bool timeoutSet;
        MyCatConnection connection;

        public CommandTimer(MyCatConnection connection, int timeout)
        {
            this.connection = connection;
            if (connection != null)
            {
                timeoutSet = connection.SetCommandTimeout(timeout);
            }
        }

#region IDisposable Members
        public void Dispose()
        {
            if (timeoutSet)
            {
                timeoutSet = false;
                connection.ClearCommandTimeout();
                connection = null;
            }
        }
#endregion
    }
}
