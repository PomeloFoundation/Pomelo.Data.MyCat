// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.IO;
using System.Linq;
using System.Collections;
using System.Text;
using System.Data;
using System.Data.Common;
using Pomelo.Data.Common;
using System.ComponentModel;
using System.Threading;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;
#if SUPPORT_REPLICATION
using Pomelo.Data.MyCat.Replication;
#endif

namespace Pomelo.Data.MyCat
{
    /// <include file='docs/mysqlcommand.xml' path='docs/ClassSummary/*'/> 
#if NETSTANDARD1_3
    public sealed partial class MyCatCommand
#else
    public sealed partial class MyCatCommand : ICloneable, IDisposable
#endif

    {
        MyCatConnection connection;
        MyCatTransaction curTransaction;
        string cmdText;
        CommandType cmdType;
        MyCatParameterCollection parameters;
        private IAsyncResult asyncResult;
        internal Int64 lastInsertedId;
        private PreparableStatement statement;
        private int commandTimeout;
        private bool canceled;
        private bool resetSqlSelect;
        List<MyCatCommand> batch;
        private string batchableCommandText;
        CommandTimer commandTimer;
        private bool useDefaultTimeout;
        private bool shouldCache;
        private int cacheAge;
        private bool internallyCreated;

        /// <include file='docs/mysqlcommand.xml' path='docs/ctor1/*'/>
        public MyCatCommand()
        {
            cmdType = CommandType.Text;
            parameters = new MyCatParameterCollection(this);
            cmdText = String.Empty;
            useDefaultTimeout = true;
            Constructor();
        }

        partial void Constructor();

        /// <include file='docs/mysqlcommand.xml' path='docs/ctor2/*'/>
        public MyCatCommand(string cmdText)
          : this()
        {
            CommandText = cmdText;
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/ctor3/*'/>
        public MyCatCommand(string cmdText, MyCatConnection connection)
          : this(cmdText)
        {
            Connection = connection;
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/ctor4/*'/>
        public MyCatCommand(string cmdText, MyCatConnection connection,
                MyCatTransaction transaction)
          :
          this(cmdText, connection)
        {
            curTransaction = transaction;
        }

        #region Destructor
#if NETSTANDARD1_3
        ~MyCatCommand()
        {
            this.Dispose();
        }
#else
        ~MyCatCommand()
        {
            Dispose(false);
        }

#endif
        #endregion

        #region Properties


        /// <include file='docs/mysqlcommand.xml' path='docs/LastInseredId/*'/>
        [Browsable(false)]
        public Int64 LastInsertedId
        {
            get { return lastInsertedId; }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/CommandText/*'/>
        [Category("Data")]
        [Description("Command text to execute")]
        // [Editor("Pomelo.Data.Common.Design.SqlCommandTextEditor,MyCatClient.Design", typeof(System.Drawing.Design.UITypeEditor))]
        public override string CommandText
        {
            get { return cmdText; }
            set
            {
                cmdText = value ?? string.Empty;
                statement = null;
                batchableCommandText = null;
                if (cmdText != null && cmdText.EndsWith("DEFAULT VALUES", StringComparison.OrdinalIgnoreCase))
                {
                    cmdText = cmdText.Substring(0, cmdText.Length - 14);
                    cmdText = cmdText + "() VALUES ()";
                }
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/CommandTimeout/*'/>
        [Category("Misc")]
        [Description("Time to wait for command to execute")]
        [DefaultValue(30)]
        public override int CommandTimeout
        {
            get { return useDefaultTimeout ? 30 : commandTimeout; }
            set
            {
                if (commandTimeout < 0)
                    Throw(new ArgumentException("Command timeout must not be negative"));

                // Timeout in milliseconds should not exceed maximum for 32 bit
                // signed integer (~24 days), because underlying driver (and streams)
                // use milliseconds expressed ints for timeout values.
                // Hence, truncate the value.
                int timeout = Math.Min(value, Int32.MaxValue / 1000);
                if (timeout != value)
                {
                    MyCatTrace.LogWarning(connection.ServerThread,
                    "Command timeout value too large ("
                    + value + " seconds). Changed to max. possible value ("
                    + timeout + " seconds)");
                }
                commandTimeout = timeout;
                useDefaultTimeout = false;
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/CommandType/*'/>
        [Category("Data")]
        public override CommandType CommandType
        {
            get { return cmdType; }
            set { cmdType = value; }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/IsPrepared/*'/>
        [Browsable(false)]
        public bool IsPrepared
        {
            get { return statement != null && statement.IsPrepared; }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/Connection/*'/>
        [Category("Behavior")]
        [Description("Connection used by the command")]
        public new MyCatConnection Connection
        {
            get { return connection; }
            set
            {
                /*
                * The connection is associated with the transaction
                * so set the transaction object to return a null reference if the connection 
                * is reset.
                */
                if (connection != value)
                    Transaction = null;

                connection = value;

                // if the user has not already set the command timeout, then
                // take the default from the connection
                if (connection != null)
                {
                    if (useDefaultTimeout)
                    {
                        commandTimeout = (int)connection.Settings.DefaultCommandTimeout;
                        useDefaultTimeout = false;
                    }

                    EnableCaching = connection.Settings.TableCaching;
                    CacheAge = connection.Settings.DefaultTableCacheAge;
                }
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/Parameters/*'/>
        [Category("Data")]
        [Description("The parameters collection")]
#if !NETSTANDARD1_3
        [DesignerSerializationVisibility(DesignerSerializationVisibility.Content)]
#endif
        public new MyCatParameterCollection Parameters
        {
            get { return parameters; }
        }


        /// <include file='docs/mysqlcommand.xml' path='docs/Transaction/*'/>
        [Browsable(false)]
        public new MyCatTransaction Transaction
        {
            get { return curTransaction; }
            set { curTransaction = value; }
        }

        public bool EnableCaching
        {
            get { return shouldCache; }
            set { shouldCache = value; }
        }

        public int CacheAge
        {
            get { return cacheAge; }
            set { cacheAge = value; }
        }

        internal List<MyCatCommand> Batch
        {
            get { return batch; }
        }

        internal bool Canceled
        {
            get { return canceled; }
        }

        internal string BatchableCommandText
        {
            get { return batchableCommandText; }
        }

        internal bool InternallyCreated
        {
            get { return internallyCreated; }
            set { internallyCreated = value; }
        }

        #endregion

        #region Methods

        /// <summary>
        /// Attempts to cancel the execution of a currently active command
        /// </summary>
        /// <remarks>
        /// Cancelling a currently active query only works with MySQL versions 5.0.0 and higher.
        /// </remarks>
        public override void Cancel()
        {
            connection.CancelQuery(connection.ConnectionTimeout);
            canceled = true;
        }

        /// <summary>
        /// Creates a new instance of a <see cref="MyCatParameter"/> object.
        /// </summary>
        /// <remarks>
        /// This method is a strongly-typed version of <see cref="IDbCommand.CreateParameter"/>.
        /// </remarks>
        /// <returns>A <see cref="MyCatParameter"/> object.</returns>
        /// 
        public new MyCatParameter CreateParameter()
        {
            return (MyCatParameter)CreateDbParameter();
        }

        /// <summary>
        /// Check the connection to make sure
        ///		- it is open
        ///		- it is not currently being used by a reader
        ///		- and we have the right version of MySQL for the requested command type
        /// </summary>
        private void CheckState()
        {
            // There must be a valid and open connection.
            if (connection == null)
                Throw(new InvalidOperationException("Connection must be valid and open."));

            if (connection.State != ConnectionState.Open && !connection.SoftClosed)
                Throw(new InvalidOperationException("Connection must be valid and open."));

            // Data readers have to be closed first
            //if (connection.IsInUse && !this.internallyCreated)
            //    Throw(new MyCatException("There is already an open DataReader associated with this Connection which must be closed first."));
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/ExecuteNonQuery/*'/>
        public override int ExecuteNonQuery()
        {


#if !NETSTANDARD1_3
            int records = -1;
            // give our interceptors a shot at it first
            if (connection != null &&
                 connection.commandInterceptor != null &&
                 connection.commandInterceptor.ExecuteNonQuery(CommandText, ref records))
                return records;
#endif

            // ok, none of our interceptors handled this so we default
            using (MyCatDataReader reader = ExecuteReader())
            {
#if !NETSTANDARD1_3
                reader.Close();
#else
                reader.Dispose();
#endif
                return reader.RecordsAffected;
            }
        }

        internal void ClearCommandTimer()
        {
            if (commandTimer != null)
            {
                commandTimer.Dispose();
                commandTimer = null;
            }
        }

        internal void Close(MyCatDataReader reader)
        {
            if (statement != null)
                statement.Close(reader);
            ResetSqlSelectLimit();
            if (statement != null && connection != null && connection.driver != null)
                connection.driver.CloseQuery(connection, statement.StatementId);
            ClearCommandTimer();
        }

        /// <summary>
        /// Reset reader to null, to avoid "There is already an open data reader"
        /// on the next ExecuteReader(). Used in error handling scenarios.
        /// </summary>
        private void ResetReader()
        {
            if (connection != null && connection.Reader != null)
            {
#if !NETSTANDARD1_3
                foreach (var x in connection.Reader)
                    x.Close();
#else
                foreach (var x in connection.Reader)
                    x.Dispose();
#endif
                connection.Reader = null;
            }
        }

        /// <summary>
        /// Reset SQL_SELECT_LIMIT that could have been modified by CommandBehavior.
        /// </summary>
        internal void ResetSqlSelectLimit()
        {
            // if we are supposed to reset the sql select limit, do that here
            if (resetSqlSelect)
            {
                resetSqlSelect = false;
                MyCatCommand command = new MyCatCommand("SET SQL_SELECT_LIMIT=DEFAULT", connection);
                command.internallyCreated = true;
                command.ExecuteNonQuery();
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/ExecuteReader/*'/>
        public new MyCatDataReader ExecuteReader()
        {
            return ExecuteReader(CommandBehavior.Default);
        }

        private MyCatDataReader _ExecuteReader(CommandBehavior behavior, string sql)
        {
#if !NETSTANDARD1_3
            // give our interceptors a shot at it first
            MyCatDataReader interceptedReader = null;
            if (connection != null &&
                 connection.commandInterceptor != null &&
                 connection.commandInterceptor.ExecuteReader(CommandText, behavior, ref interceptedReader))
                return interceptedReader;
#endif

            // interceptors didn't handle this so we fall through
            bool success = false;
            CheckState();
            Driver driver = connection.driver;

            cmdText = cmdText.Trim();
            if (String.IsNullOrEmpty(cmdText))
                Throw(new InvalidOperationException(Resources.CommandTextNotInitialized));
            
            lock (driver)
            {
#if !NETSTANDARD1_3
                System.Transactions.Transaction curTrans = System.Transactions.Transaction.Current;

                if (curTrans != null)
                {
                    bool inRollback = false;
                    if (driver.CurrentTransaction != null)
                        inRollback = driver.CurrentTransaction.InRollback;
                    if (!inRollback)
                    {
                        System.Transactions.TransactionStatus status = System.Transactions.TransactionStatus.InDoubt;
                        try
                        {
                            // in some cases (during state transitions) this throws
                            // an exception. Ignore exceptions, we're only interested 
                            // whether transaction was aborted or not.
                            status = curTrans.TransactionInformation.Status;
                        }
                        catch (System.Transactions.TransactionException)
                        {
                        }
                        if (status == System.Transactions.TransactionStatus.Aborted)
                            Throw(new System.Transactions.TransactionAbortedException());
                    }
                }
#endif
                commandTimer = new CommandTimer(connection, CommandTimeout);

                lastInsertedId = -1;

                if (CommandType == CommandType.TableDirect)
                    sql = "SELECT * FROM " + sql;
                else if (CommandType == CommandType.Text)
                {
                    // validates single word statetment (maybe is a stored procedure call)
                    if (sql.IndexOf(" ") == -1)
                    {
                        if (AddCallStatement(sql))
                            sql = "call " + sql;
                    }
                }

                // if we are on a replicated connection, we are only allow readonly statements
                if (connection.Settings.Replication && !InternallyCreated)
                    EnsureCommandIsReadOnly(sql);

                if (statement == null || !statement.IsPrepared)
                {
                    if (CommandType == CommandType.StoredProcedure)
                        statement = new StoredProcedure(this, sql);
                    else
                        statement = new PreparableStatement(this, sql);
                }

                // stored procs are the only statement type that need do anything during resolve
                statement.Resolve(false);

                // Now that we have completed our resolve step, we can handle our
                // command behaviors
                HandleCommandBehaviors(behavior);


                try
                {
                    MyCatDataReader reader = new MyCatDataReader(this, statement, behavior);
                    connection.Reader.Add(reader);
                    canceled = false;
                    // execute the statement
                    statement.Execute();
                    // wait for data to return
                    reader.NextResult();
                    success = true;
                    return reader;
                }
                catch (TimeoutException tex)
                {
                    connection.HandleTimeoutOrThreadAbort(tex);
                    throw; //unreached
                }
                catch (ThreadAbortException taex)
                {
                    connection.HandleTimeoutOrThreadAbort(taex);
                    throw;
                }
                catch (IOException ioex)
                {
                    connection.Abort(); // Closes connection without returning it to the pool
                    throw new MyCatException(Resources.FatalErrorDuringExecute, ioex);
                }
                catch (MyCatException ex)
                {

                    if (ex.InnerException is TimeoutException)
                        throw; // already handled

                    try
                    {
                        ResetReader();
                        ResetSqlSelectLimit();
                    }
                    catch (Exception)
                    {
                        // Reset SqlLimit did not work, connection is hosed.
                        Connection.Abort();
                        throw new MyCatException(ex.Message, true, ex);
                    }

                    // if we caught an exception because of a cancel, then just return null
                    if (ex.IsQueryAborted)
                        return null;
                    if (ex.IsFatal)
                        Connection.Close();
                    if (ex.Number == 0)
                        throw new MyCatException(Resources.FatalErrorDuringExecute, ex);
                    throw;
                }
                finally
                {
                    if (connection != null)
                    {
                        if (connection.Reader == null)
                        {
                            // Something went seriously wrong,  and reader would not
                            // be able to clear timeout on closing.
                            // So we clear timeout here.
                            ClearCommandTimer();
                        }
                        if (!success)
                        {
                            // ExecuteReader failed.Close Reader and set to null to 
                            // prevent subsequent errors with DataReaderOpen
                            ResetReader();
                        }
                    }
                }
            }
        }

        public static int[] IndexOfMany(string self, string substr)
        {
            var ret = new List<int>();
            var ch1 = self.ToCharArray();
            var ch2 = substr.ToCharArray();
            for (var i = 0; i <= ch1.Length - ch2.Length; i++)
            {
                var flag = true;
                for (var j = 0; j < ch2.Length; i++)
                {
                    if (ch1[i + j] != ch2[j])
                    {
                        flag = false;
                        break;
                    }
                }
                if (flag)
                    ret.Add(i);
            }
            return ret.ToArray();
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/ExecuteReader1/*'/>
        public new MyCatDataReader ExecuteReader(CommandBehavior behavior)
        {
#if !NETSTANDARD1_3
            // give our interceptors a shot at it first
            MyCatDataReader interceptedReader = null;
            if (connection != null &&
                 connection.commandInterceptor != null &&
                 connection.commandInterceptor.ExecuteReader(CommandText, behavior, ref interceptedReader))
                return interceptedReader;
#endif

            // interceptors didn't handle this so we fall through
            bool success = false;
            CheckState();
            Driver driver = connection.driver;

            cmdText = cmdText.Trim();
            if (String.IsNullOrEmpty(cmdText))
                Throw(new InvalidOperationException(Resources.CommandTextNotInitialized));

            string sql = cmdText.Trim(';');
            var splited_sql = sql.Split(';');
            var not_second_query = splited_sql.First().IndexOf("SELECT") >= 0 || splited_sql.Count() == 1;
            var second_query = splited_sql.Where(x => x.IndexOf("SELECT") >= 0).ToList();
            if (!not_second_query)
                sql = string.Join(";", splited_sql.Where(x => x.IndexOf("SELECT") < 0));

            lock (driver)
            {
#if !NETSTANDARD1_3
                System.Transactions.Transaction curTrans = System.Transactions.Transaction.Current;

                if (curTrans != null)
                {
                    bool inRollback = false;
                    if (driver.CurrentTransaction != null)
                        inRollback = driver.CurrentTransaction.InRollback;
                    if (!inRollback)
                    {
                        System.Transactions.TransactionStatus status = System.Transactions.TransactionStatus.InDoubt;
                        try
                        {
                            // in some cases (during state transitions) this throws
                            // an exception. Ignore exceptions, we're only interested 
                            // whether transaction was aborted or not.
                            status = curTrans.TransactionInformation.Status;
                        }
                        catch (System.Transactions.TransactionException)
                        {
                        }
                        if (status == System.Transactions.TransactionStatus.Aborted)
                            Throw(new System.Transactions.TransactionAbortedException());
                    }
                }
#endif
                commandTimer = new CommandTimer(connection, CommandTimeout);

                lastInsertedId = -1;

                if (CommandType == CommandType.TableDirect)
                    sql = "SELECT * FROM " + sql;
                else if (CommandType == CommandType.Text)
                {
                    // validates single word statetment (maybe is a stored procedure call)
                    if (sql.IndexOf(" ") == -1)
                    {
                        if (AddCallStatement(sql))
                            sql = "call " + sql;
                    }
                }

                // if we are on a replicated connection, we are only allow readonly statements
                if (connection.Settings.Replication && !InternallyCreated)
                    EnsureCommandIsReadOnly(sql);

                if (statement == null || !statement.IsPrepared)
                {
                    if (CommandType == CommandType.StoredProcedure)
                        statement = new StoredProcedure(this, sql);
                    else
                        statement = new PreparableStatement(this, sql);
                }

                // stored procs are the only statement type that need do anything during resolve
                statement.Resolve(false);

                // Now that we have completed our resolve step, we can handle our
                // command behaviors
                HandleCommandBehaviors(behavior);


                try
                {
                    MyCatDataReader reader = new MyCatDataReader(this, statement, behavior);
                    connection.Reader.Add(reader);
                    canceled = false;
                    // execute the statement
                    statement.Execute();
                    // wait for data to return
                    try
                    {
                        reader.NextResult();
                    }
                    catch (MyCatException ex)
                    {
                        if (ex.Number != 1064)
                            throw;
                    }
                    success = true;
                    
                    if (!not_second_query)
                    {
                        reader.Dispose();
                        connection.Reader.Remove(reader);
                        if (second_query.Count > 0)
                            return _ExecuteReader(behavior, second_query.First());
                    }

                    return reader;
                }
                catch (TimeoutException tex)
                {
                    connection.HandleTimeoutOrThreadAbort(tex);
                    throw; //unreached
                }
                catch (ThreadAbortException taex)
                {
                    connection.HandleTimeoutOrThreadAbort(taex);
                    throw;
                }
                catch (IOException ioex)
                {
                    connection.Abort(); // Closes connection without returning it to the pool
                    throw new MyCatException(Resources.FatalErrorDuringExecute, ioex);
                }
                catch (MyCatException ex)
                {

                    if (ex.InnerException is TimeoutException)
                        throw; // already handled

                    try
                    {
                        ResetReader();
                        ResetSqlSelectLimit();
                    }
                    catch (Exception)
                    {
                        // Reset SqlLimit did not work, connection is hosed.
                        Connection.Abort();
                        throw new MyCatException(ex.Message, true, ex);
                    }

                    // if we caught an exception because of a cancel, then just return null
                    if (ex.IsQueryAborted)
                        return null;
                    if (ex.IsFatal)
                        Connection.Close();
                    if (ex.Number == 0)
                        throw new MyCatException(Resources.FatalErrorDuringExecute, ex);
                    throw;
                }
                finally
                {
                    if (connection != null)
                    {
                        if (connection.Reader == null)
                        {
                            // Something went seriously wrong,  and reader would not
                            // be able to clear timeout on closing.
                            // So we clear timeout here.
                            ClearCommandTimer();
                        }
                        if (!success)
                        {
                            // ExecuteReader failed.Close Reader and set to null to 
                            // prevent subsequent errors with DataReaderOpen
                            ResetReader();
                        }
                    }
                }
            }
        }

        private void EnsureCommandIsReadOnly(string sql)
        {
            sql = StringUtility.ToLowerInvariant(sql);
            if (!sql.StartsWith("select") && !sql.StartsWith("show"))
                Throw(new MyCatException(Resources.ReplicatedConnectionsAllowOnlyReadonlyStatements));
            if (sql.EndsWith("for update") || sql.EndsWith("lock in share mode"))
                Throw(new MyCatException(Resources.ReplicatedConnectionsAllowOnlyReadonlyStatements));
        }

        private bool IsReadOnlyCommand(string sql)
        {
            sql = sql.ToLower();
            return (sql.StartsWith("select") || sql.StartsWith("show"))
              && !(sql.EndsWith("for update") || sql.EndsWith("lock in share mode"));
        }


        /// <include file='docs/mysqlcommand.xml' path='docs/ExecuteScalar/*'/>
        public override object ExecuteScalar()
        {
            lastInsertedId = -1;
            object val = null;

#if !NETSTANDARD1_3
            // give our interceptors a shot at it first
            if (connection != null &&
                connection.commandInterceptor.ExecuteScalar(CommandText, ref val))
                return val;
#endif

            using (MyCatDataReader reader = ExecuteReader())
            {
                if (reader.Read())
                    val = reader.GetValue(0);
            }

            return val;
        }

        private void HandleCommandBehaviors(CommandBehavior behavior)
        {
            if ((behavior & CommandBehavior.SchemaOnly) != 0)
            {
                new MyCatCommand("SET SQL_SELECT_LIMIT=0", connection).ExecuteNonQuery();
                resetSqlSelect = true;
            }
            else if ((behavior & CommandBehavior.SingleRow) != 0)
            {
                new MyCatCommand("SET SQL_SELECT_LIMIT=1", connection).ExecuteNonQuery();
                resetSqlSelect = true;
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/Prepare2/*'/>
        private void Prepare(int cursorPageSize)
        {
            using (new CommandTimer(Connection, CommandTimeout))
            {
                // if the length of the command text is zero, then just return
                string psSQL = CommandText;
                if (psSQL == null ||
                     psSQL.Trim().Length == 0)
                    return;

                if (CommandType == CommandType.StoredProcedure)
                    statement = new StoredProcedure(this, CommandText);
                else
                    statement = new PreparableStatement(this, CommandText);

                statement.Resolve(true);
                statement.Prepare();
            }
        }

        /// <include file='docs/mysqlcommand.xml' path='docs/Prepare/*'/>
        public override void Prepare()
        {
            if (connection == null)
                Throw(new InvalidOperationException("The connection property has not been set."));
            if (connection.State != ConnectionState.Open)
                Throw(new InvalidOperationException("The connection is not open."));
            if (connection.Settings.IgnorePrepare)
                return;

            Prepare(0);
        }
        #endregion

        #region Async Methods

        internal delegate object AsyncDelegate(int type, CommandBehavior behavior);
        internal AsyncDelegate caller = null;
        internal Exception thrownException;

        internal object AsyncExecuteWrapper(int type, CommandBehavior behavior)
        {
            thrownException = null;
            try
            {
                if (type == 1)
                    return ExecuteReader(behavior);
                return ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                thrownException = ex;
            }
            return null;
        }

        /// <summary>
        /// Initiates the asynchronous execution of the SQL statement or stored procedure 
        /// that is described by this <see cref="MyCatCommand"/>, and retrieves one or more 
        /// result sets from the server. 
        /// </summary>
        /// <returns>An <see cref="IAsyncResult"/> that can be used to poll, wait for results, 
        /// or both; this value is also needed when invoking EndExecuteReader, 
        /// which returns a <see cref="MyCatDataReader"/> instance that can be used to retrieve 
        /// the returned rows. </returns>
        public IAsyncResult BeginExecuteReader()
        {
            return BeginExecuteReader(CommandBehavior.Default);
        }

        /// <summary>
        /// Initiates the asynchronous execution of the SQL statement or stored procedure 
        /// that is described by this <see cref="MyCatCommand"/> using one of the 
        /// <b>CommandBehavior</b> values. 
        /// </summary>
        /// <param name="behavior">One of the <see cref="CommandBehavior"/> values, indicating 
        /// options for statement execution and data retrieval.</param>
        /// <returns>An <see cref="IAsyncResult"/> that can be used to poll, wait for results, 
        /// or both; this value is also needed when invoking EndExecuteReader, 
        /// which returns a <see cref="MyCatDataReader"/> instance that can be used to retrieve 
        /// the returned rows. </returns>
        public IAsyncResult BeginExecuteReader(CommandBehavior behavior)
        {
            if (caller != null)
                Throw(new MyCatException(Resources.UnableToStartSecondAsyncOp));

            caller = new AsyncDelegate(AsyncExecuteWrapper);
            asyncResult = caller.BeginInvoke(1, behavior, null, null);
            return asyncResult;
        }

        /// <summary>
        /// Finishes asynchronous execution of a SQL statement, returning the requested 
        /// <see cref="MyCatDataReader"/>.
        /// </summary>
        /// <param name="result">The <see cref="IAsyncResult"/> returned by the call to 
        /// <see cref="BeginExecuteReader()"/>.</param>
        /// <returns>A <b>MyCatDataReader</b> object that can be used to retrieve the requested rows. </returns>
        public MyCatDataReader EndExecuteReader(IAsyncResult result)
        {
            result.AsyncWaitHandle.WaitOne();
            AsyncDelegate c = caller;
            caller = null;
            if (thrownException != null)
                throw thrownException;
            return (MyCatDataReader)c.EndInvoke(result);
        }

        /// <summary>
        /// Initiates the asynchronous execution of the SQL statement or stored procedure 
        /// that is described by this <see cref="MyCatCommand"/>. 
        /// </summary>
        /// <param name="callback">
        /// An <see cref="AsyncCallback"/> delegate that is invoked when the command's 
        /// execution has completed. Pass a null reference (<b>Nothing</b> in Visual Basic) 
        /// to indicate that no callback is required.</param>
        /// <param name="stateObject">A user-defined state object that is passed to the 
        /// callback procedure. Retrieve this object from within the callback procedure 
        /// using the <see cref="IAsyncResult.AsyncState"/> property.</param>
        /// <returns>An <see cref="IAsyncResult"/> that can be used to poll or wait for results, 
        /// or both; this value is also needed when invoking <see cref="EndExecuteNonQuery"/>, 
        /// which returns the number of affected rows. </returns>
        public IAsyncResult BeginExecuteNonQuery(AsyncCallback callback, object stateObject)
        {
            if (caller != null)
                Throw(new MyCatException(Resources.UnableToStartSecondAsyncOp));

            caller = new AsyncDelegate(AsyncExecuteWrapper);
            asyncResult = caller.BeginInvoke(2, CommandBehavior.Default,
                callback, stateObject);
            return asyncResult;
        }

        /// <summary>
        /// Initiates the asynchronous execution of the SQL statement or stored procedure 
        /// that is described by this <see cref="MyCatCommand"/>. 
        /// </summary>
        /// <returns>An <see cref="IAsyncResult"/> that can be used to poll or wait for results, 
        /// or both; this value is also needed when invoking <see cref="EndExecuteNonQuery"/>, 
        /// which returns the number of affected rows. </returns>
        public IAsyncResult BeginExecuteNonQuery()
        {
            if (caller != null)
                Throw(new MyCatException(Resources.UnableToStartSecondAsyncOp));

            caller = new AsyncDelegate(AsyncExecuteWrapper);
            asyncResult = caller.BeginInvoke(2, CommandBehavior.Default, null, null);
            return asyncResult;
        }

        /// <summary>
        /// Finishes asynchronous execution of a SQL statement. 
        /// </summary>
        /// <param name="asyncResult">The <see cref="IAsyncResult"/> returned by the call 
        /// to <see cref="BeginExecuteNonQuery()"/>.</param>
        /// <returns></returns>
        public int EndExecuteNonQuery(IAsyncResult asyncResult)
        {
            asyncResult.AsyncWaitHandle.WaitOne();
            AsyncDelegate c = caller;
            caller = null;
            if (thrownException != null)
                throw thrownException;
            return (int)c.EndInvoke(asyncResult);
        }

        #endregion

        #region Private Methods

        /*		private ArrayList PrepareSqlBuffers(string sql)
                    {
                        ArrayList buffers = new ArrayList();
                        MyCatStreamWriter writer = new MyCatStreamWriter(new MemoryStream(), connection.Encoding);
                        writer.Version = connection.driver.Version;

                        // if we are executing as a stored procedure, then we need to add the call
                        // keyword.
                        if (CommandType == CommandType.StoredProcedure)
                        {
                            if (storedProcedure == null)
                                storedProcedure = new StoredProcedure(this);
                            sql = storedProcedure.Prepare( CommandText );
                        }

                        // tokenize the SQL
                        sql = sql.TrimStart(';').TrimEnd(';');
                        ArrayList tokens = TokenizeSql( sql );

                        foreach (string token in tokens)
                        {
                            if (token.Trim().Length == 0) continue;
                            if (token == ";" && ! connection.driver.SupportsBatch)
                            {
                                MemoryStream ms = (MemoryStream)writer.Stream;
                                if (ms.Length > 0)
                                    buffers.Add( ms );

                                writer = new MyCatStreamWriter(new MemoryStream(), connection.Encoding);
                                writer.Version = connection.driver.Version;
                                continue;
                            }
                            else if (token[0] == parameters.ParameterMarker) 
                            {
                                if (SerializeParameter(writer, token)) continue;
                            }

                            // our fall through case is to write the token to the byte stream
                            writer.WriteStringNoNull(token);
                        }

                        // capture any buffer that is left over
                        MemoryStream mStream = (MemoryStream)writer.Stream;
                        if (mStream.Length > 0)
                            buffers.Add( mStream );

                        return buffers;
                    }*/

        internal long EstimatedSize()
        {
            long size = CommandText.Length;
            foreach (MyCatParameter parameter in Parameters)
                size += parameter.EstimatedSize();
            return size;
        }

        /// <summary>
        /// Verifies if a query is valid even if it has not spaces or is a stored procedure call
        /// </summary>
        /// <param name="query">Query to validate</param>
        /// <returns>If it is necessary to add call statement</returns>
        private bool AddCallStatement(string query)
        {
            /*PATTERN MATCHES
             * SELECT`user`FROM`mysql`.`user`;, select(left('test',1));, do(1);, commit, rollback, use, begin, end, use`sakila`;, select`test`;, select'1'=1;, SET@test='test';
             */
            string pattern = @"^|COMMIT|ROLLBACK|BEGIN|END|DO\S+|SELECT\S+[FROM|\S+]|USE?\S+|SET\S+";
            System.Text.RegularExpressions.Regex regex = new System.Text.RegularExpressions.Regex(pattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            return !(regex.Matches(query).Count > 0);
        }

        #endregion

        #region ICloneable

        /// <summary>
        /// Creates a clone of this MyCatCommand object.  CommandText, Connection, and Transaction properties
        /// are included as well as the entire parameter list.
        /// </summary>
        /// <returns>The cloned MyCatCommand object</returns>
        public MyCatCommand Clone()
        {
            MyCatCommand clone = new MyCatCommand(cmdText, connection, curTransaction);
            clone.CommandType = CommandType;
            clone.commandTimeout = commandTimeout;
            clone.useDefaultTimeout = useDefaultTimeout;
            clone.batchableCommandText = batchableCommandText;
            clone.EnableCaching = EnableCaching;
            clone.CacheAge = CacheAge;
            PartialClone(clone);

            foreach (MyCatParameter p in parameters)
            {
                clone.Parameters.Add(p.Clone());
            }
            return clone;
        }

        partial void PartialClone(MyCatCommand clone);
#if !(NETSTANDARD1_3)
        object ICloneable.Clone()
        {
            return this.Clone();
        }
#endif
        #endregion

        #region Batching support

        internal void AddToBatch(MyCatCommand command)
        {
            if (batch == null)
                batch = new List<MyCatCommand>();
            batch.Add(command);
        }

        internal string GetCommandTextForBatching()
        {
            if (batchableCommandText == null)
            {
                // if the command starts with insert and is "simple" enough, then
                // we can use the multi-value form of insert
                if (String.Compare(CommandText.Substring(0, 6), "INSERT", StringComparison.OrdinalIgnoreCase) == 0)
                {
                    MyCatCommand cmd = new MyCatCommand("SELECT @@sql_mode", Connection);
                    string sql_mode = StringUtility.ToUpperInvariant(cmd.ExecuteScalar().ToString());
                    MyCatTokenizer tokenizer = new MyCatTokenizer(CommandText);
                    tokenizer.AnsiQuotes = sql_mode.IndexOf("ANSI_QUOTES") != -1;
                    tokenizer.BackslashEscapes = sql_mode.IndexOf("NO_BACKSLASH_ESCAPES") == -1;
                    string token = StringUtility.ToLowerInvariant(tokenizer.NextToken());
                    while (token != null)
                    {
                        if (StringUtility.ToUpperInvariant(token) == "VALUES" &&
                            !tokenizer.Quoted)
                        {
                            token = tokenizer.NextToken();
                            if (token != "(")
                                throw new Exception();

                            // find matching right paren, and ensure that parens 
                            // are balanced.
                            int openParenCount = 1;
                            while (token != null)
                            {
                                batchableCommandText += token;
                                token = tokenizer.NextToken();

                                if (token == "(")
                                    openParenCount++;
                                else if (token == ")")
                                    openParenCount--;

                                if (openParenCount == 0)
                                    break;
                            }

                            if (token != null)
                                batchableCommandText += token;
                            token = tokenizer.NextToken();
                            if (token != null && (token == "," ||
                                StringUtility.ToUpperInvariant(token) == "ON"))
                            {
                                batchableCommandText = null;
                                break;
                            }
                        }
                        token = tokenizer.NextToken();
                    }
                }
                // Otherwise use the command verbatim
                else batchableCommandText = CommandText;
            }

            return batchableCommandText;
        }

        #endregion

        // This method is used to throw all exceptions from this class.  
        private void Throw(Exception ex)
        {
            if (connection != null)
                connection.Throw(ex);
            throw ex;
        }

#if NETSTANDARD1_3
        public new void Dispose()
        {
            GC.SuppressFinalize(this);
        }
#else
        public new void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected override void Dispose(bool disposing)
        {
            if (statement != null && statement.IsPrepared)
                statement.CloseStatement();

            base.Dispose(disposing);
        }

#endif
    }
}

