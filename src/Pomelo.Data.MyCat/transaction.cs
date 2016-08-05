// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
#if !RT
using System.Data;
#endif

namespace Pomelo.Data.MyCat
{
    /// <include file='docs/MyCatTransaction.xml' path='docs/Class/*'/>
    public sealed partial class MyCatTransaction : IDisposable
    {
        private IsolationLevel level;
        private MyCatConnection conn;
        private bool open;

        internal MyCatTransaction(MyCatConnection c, IsolationLevel il)
        {
            conn = c;
            level = il;
            open = true;
        }

        #region Destructor
        ~MyCatTransaction()
        {
            Dispose(false);
        }
        #endregion

        #region Properties

        /// <summary>
        /// Gets the <see cref="MyCatConnection"/> object associated with the transaction, or a null reference (Nothing in Visual Basic) if the transaction is no longer valid.
        /// </summary>
        /// <value>The <see cref="MyCatConnection"/> object associated with this transaction.</value>
        /// <remarks>
        /// A single application may have multiple database connections, each 
        /// with zero or more transactions. This property enables you to 
        /// determine the connection object associated with a particular 
        /// transaction created by <see cref="MyCatConnection.BeginTransaction()"/>.
        /// </remarks>
        public new MyCatConnection Connection
        {
            get { return conn; }
        }

        /// <summary>
        /// Specifies the <see cref="IsolationLevel"/> for this transaction.
        /// </summary>
        /// <value>
        /// The <see cref="IsolationLevel"/> for this transaction. The default is <b>ReadCommitted</b>.
        /// </value>
        /// <remarks>
        /// Parallel transactions are not supported. Therefore, the IsolationLevel 
        /// applies to the entire transaction.
        /// </remarks>
        public override IsolationLevel IsolationLevel
        {
            get { return level; }
        }

        #endregion

        public new void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if ((conn != null && conn.State == ConnectionState.Open || conn.SoftClosed) && open)
                    Rollback();
            }
        }

        /// <include file='docs/MyCatTransaction.xml' path='docs/Commit/*'/>
        public override void Commit()
        {
            if (conn == null || (conn.State != ConnectionState.Open && !conn.SoftClosed))
                throw new InvalidOperationException("Connection must be valid and open to commit transaction");
            if (!open)
                throw new InvalidOperationException("Transaction has already been committed or is not pending");
            MyCatCommand cmd = new MyCatCommand("COMMIT", conn);
            cmd.ExecuteNonQuery();
            open = false;
        }

        /// <include file='docs/MyCatTransaction.xml' path='docs/Rollback/*'/>
        public override void Rollback()
        {
            if (conn == null || (conn.State != ConnectionState.Open && !conn.SoftClosed))
                throw new InvalidOperationException("Connection must be valid and open to rollback transaction");
            if (!open)
                throw new InvalidOperationException("Transaction has already been rolled back or is not pending");
            MyCatCommand cmd = new MyCatCommand("ROLLBACK", conn);
            cmd.ExecuteNonQuery();
            open = false;
        }

    }
}
