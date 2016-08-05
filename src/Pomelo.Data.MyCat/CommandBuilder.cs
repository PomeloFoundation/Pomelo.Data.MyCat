// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if NET451
using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;

using System.Text;
using Pomelo.Data.Common;
using System.Collections;
using Pomelo.Data.Types;
using System.Globalization;

using System.Collections.Generic;

namespace Pomelo.Data.MyCat
{
    /// <include file='docs/MyCatCommandBuilder.xml' path='docs/class/*'/>
    [ToolboxItem(false)]
    [System.ComponentModel.DesignerCategory("Code")]
    public sealed class MyCatCommandBuilder : DbCommandBuilder
    {
        /// <include file='docs/MyCatCommandBuilder.xml' path='docs/Ctor/*'/>
        public MyCatCommandBuilder()
        {
            QuotePrefix = QuoteSuffix = "`";
        }

        /// <include file='docs/MyCatCommandBuilder.xml' path='docs/Ctor2/*'/>
        public MyCatCommandBuilder(MyCatDataAdapter adapter)
          : this()
        {
            DataAdapter = adapter;
        }

        /// <include file='docs/mysqlcommandBuilder.xml' path='docs/DataAdapter/*'/>
        public new MyCatDataAdapter DataAdapter
        {
            get { return (MyCatDataAdapter)base.DataAdapter; }
            set { base.DataAdapter = value; }
        }

        #region Public Methods

        /// <summary>
        /// Retrieves parameter information from the stored procedure specified 
        /// in the MyCatCommand and populates the Parameters collection of the 
        /// specified MyCatCommand object.
        /// This method is not currently supported since stored procedures are 
        /// not available in MyCat.
        /// </summary>
        /// <param name="command">The MyCatCommand referencing the stored 
        /// procedure from which the parameter information is to be derived. 
        /// The derived parameters are added to the Parameters collection of the 
        /// MyCatCommand.</param>
        /// <exception cref="InvalidOperationException">The command text is not 
        /// a valid stored procedure name.</exception>
        public static void DeriveParameters(MyCatCommand command)
        {
            if (command.CommandType != CommandType.StoredProcedure)
                throw new InvalidOperationException(Resources.CanNotDeriveParametersForTextCommands);

            // retrieve the proc definition from the cache.
            string spName = command.CommandText;
            if (spName.IndexOf(".") == -1)
                spName = command.Connection.Database + "." + spName;

            try
            {
                ProcedureCacheEntry entry = command.Connection.ProcedureCache.GetProcedure(command.Connection, spName, null);
                command.Parameters.Clear();
                foreach (MyCatSchemaRow row in entry.parameters.Rows)
                {
                    MyCatParameter p = new MyCatParameter();
                    p.ParameterName = String.Format("@{0}", row["PARAMETER_NAME"]);
                    if (row["ORDINAL_POSITION"].Equals(0) && p.ParameterName == "@")
                        p.ParameterName = "@RETURN_VALUE";
                    p.Direction = GetDirection(row);
                    bool unsigned = StoredProcedure.GetFlags(row["DTD_IDENTIFIER"].ToString()).IndexOf("UNSIGNED") != -1;
                    bool real_as_float = entry.procedure.Rows[0]["SQL_MODE"].ToString().IndexOf("REAL_AS_FLOAT") != -1;
                    p.MyCatDbType = MetaData.NameToType(row["DATA_TYPE"].ToString(),
                      unsigned, real_as_float, command.Connection);
                    if (row["CHARACTER_MAXIMUM_LENGTH"] != null)
                        p.Size = (int)row["CHARACTER_MAXIMUM_LENGTH"];
#if NET452 || DNX452 || NETSTANDARD1_3
          if (row["NUMERIC_PRECISION"] != null)
            p.Precision = Convert.ToByte(row["NUMERIC_PRECISION"]);
          if (row["NUMERIC_SCALE"] != null )
            p.Scale = Convert.ToByte(row["NUMERIC_SCALE"]);
#endif
                    if (p.MyCatDbType == MyCatDbType.Set || p.MyCatDbType == MyCatDbType.Enum)
                        p.PossibleValues = GetPossibleValues(row);
                    command.Parameters.Add(p);
                }
            }
            catch (InvalidOperationException ioe)
            {
                throw new MyCatException(Resources.UnableToDeriveParameters, ioe);
            }
        }

        private static List<string> GetPossibleValues(MyCatSchemaRow row)
        {
            string[] types = new string[] { "ENUM", "SET" };
            string dtdIdentifier = row["DTD_IDENTIFIER"].ToString().Trim();

            int index = 0;
            for (; index < 2; index++)
                if (dtdIdentifier.StartsWith(types[index], StringComparison.OrdinalIgnoreCase))
                    break;
            if (index == 2) return null;
            dtdIdentifier = dtdIdentifier.Substring(types[index].Length).Trim();
            dtdIdentifier = dtdIdentifier.Trim('(', ')').Trim();

            List<string> values = new List<string>();
            MyCatTokenizer tokenzier = new MyCatTokenizer(dtdIdentifier);
            string token = tokenzier.NextToken();
            int start = tokenzier.StartIndex;
            while (true)
            {
                if (token == null || token == ",")
                {
                    int end = dtdIdentifier.Length - 1;
                    if (token == ",")
                        end = tokenzier.StartIndex;

                    string value = dtdIdentifier.Substring(start, end - start).Trim('\'', '\"').Trim();
                    values.Add(value);
                    start = tokenzier.StopIndex;
                }
                if (token == null) break;
                token = tokenzier.NextToken();
            }
            return values;
        }

        private static ParameterDirection GetDirection(MyCatSchemaRow row)
        {
            string mode = row["PARAMETER_MODE"].ToString();
            int ordinal = Convert.ToInt32(row["ORDINAL_POSITION"]);

            if (0 == ordinal)
                return ParameterDirection.ReturnValue;
            else if (mode == "IN")
                return ParameterDirection.Input;
            else if (mode == "OUT")
                return ParameterDirection.Output;
            return ParameterDirection.InputOutput;
        }

        /// <summary>
        /// Gets the delete command.
        /// </summary>
        /// <returns></returns>
        public new MyCatCommand GetDeleteCommand()
        {
            return (MyCatCommand)base.GetDeleteCommand();
        }

        /// <summary>
        /// Gets the update command.
        /// </summary>
        /// <returns></returns>
        public new MyCatCommand GetUpdateCommand()
        {
            return (MyCatCommand)base.GetUpdateCommand();
        }

        /// <summary>
        /// Gets the insert command.
        /// </summary>
        /// <returns></returns>
        public new MyCatCommand GetInsertCommand()
        {
            return (MyCatCommand)GetInsertCommand(false);
        }

        public override string QuoteIdentifier(string unquotedIdentifier)
        {
            if (unquotedIdentifier == null)
                throw new
ArgumentNullException("unquotedIdentifier");

            // don't quote again if it is already quoted
            if (unquotedIdentifier.StartsWith(QuotePrefix) &&
              unquotedIdentifier.EndsWith(QuoteSuffix))
                return unquotedIdentifier;

            unquotedIdentifier = unquotedIdentifier.Replace(QuotePrefix, QuotePrefix + QuotePrefix);

            return String.Format("{0}{1}{2}", QuotePrefix, unquotedIdentifier, QuoteSuffix);
        }

        public override string UnquoteIdentifier(string quotedIdentifier)
        {
            if (quotedIdentifier == null)
                throw new
ArgumentNullException("quotedIdentifier");

            // don't unquote again if it is already unquoted
            if (!quotedIdentifier.StartsWith(QuotePrefix) ||
              !quotedIdentifier.EndsWith(QuoteSuffix))
                return quotedIdentifier;

            if (quotedIdentifier.StartsWith(QuotePrefix))
                quotedIdentifier = quotedIdentifier.Substring(1);
            if (quotedIdentifier.EndsWith(QuoteSuffix))
                quotedIdentifier = quotedIdentifier.Substring(0, quotedIdentifier.Length - 1);

            quotedIdentifier = quotedIdentifier.Replace(QuotePrefix + QuotePrefix, QuotePrefix);

            return quotedIdentifier;
        }

        #endregion

        protected override DataTable GetSchemaTable(DbCommand sourceCommand)
        {
            DataTable schemaTable = base.GetSchemaTable(sourceCommand);

            foreach (DataRow row in schemaTable.Rows)
                if (row["BaseSchemaName"].Equals(sourceCommand.Connection.Database))
                    row["BaseSchemaName"] = null;

            return schemaTable;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        protected override string GetParameterName(string parameterName)
        {
            StringBuilder sb = new StringBuilder(parameterName);
            sb.Replace(" ", "");
            sb.Replace("/", "_per_");
            sb.Replace("-", "_");
            sb.Replace(")", "_cb_");
            sb.Replace("(", "_ob_");
            sb.Replace("%", "_pct_");
            sb.Replace("<", "_lt_");
            sb.Replace(">", "_gt_");
            sb.Replace(".", "_pt_");
            return String.Format("@{0}", sb.ToString());
        }

        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row,
          StatementType statementType, bool whereClause)
        {
            ((MyCatParameter)parameter).MyCatDbType = (MyCatDbType)row["ProviderType"];
        }

        protected override string GetParameterName(int parameterOrdinal)
        {
            return String.Format("@p{0}", parameterOrdinal.ToString(CultureInfo.InvariantCulture));
        }

        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            return String.Format("@p{0}", parameterOrdinal.ToString(CultureInfo.InvariantCulture));
        }

        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            MyCatDataAdapter myAdapter = (adapter as MyCatDataAdapter);
            if (adapter != base.DataAdapter)
                myAdapter.RowUpdating += new MyCatRowUpdatingEventHandler(RowUpdating);
            else
                myAdapter.RowUpdating -= new MyCatRowUpdatingEventHandler(RowUpdating);
        }

        private void RowUpdating(object sender, MyCatRowUpdatingEventArgs args)
        {
            base.RowUpdatingHandler(args);
        }

    }
}
#endif
