// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.Text;
using System.Collections.Generic;

using System.Data;

namespace Pomelo.Data.MyCat
{
    /// <summary>
    /// Summary description for PreparedStatement.
    /// </summary>
    internal class PreparableStatement : Statement
    {
        private int executionCount;
        private int statementId;
#if NET451
        BitArray nullMap;
#else
        RtBitArray nullMap;
#endif
        List<MyCatParameter> parametersToSend = new List<MyCatParameter>();
        MyCatPacket packet;
        int dataPosition;
        int nullMapPosition;

        public PreparableStatement(MyCatCommand command, string text)
          : base(command, text)
        {
        }

#region Properties

        public int ExecutionCount
        {
            get { return executionCount; }
            set { executionCount = value; }
        }

        public bool IsPrepared
        {
            get { return statementId > 0; }
        }

        public int StatementId
        {
            get { return statementId; }
        }

#endregion

        public virtual void Prepare()
        {
            // strip out names from parameter markers
            string text;
            List<string> parameter_names = PrepareCommandText(out text);

            // ask our connection to send the prepare command
            MyCatField[] paramList = null;
            statementId = Driver.PrepareStatement(text, ref paramList);

            // now we need to assign our field names since we stripped them out
            // for the prepare
            for (int i = 0; i < parameter_names.Count; i++)
            {
                //paramList[i].ColumnName = (string) parameter_names[i];
                string parameterName = (string)parameter_names[i];
                MyCatParameter p = Parameters.GetParameterFlexible(parameterName, false);
                if (p == null)
                    throw new InvalidOperationException(
                        String.Format(Resources.ParameterNotFoundDuringPrepare, parameterName));
                p.Encoding = paramList[i].Encoding;
                parametersToSend.Add(p);
            }

            // now prepare our null map
            int numNullBytes = 0;
            if (paramList != null && paramList.Length > 0)
            {
#if NET451
                nullMap = new BitArray(paramList.Length);
#else
                nullMap= new RtBitArray(paramList.Length);
#endif
                numNullBytes = (nullMap.Count + 7) / 8;
            }

            packet = new MyCatPacket(Driver.Encoding);

            // write out some values that do not change run to run
            packet.WriteByte(0);
            packet.WriteInteger(statementId, 4);
            packet.WriteByte((byte)0); // flags; always 0 for 4.1
            packet.WriteInteger(1, 4); // interation count; 1 for 4.1
            nullMapPosition = packet.Position;
            packet.Position += numNullBytes;  // leave room for our null map
            packet.WriteByte(1); // rebound flag
                                 // write out the parameter types
            foreach (MyCatParameter p in parametersToSend)
                packet.WriteInteger(p.GetPSType(), 2);
            dataPosition = packet.Position;
        }

        public override void Execute()
        {
            // if we are not prepared, then call down to our base
            if (!IsPrepared)
            {
                base.Execute();
                return;
            }

            //TODO: support long data here
            // create our null bitmap

            // we check this because Mono doesn't ignore the case where nullMapBytes
            // is zero length.
            //            if (nullMapBytes.Length > 0)
            //          {
            //            byte[] bits = packet.Buffer;
            //          nullMap.CopyTo(bits, 
            //        nullMap.CopyTo(nullMapBytes, 0);

            // start constructing our packet
            //            if (Parameters.Count > 0)
            //              nullMap.CopyTo(packet.Buffer, nullMapPosition);
            //if (parameters != null && parameters.Count > 0)
            //else
            //	packet.WriteByte( 0 );
            //TODO:  only send rebound if parms change

            // now write out all non-null values
            packet.Position = dataPosition;
            for (int i = 0; i < parametersToSend.Count; i++)
            {
                MyCatParameter p = parametersToSend[i];
                nullMap[i] = (p.Value == DBNull.Value || p.Value == null) ||
                    p.Direction == ParameterDirection.Output;
                if (nullMap[i]) continue;
                packet.Encoding = p.Encoding;
                p.Serialize(packet, true, Connection.Settings);
            }
            if (nullMap != null)
            {
               nullMap.CopyTo(packet.Buffer, nullMapPosition);
            }

            executionCount++;

            Driver.ExecuteStatement(packet);
        }

        

        public override bool ExecuteNext()
        {
            if (!IsPrepared)
                return base.ExecuteNext();
            return false;
        }

        /// <summary>
        /// Prepares CommandText for use with the Prepare method
        /// </summary>
        /// <returns>Command text stripped of all paramter names</returns>
        /// <remarks>
        /// Takes the output of TokenizeSql and creates a single string of SQL
        /// that only contains '?' markers for each parameter.  It also creates
        /// the parameterMap array list that includes all the paramter names in the
        /// order they appeared in the SQL
        /// </remarks>
        private List<string> PrepareCommandText(out string stripped_sql)
        {
            StringBuilder newSQL = new StringBuilder();
            List<string> parameterMap = new List<string>();

            int startPos = 0;
            string sql = ResolvedCommandText;
            MyCatTokenizer tokenizer = new MyCatTokenizer(sql);
            string parameter = tokenizer.NextParameter();
            while (parameter != null)
            {
                if (parameter.IndexOf(StoredProcedure.ParameterPrefix) == -1)
                {
                    newSQL.Append(sql.Substring(startPos, tokenizer.StartIndex - startPos));
                    newSQL.Append("?");
                    parameterMap.Add(parameter);
                    startPos = tokenizer.StopIndex;
                }
                parameter = tokenizer.NextParameter();
            }
            newSQL.Append(sql.Substring(startPos));
            stripped_sql = newSQL.ToString();
            return parameterMap;
        }

        public virtual void CloseStatement()
        {
            if (!IsPrepared) return;

            Driver.CloseStatement(statementId);
            statementId = 0;
        }
    }
}
