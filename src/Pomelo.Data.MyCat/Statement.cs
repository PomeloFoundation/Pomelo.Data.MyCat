// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.IO;
using System.Text;
using Pomelo.Data.Common;
using System.Data;

using System.Collections.Generic;

namespace Pomelo.Data.MyCat
{
  internal abstract class Statement
  {
    protected MyCatCommand command;
    protected string commandText;
    private List<MyCatPacket> buffers;

    private Statement(MyCatCommand cmd)
    {
      command = cmd;
      buffers = new List<MyCatPacket>();
    }

    public Statement(MyCatCommand cmd, string text)
      : this(cmd)
    {
      commandText = text;
    }

    #region Properties

    public virtual string ResolvedCommandText
    {
      get { return commandText; }
    }

    protected Driver Driver
    {
      get { return command.Connection.driver; }
    }

    protected MyCatConnection Connection
    {
      get { return command.Connection; }
    }

    protected MyCatParameterCollection Parameters
    {
      get { return command.Parameters; }
    }

    #endregion

    public virtual void Close(MyCatDataReader reader)
    {
    }

    public virtual void Resolve(bool preparing)
    {
    }

    public virtual void Execute()
    {
      // we keep a reference to this until we are done
      BindParameters();
      ExecuteNext();
    }

    public virtual bool ExecuteNext()
    {
      if (buffers.Count == 0)
        return false;

      MyCatPacket packet = (MyCatPacket)buffers[0];
      //MemoryStream ms = stream.InternalBuffer;
      Driver.SendQuery(packet);
      buffers.RemoveAt(0);
      return true;
    }

    protected virtual void BindParameters()
    {
      MyCatParameterCollection parameters = command.Parameters;
      int index = 0;

      while (true)
      {
        InternalBindParameters(ResolvedCommandText, parameters, null);

        // if we are not batching, then we are done.  This is only really relevant the
        // first time through
        if (command.Batch == null) return;
        while (index < command.Batch.Count)
        {
          MyCatCommand batchedCmd = command.Batch[index++];
          MyCatPacket packet = (MyCatPacket)buffers[buffers.Count - 1];

          // now we make a guess if this statement will fit in our current stream
          long estimatedCmdSize = batchedCmd.EstimatedSize();
          if (((packet.Length - 4) + estimatedCmdSize) > Connection.driver.MaxPacketSize)
          {
            // it won't, so we setup to start a new run from here
            parameters = batchedCmd.Parameters;
            break;
          }

          // looks like we might have room for it so we remember the current end of the stream
          buffers.RemoveAt(buffers.Count - 1);
          //long originalLength = packet.Length - 4;

          // and attempt to stream the next command
          string text = ResolvedCommandText;
          if (text.StartsWith("(", StringComparison.Ordinal))
            packet.WriteStringNoNull(", ");
          else
            packet.WriteStringNoNull("; ");
          InternalBindParameters(text, batchedCmd.Parameters, packet);
          if ((packet.Length - 4) > Connection.driver.MaxPacketSize)
          {
            //TODO
            //stream.InternalBuffer.SetLength(originalLength);
            parameters = batchedCmd.Parameters;
            break;
          }
        }
        if (index == command.Batch.Count)
          return;
      }
    }

    private void InternalBindParameters(string sql, MyCatParameterCollection parameters,
        MyCatPacket packet)
    {
      bool sqlServerMode = command.Connection.Settings.SqlServerMode;

      if (packet == null)
      {
        packet = new MyCatPacket(Driver.Encoding);
        packet.Version = Driver.Version;
        packet.WriteByte(0);
      }

      MyCatTokenizer tokenizer = new MyCatTokenizer(sql);
      tokenizer.ReturnComments = true;
      tokenizer.SqlServerMode = sqlServerMode;

      int pos = 0;
      string token = tokenizer.NextToken();
      int parameterCount = 0;
      while (token != null)
      {
        // serialize everything that came before the token (i.e. whitespace)
        packet.WriteStringNoNull(sql.Substring(pos, tokenizer.StartIndex - pos));
        pos = tokenizer.StopIndex;
        if (MyCatTokenizer.IsParameter(token))
        {
          if ((!parameters.containsUnnamedParameters && token.Length == 1 && parameterCount > 0) || parameters.containsUnnamedParameters && token.Length > 1)
            throw new MyCatException(Resources.MixedParameterNamingNotAllowed);

          parameters.containsUnnamedParameters = token.Length == 1;
          if (SerializeParameter(parameters, packet, token, parameterCount))
            token = null;
          parameterCount++;
        }
        if (token != null)
        {
          if (sqlServerMode && tokenizer.Quoted && token.StartsWith("[", StringComparison.Ordinal))
            token = String.Format("`{0}`", token.Substring(1, token.Length - 2));
          packet.WriteStringNoNull(token);
        }
        token = tokenizer.NextToken();
      }
      buffers.Add(packet);
    }

    protected virtual bool ShouldIgnoreMissingParameter(string parameterName)
    {
      if (Connection.Settings.AllowUserVariables)
        return true;
      if (parameterName.StartsWith("@" + StoredProcedure.ParameterPrefix, StringComparison.OrdinalIgnoreCase))
        return true;
      if (parameterName.Length > 1 &&
          (parameterName[1] == '`' || parameterName[1] == '\''))
        return true;
      return false;
    }

    /// <summary>
    /// Serializes the given parameter to the given memory stream
    /// </summary>
    /// <remarks>
    /// <para>This method is called by PrepareSqlBuffers to convert the given
    /// parameter to bytes and write those bytes to the given memory stream.
    /// </para>
    /// </remarks>
    /// <returns>True if the parameter was successfully serialized, false otherwise.</returns>
    private bool SerializeParameter(MyCatParameterCollection parameters,
                                    MyCatPacket packet, string parmName, int parameterIndex)
    {
      MyCatParameter parameter = null;

      if (!parameters.containsUnnamedParameters)
        parameter = parameters.GetParameterFlexible(parmName, false);
      else
      {
        if (parameterIndex <= parameters.Count)
          parameter = parameters[parameterIndex];
        else
          throw new MyCatException(Resources.ParameterIndexNotFound);
      }

      if (parameter == null)
      {
        // if we are allowing user variables and the parameter name starts with @
        // then we can't throw an exception
        if (parmName.StartsWith("@", StringComparison.Ordinal) && ShouldIgnoreMissingParameter(parmName))
          return false;
        throw new MyCatException(
            String.Format(Resources.ParameterMustBeDefined, parmName));
      }
      parameter.Serialize(packet, false, Connection.Settings);
      return true;
    }
  }
}
