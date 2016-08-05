// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.Data;
using System.Collections.Generic;

using System.Diagnostics;
using System.Text;
using System.Globalization;

namespace Pomelo.Data.MyCat
{
  internal class ProcedureCacheEntry
  {
    public MyCatSchemaCollection procedure;
    public MyCatSchemaCollection parameters;
  }

  internal class ProcedureCache
  {
    private Dictionary<int, ProcedureCacheEntry> procHash;
    private Queue<int> hashQueue;
    private int maxSize;

    public ProcedureCache(int size)
    {
      maxSize = size;
      hashQueue = new Queue<int>(maxSize);
      procHash = new Dictionary<int, ProcedureCacheEntry>(maxSize);
    }

    public ProcedureCacheEntry GetProcedure(MyCatConnection conn, string spName, string cacheKey)
    {
      ProcedureCacheEntry proc = null;

      if (cacheKey != null)
      {
        int hash = cacheKey.GetHashCode();

        lock (procHash)
        {
          procHash.TryGetValue(hash, out proc);
        }
      }
      if (proc == null)
      {
        proc = AddNew(conn, spName);
        conn.PerfMonitor.AddHardProcedureQuery();
        if (conn.Settings.Logging)
          MyCatTrace.LogInformation(conn.ServerThread,
            String.Format(Resources.HardProcQuery, spName));
      }
      else
      {
        conn.PerfMonitor.AddSoftProcedureQuery();
        if (conn.Settings.Logging)
          MyCatTrace.LogInformation(conn.ServerThread,
            String.Format(Resources.SoftProcQuery, spName));
      }
      return proc;
    }

    internal string GetCacheKey(string spName, ProcedureCacheEntry proc)
    {
      string retValue = String.Empty;
      StringBuilder key = new StringBuilder(spName);
      key.Append("(");
      string delimiter = "";
      if (proc.parameters != null)
      {
        foreach (MyCatSchemaRow row in proc.parameters.Rows)
        {
          if (row["ORDINAL_POSITION"].Equals(0))
            retValue = "?=";
          else
          {
            key.AppendFormat(CultureInfo.InvariantCulture, "{0}?", delimiter);
            delimiter = ",";
          }
        }
      }
      key.Append(")");
      return retValue + key.ToString();
    }

    private ProcedureCacheEntry AddNew(MyCatConnection connection, string spName)
    {
      ProcedureCacheEntry procData = GetProcData(connection, spName);
      if (maxSize > 0)
      {
        string cacheKey = GetCacheKey(spName, procData);
        int hash = cacheKey.GetHashCode();
        lock (procHash)
        {
          if (procHash.Keys.Count >= maxSize)
            TrimHash();
          if (!procHash.ContainsKey(hash))
          {
            procHash[hash] = procData;
            hashQueue.Enqueue(hash);
          }
        }
      }
      return procData;
    }

    private void TrimHash()
    {
      int oldestHash = hashQueue.Dequeue();
      procHash.Remove(oldestHash);
    }

    private static ProcedureCacheEntry GetProcData(MyCatConnection connection, string spName)
    {
      string schema = String.Empty;
      string name = spName;

      int dotIndex = spName.IndexOf(".");
      if (dotIndex != -1)
      {
        schema = spName.Substring(0, dotIndex);
        name = spName.Substring(dotIndex + 1, spName.Length - dotIndex - 1);
      }

      string[] restrictions = new string[4];
      restrictions[1] = schema.Length > 0 ? schema : connection.CurrentDatabase();
      restrictions[2] = name;
      MyCatSchemaCollection proc = connection.GetSchemaCollection("procedures", restrictions);
      if (proc.Rows.Count > 1)
        throw new MyCatException(Resources.ProcAndFuncSameName);
      if (proc.Rows.Count == 0)
        throw new MyCatException(String.Format(Resources.InvalidProcName, name, schema));

      ProcedureCacheEntry entry = new ProcedureCacheEntry();
      entry.procedure = proc;

      // we don't use GetSchema here because that would cause another
      // query of procedures and we don't need that since we already
      // know the procedure we care about.
      ISSchemaProvider isp = new ISSchemaProvider(connection);
      string[] rest = isp.CleanRestrictions(restrictions);
      MyCatSchemaCollection parameters = isp.GetProcedureParameters(rest, proc);
      entry.parameters = parameters;

      return entry;
    }
  }
}