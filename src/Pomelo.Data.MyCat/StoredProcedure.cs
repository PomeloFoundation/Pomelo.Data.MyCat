// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.Globalization;
using System.Text;
using Pomelo.Data.Types;


namespace Pomelo.Data.MyCat
{
  /// <summary>
  /// Summary description for StoredProcedure.
  /// </summary>
  internal class StoredProcedure : PreparableStatement
  {
    private string outSelect;
    private string resolvedCommandText;
    private bool serverProvidingOutputParameters;

    // Prefix used for to generate inout or output parameters names
    internal const string ParameterPrefix = "_cnet_param_";

    public StoredProcedure(MyCatCommand cmd, string text)
      : base(cmd, text)
    {
    }

    private MyCatParameter GetReturnParameter()
    {
      if (Parameters != null)
        foreach (MyCatParameter p in Parameters)
          if (p.Direction == ParameterDirection.ReturnValue)
            return p;
      return null;
    }

    public bool ServerProvidingOutputParameters
    {
      get { return serverProvidingOutputParameters; }
    }

    public override string ResolvedCommandText
    {
      get { return resolvedCommandText; }
    }

    internal string GetCacheKey(string spName)
    {
      string retValue = String.Empty;
      StringBuilder key = new StringBuilder(spName);
      key.Append("(");
      string delimiter = "";
      foreach (MyCatParameter p in command.Parameters)
      {
        if (p.Direction == ParameterDirection.ReturnValue)
          retValue = "?=";
        else
        {
          key.AppendFormat(CultureInfo.InvariantCulture, "{0}?", delimiter);
          delimiter = ",";
        }
      }
      key.Append(")");
      return retValue + key.ToString();
    }

    private ProcedureCacheEntry GetParameters(string procName)
    {
      string procCacheKey = GetCacheKey(procName);
      ProcedureCacheEntry entry = Connection.ProcedureCache.GetProcedure(Connection, procName, procCacheKey);
      return entry;
    }

    public static string GetFlags(string dtd)
    {
      int x = dtd.Length - 1;
      while (x > 0 && (Char.IsLetterOrDigit(dtd[x]) || dtd[x] == ' '))
        x--;
      string dtdSubstring = dtd.Substring(x);
      return StringUtility.ToUpperInvariant(dtdSubstring);
    }

    private string FixProcedureName(string name)
    {
      string[] parts = name.Split('.');
      for (int i = 0; i < parts.Length; i++)
        if (!parts[i].StartsWith("`", StringComparison.Ordinal))
          parts[i] = String.Format("`{0}`", parts[i]);
      if (parts.Length == 1) return parts[0];
      return String.Format("{0}.{1}", parts[0], parts[1]);
    }

    private MyCatParameter GetAndFixParameter(string spName, MyCatSchemaRow param, bool realAsFloat, MyCatParameter returnParameter)
    {
      string mode = (string)param["PARAMETER_MODE"];
      string pName = (string)param["PARAMETER_NAME"];

      if (param["ORDINAL_POSITION"].Equals(0))
      {
        if (returnParameter == null)
          throw new InvalidOperationException(
              String.Format(Resources.RoutineRequiresReturnParameter, spName));
        pName = returnParameter.ParameterName;
      }

      // make sure the parameters given to us have an appropriate type set if it's not already
      MyCatParameter p = command.Parameters.GetParameterFlexible(pName, true);
      if (!p.TypeHasBeenSet)
      {
        string datatype = (string)param["DATA_TYPE"];
        bool unsigned = GetFlags(param["DTD_IDENTIFIER"].ToString()).IndexOf("UNSIGNED") != -1;
        p.MyCatDbType = MetaData.NameToType(datatype, unsigned, realAsFloat, Connection);
      }
      return p;
    }

    private MyCatParameterCollection CheckParameters(string spName)
    {
      MyCatParameterCollection newParms = new MyCatParameterCollection(command);
      MyCatParameter returnParameter = GetReturnParameter();

      ProcedureCacheEntry entry = GetParameters(spName);
      if (entry.procedure == null || entry.procedure.Rows.Count == 0)
        throw new InvalidOperationException(String.Format(Resources.RoutineNotFound, spName));

      bool realAsFloat = entry.procedure.Rows[0]["SQL_MODE"].ToString().IndexOf("REAL_AS_FLOAT") != -1;

      foreach (MyCatSchemaRow param in entry.parameters.Rows)
        newParms.Add(GetAndFixParameter(spName, param, realAsFloat, returnParameter));
      return newParms;
    }

    public override void Resolve(bool preparing)
    {
      // check to see if we are already resolved
      if (resolvedCommandText != null) return;

      serverProvidingOutputParameters = Driver.SupportsOutputParameters && preparing;

      // first retrieve the procedure definition from our
      // procedure cache
      string spName = commandText;
      if (spName.IndexOf(".") == -1 && !String.IsNullOrEmpty(Connection.Database))
        spName = Connection.Database + "." + spName;
      spName = FixProcedureName(spName);

      MyCatParameter returnParameter = GetReturnParameter();

      MyCatParameterCollection parms = command.Connection.Settings.CheckParameters ?
          CheckParameters(spName) : Parameters;

      string setSql = SetUserVariables(parms, preparing);
      string callSql = CreateCallStatement(spName, returnParameter, parms);
      string outSql = CreateOutputSelect(parms, preparing);
      resolvedCommandText = String.Format("{0}{1}{2}", setSql, callSql, outSql);
    }

    private string SetUserVariables(MyCatParameterCollection parms, bool preparing)
    {
      StringBuilder setSql = new StringBuilder();

      if (serverProvidingOutputParameters) return setSql.ToString();

      string delimiter = String.Empty;
      foreach (MyCatParameter p in parms)
      {
        if (p.Direction != ParameterDirection.InputOutput) continue;

        string pName = "@" + p.BaseName;
        string uName = "@" + ParameterPrefix + p.BaseName;
        string sql = String.Format("SET {0}={1}", uName, pName);

        if (command.Connection.Settings.AllowBatch && !preparing)
        {
          setSql.AppendFormat(CultureInfo.InvariantCulture, "{0}{1}", delimiter, sql);
          delimiter = "; ";
        }
        else
        {
          MyCatCommand cmd = new MyCatCommand(sql, command.Connection);
          cmd.Parameters.Add(p);
          cmd.ExecuteNonQuery();
        }
      }
      if (setSql.Length > 0)
        setSql.Append("; ");
      return setSql.ToString();
    }

    private string CreateCallStatement(string spName, MyCatParameter returnParameter, MyCatParameterCollection parms)
    {
      StringBuilder callSql = new StringBuilder();

      string delimiter = String.Empty;
      foreach (MyCatParameter p in parms)
      {
        if (p.Direction == ParameterDirection.ReturnValue) continue;

        string pName = "@" + p.BaseName;
        string uName = "@" + ParameterPrefix + p.BaseName;

        bool useRealVar = p.Direction == ParameterDirection.Input || serverProvidingOutputParameters;
        callSql.AppendFormat(CultureInfo.InvariantCulture, "{0}{1}", delimiter, useRealVar ? pName : uName);
        delimiter = ", ";
      }

      if (returnParameter == null)
        return String.Format("CALL {0}({1})", spName, callSql.ToString());
      else
        return String.Format("SET @{0}{1}={2}({3})", ParameterPrefix, returnParameter.BaseName, spName, callSql.ToString());
    }

    private string CreateOutputSelect(MyCatParameterCollection parms, bool preparing)
    {
      StringBuilder outSql = new StringBuilder();

      string delimiter = String.Empty;
      foreach (MyCatParameter p in parms)
      {
        if (p.Direction == ParameterDirection.Input) continue;
        if ((p.Direction == ParameterDirection.InputOutput ||
            p.Direction == ParameterDirection.Output) &&
            serverProvidingOutputParameters) continue;
        string pName = "@" + p.BaseName;
        string uName = "@" + ParameterPrefix + p.BaseName;

        outSql.AppendFormat(CultureInfo.InvariantCulture, "{0}{1}", delimiter, uName);
        delimiter = ", ";
      }

      if (outSql.Length == 0) return String.Empty;

      if (command.Connection.Settings.AllowBatch && !preparing)
        return String.Format(";SELECT {0}", outSql.ToString());

      outSelect = String.Format("SELECT {0}", outSql.ToString());
      return String.Empty;
    }

    internal void ProcessOutputParameters(MyCatDataReader reader)
    {
      // We apparently need to always adjust our output types since the server
      // provided data types are not always right
      AdjustOutputTypes(reader);

      if ((reader.CommandBehavior & CommandBehavior.SchemaOnly) != 0)
        return;
      
      // now read the output parameters data row
      reader.Read();

      string prefix = "@" + StoredProcedure.ParameterPrefix;

      for (int i = 0; i < reader.FieldCount; i++)
      {
        string fieldName = reader.GetName(i);
        if (fieldName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
          fieldName = fieldName.Remove(0, prefix.Length);
        MyCatParameter parameter = command.Parameters.GetParameterFlexible(fieldName, true);
        parameter.Value = reader.GetValue(i);
      }
    }

    private void AdjustOutputTypes(MyCatDataReader reader)
    {
      // since MySQL likes to return user variables as strings
      // we reset the types of the readers internal value objects
      // this will allow those value objects to parse the string based
      // return values
      for (int i = 0; i < reader.FieldCount; i++)
      {
        string fieldName = reader.GetName(i);
        if (fieldName.IndexOf(StoredProcedure.ParameterPrefix) != -1)
          fieldName = fieldName.Remove(0, StoredProcedure.ParameterPrefix.Length + 1);
        MyCatParameter parameter = command.Parameters.GetParameterFlexible(fieldName, true);

        IMyCatValue v = MyCatField.GetIMyCatValue(parameter.MyCatDbType);
        if (v is MyCatBit)
        {
          MyCatBit bit = (MyCatBit)v;
          bit.ReadAsString = true;
          reader.ResultSet.SetValueObject(i, bit);
        }
        else
          reader.ResultSet.SetValueObject(i, v);
      }
    }

    public override void Close(MyCatDataReader reader)
    {
      base.Close(reader);
      if (String.IsNullOrEmpty(outSelect)) return;
      if ((reader.CommandBehavior & CommandBehavior.SchemaOnly) != 0) return;

      MyCatCommand cmd = new MyCatCommand(outSelect, command.Connection);
      using (MyCatDataReader rdr = cmd.ExecuteReader(reader.CommandBehavior))
      {
        ProcessOutputParameters(rdr);
      }
    }
  }
}
