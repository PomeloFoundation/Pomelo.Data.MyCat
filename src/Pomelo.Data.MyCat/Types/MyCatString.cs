// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.IO;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{
  internal struct MyCatString : IMyCatValue
  {
    private string mValue;
    private bool isNull;
    private MyCatDbType type;

    public MyCatString(MyCatDbType type, bool isNull)
    {
      this.type = type;
      this.isNull = isNull;
      mValue = String.Empty;
    }

    public MyCatString(MyCatDbType type, string val)
    {
      this.type = type;
      this.isNull = false;
      mValue = val;
    }

    #region IMyCatValue Members

    public bool IsNull
    {
      get { return isNull; }
    }

    MyCatDbType IMyCatValue.MyCatDbType
    {
      get { return type; }
    }

    object IMyCatValue.Value
    {
      get { return mValue; }
    }

    public string Value
    {
      get { return mValue; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(string); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return type == MyCatDbType.Set ? "SET" : type == MyCatDbType.Enum ? "ENUM" : "VARCHAR"; }
    }


    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      string v = val.ToString();
      if (length > 0)
      {
        length = Math.Min(length, v.Length);
        v = v.Substring(0, length);
      }

      if (binary)
        packet.WriteLenString(v);
      else
        packet.WriteStringNoNull("'" + MyCatHelper.EscapeString(v) + "'");
    }

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      if (nullVal)
        return new MyCatString(type, true);

      string s = String.Empty;
      if (length == -1)
        s = packet.ReadLenString();
      else
        s = packet.ReadString(length);
      MyCatString str = new MyCatString(type, s);
      return str;
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      int len = (int)packet.ReadFieldLength();
      packet.Position += len;
    }

    #endregion

    internal static void SetDSInfo(MyCatSchemaCollection sc)
    {
      string[] types = new string[] { "CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "SET", 
                "ENUM", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT" };
      MyCatDbType[] dbtype = new MyCatDbType[] { MyCatDbType.String, MyCatDbType.String,
                MyCatDbType.VarChar, MyCatDbType.VarChar, MyCatDbType.Set, MyCatDbType.Enum, 
                MyCatDbType.TinyText, MyCatDbType.Text, MyCatDbType.MediumText, 
                MyCatDbType.LongText };

      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      for (int x = 0; x < types.Length; x++)
      {
        MyCatSchemaRow row = sc.AddRow();
        row["TypeName"] = types[x];
        row["ProviderDbType"] = dbtype[x];
        row["ColumnSize"] = 0;
        row["CreateFormat"] = x < 4 ? types[x] + "({0})" : types[x];
        row["CreateParameters"] = x < 4 ? "size" : null;
        row["DataType"] = "System.String";
        row["IsAutoincrementable"] = false;
        row["IsBestMatch"] = true;
        row["IsCaseSensitive"] = false;
        row["IsFixedLength"] = false;
        row["IsFixedPrecisionScale"] = true;
        row["IsLong"] = false;
        row["IsNullable"] = true;
        row["IsSearchable"] = true;
        row["IsSearchableWithLike"] = true;
        row["IsUnsigned"] = false;
        row["MaximumScale"] = 0;
        row["MinimumScale"] = 0;
        row["IsConcurrencyType"] = DBNull.Value;
        row["IsLiteralSupported"] = false;
        row["LiteralPrefix"] = null;
        row["LiteralSuffix"] = null;
        row["NativeDataType"] = null;
      }
    }
  }
}