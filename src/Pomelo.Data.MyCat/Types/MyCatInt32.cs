// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using Pomelo.Data.MyCat;
using System.Globalization;

namespace Pomelo.Data.Types
{
  internal struct MyCatInt32 : IMyCatValue
  {
    private int mValue;
    private bool isNull;
    private bool is24Bit;

    private MyCatInt32(MyCatDbType type)
    {
      is24Bit = type == MyCatDbType.Int24 ? true : false;
      isNull = true;
      mValue = 0;
    }

    public MyCatInt32(MyCatDbType type, bool isNull)
      : this(type)
    {
      this.isNull = isNull;
    }

    public MyCatInt32(MyCatDbType type, int val)
      : this(type)
    {
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
      get { return MyCatDbType.Int32; }
    }

    object IMyCatValue.Value
    {
      get { return mValue; }
    }

    public int Value
    {
      get { return mValue; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(Int32); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return is24Bit ? "MEDIUMINT" : "INT"; }
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      int v = (val is Int32) ? (int)val : Convert.ToInt32(val);
      if (binary)
        packet.WriteInteger((long)v, is24Bit ? 3 : 4);
      else
        packet.WriteStringNoNull(v.ToString());
    }

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      if (nullVal)
        return new MyCatInt32((this as IMyCatValue).MyCatDbType, true);

      if (length == -1)
        return new MyCatInt32((this as IMyCatValue).MyCatDbType,
                     packet.ReadInteger(4));
      else
        return new MyCatInt32((this as IMyCatValue).MyCatDbType,
                     Int32.Parse(packet.ReadString(length),
           CultureInfo.InvariantCulture));
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      packet.Position += 4;
    }

    #endregion

    internal static void SetDSInfo(MyCatSchemaCollection sc)
    {
      string[] types = new string[] { "INT", "YEAR", "MEDIUMINT" };
      MyCatDbType[] dbtype = new MyCatDbType[] { MyCatDbType.Int32, 
                MyCatDbType.Year, MyCatDbType.Int24 };

      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      for (int x = 0; x < types.Length; x++)
      {
        MyCatSchemaRow row = sc.AddRow();
        row["TypeName"] = types[x];
        row["ProviderDbType"] = dbtype[x];
        row["ColumnSize"] = 0;
        row["CreateFormat"] = types[x];
        row["CreateParameters"] = null;
        row["DataType"] = "System.Int32";
        row["IsAutoincrementable"] = dbtype[x] == MyCatDbType.Year ? false : true;
        row["IsBestMatch"] = true;
        row["IsCaseSensitive"] = false;
        row["IsFixedLength"] = true;
        row["IsFixedPrecisionScale"] = true;
        row["IsLong"] = false;
        row["IsNullable"] = true;
        row["IsSearchable"] = true;
        row["IsSearchableWithLike"] = false;
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
