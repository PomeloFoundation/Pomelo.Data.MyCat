// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using Pomelo.Data.MyCat;
using System.Globalization;

namespace Pomelo.Data.Types
{
  internal struct MyCatUInt32 : IMyCatValue
  {
    private uint mValue;
    private bool isNull;
    private bool is24Bit;

    private MyCatUInt32(MyCatDbType type)
    {
      is24Bit = type == MyCatDbType.Int24 ? true : false;
      isNull = true;
      mValue = 0;
    }

    public MyCatUInt32(MyCatDbType type, bool isNull)
      : this(type)
    {
      this.isNull = isNull;
    }

    public MyCatUInt32(MyCatDbType type, uint val)
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
      get { return MyCatDbType.UInt32; }
    }

    object IMyCatValue.Value
    {
      get { return mValue; }
    }

    public uint Value
    {
      get { return mValue; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(UInt32); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return is24Bit ? "MEDIUMINT" : "INT"; }
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object v, int length)
    {
      uint val = (v is uint) ? (uint)v : Convert.ToUInt32(v);
      if (binary)
        packet.WriteInteger((long)val, is24Bit ? 3 : 4);
      else
        packet.WriteStringNoNull(val.ToString());
    }

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      if (nullVal)
        return new MyCatUInt32((this as IMyCatValue).MyCatDbType, true);

      if (length == -1)
        return new MyCatUInt32((this as IMyCatValue).MyCatDbType,
                     (uint)packet.ReadInteger(4));
      else
        return new MyCatUInt32((this as IMyCatValue).MyCatDbType,
                     UInt32.Parse(packet.ReadString(length), NumberStyles.Any, CultureInfo.InvariantCulture));
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      packet.Position += 4;
    }

    #endregion

    internal static void SetDSInfo(MyCatSchemaCollection sc)
    {
      string[] types = new string[] { "MEDIUMINT", "INT" };
      MyCatDbType[] dbtype = new MyCatDbType[] { MyCatDbType.UInt24, 
                MyCatDbType.UInt32 };

      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      for (int x = 0; x < types.Length; x++)
      {
        MyCatSchemaRow row = sc.AddRow();
        row["TypeName"] = types[x];
        row["ProviderDbType"] = dbtype[x];
        row["ColumnSize"] = 0;
        row["CreateFormat"] = types[x] + " UNSIGNED";
        row["CreateParameters"] = null;
        row["DataType"] = "System.UInt32";
        row["IsAutoincrementable"] = true;
        row["IsBestMatch"] = true;
        row["IsCaseSensitive"] = false;
        row["IsFixedLength"] = true;
        row["IsFixedPrecisionScale"] = true;
        row["IsLong"] = false;
        row["IsNullable"] = true;
        row["IsSearchable"] = true;
        row["IsSearchableWithLike"] = false;
        row["IsUnsigned"] = true;
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
