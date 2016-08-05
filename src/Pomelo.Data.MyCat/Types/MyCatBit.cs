// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{
  /// <summary>
  /// Summary description for MyCatUInt64.
  /// </summary>
  internal struct MyCatBit : IMyCatValue
  {
    private ulong mValue;
    private bool isNull;
    private bool readAsString;

    public MyCatBit(bool isnull)
    {
      mValue = 0;
      isNull = isnull;
      readAsString = false;
    }

    public bool ReadAsString
    {
      get { return readAsString; }
      set { readAsString = value; }
    }

    public bool IsNull
    {
      get { return isNull; }
    }

    MyCatDbType IMyCatValue.MyCatDbType
    {
      get { return MyCatDbType.Bit; }
    }

    object IMyCatValue.Value
    {
      get
      {
        return mValue;
      }
    }

    Type IMyCatValue.SystemType
    {
      get
      {
        return typeof(UInt64);
      }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return "BIT"; }
    }

    public void WriteValue(MyCatPacket packet, bool binary, object value, int length)
    {
      ulong v = (value is UInt64) ? (UInt64)value : Convert.ToUInt64(value);
      if (binary)
        packet.WriteInteger((long)v, 8);
      else
        packet.WriteStringNoNull(v.ToString());
    }

    public IMyCatValue ReadValue(MyCatPacket packet, long length, bool isNull)
    {
      this.isNull = isNull;
      if (isNull)
        return this;

      if (length == -1)
        length = packet.ReadFieldLength();

      if (ReadAsString)
        mValue = UInt64.Parse(packet.ReadString(length));
      else
        mValue = (UInt64)packet.ReadBitValue((int)length);
      return this;
    }

    public void SkipValue(MyCatPacket packet)
    {
      int len = (int)packet.ReadFieldLength();
      packet.Position += len;
    }

    internal static void SetDSInfo(MyCatSchemaCollection sc)
    {
      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      MyCatSchemaRow row = sc.AddRow();
      row["TypeName"] = "BIT";
      row["ProviderDbType"] = MyCatDbType.Bit;
      row["ColumnSize"] = 64;
      row["CreateFormat"] = "BIT";
      row["CreateParameters"] = DBNull.Value; ;
      row["DataType"] = typeof(UInt64).ToString();
      row["IsAutoincrementable"] = false;
      row["IsBestMatch"] = true;
      row["IsCaseSensitive"] = false;
      row["IsFixedLength"] = false;
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
      row["LiteralPrefix"] = DBNull.Value;
      row["LiteralSuffix"] = DBNull.Value;
      row["NativeDataType"] = DBNull.Value;
    }
  }
}
