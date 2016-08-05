// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{
  internal struct MyCatUByte : IMyCatValue
  {
    private byte mValue;
    private bool isNull;

    public MyCatUByte(bool isNull)
    {
      this.isNull = isNull;
      mValue = 0;
    }

    public MyCatUByte(byte val)
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
      get { return MyCatDbType.UByte; }
    }

    object IMyCatValue.Value
    {
      get { return mValue; }
    }

    public byte Value
    {
      get { return mValue; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(byte); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return "TINYINT"; }
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      byte v = (val is byte) ? (byte)val : Convert.ToByte(val);
      if (binary)
        packet.WriteByte(v);
      else
        packet.WriteStringNoNull(v.ToString());
    }

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      if (nullVal)
        return new MyCatUByte(true);

      if (length == -1)
        return new MyCatUByte((byte)packet.ReadByte());
      else
        return new MyCatUByte(Byte.Parse(packet.ReadString(length)));
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      packet.ReadByte();
    }

    #endregion

    internal static void SetDSInfo(MyCatSchemaCollection sc)
    {
      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      MyCatSchemaRow row = sc.AddRow();
      row["TypeName"] = "TINY INT";
      row["ProviderDbType"] = MyCatDbType.UByte;
      row["ColumnSize"] = 0;
      row["CreateFormat"] = "TINYINT UNSIGNED";
      row["CreateParameters"] = null;
      row["DataType"] = "System.Byte";
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
