// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{
  internal struct MyCatInt64 : IMyCatValue
  {
    private long mValue;
    private bool isNull;

    public MyCatInt64(bool isNull)
    {
      this.isNull = isNull;
      mValue = 0;
    }

    public MyCatInt64(long val)
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
      get { return MyCatDbType.Int64; }
    }

    object IMyCatValue.Value
    {
      get { return mValue; }
    }

    public long Value
    {
      get { return mValue; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(long); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return "BIGINT"; }
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      long v = (val is Int64) ? (Int64)val : Convert.ToInt64(val);
      if (binary)
        packet.WriteInteger(v, 8);
      else
        packet.WriteStringNoNull(v.ToString());
    }

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      if (nullVal)
        return new MyCatInt64(true);

      if (length == -1)
        return new MyCatInt64((long)packet.ReadULong(8));
      else
        return new MyCatInt64(Int64.Parse(packet.ReadString(length)));
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      packet.Position += 8;
    }

    #endregion

    internal static void SetDSInfo(MyCatSchemaCollection sc)
    {
      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      MyCatSchemaRow row = sc.AddRow();
      row["TypeName"] = "BIGINT";
      row["ProviderDbType"] = MyCatDbType.Int64;
      row["ColumnSize"] = 0;
      row["CreateFormat"] = "BIGINT";
      row["CreateParameters"] = null;
      row["DataType"] = "System.Int64";
      row["IsAutoincrementable"] = true;
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
