// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{
  internal struct MyCatInt16 : IMyCatValue
  {
    private short mValue;
    private bool isNull;

    public MyCatInt16(bool isNull)
    {
      this.isNull = isNull;
      mValue = 0;
    }

    public MyCatInt16(short val)
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
      get { return MyCatDbType.Int16; }
    }

    object IMyCatValue.Value
    {
      get { return mValue; }
    }

    public short Value
    {
      get { return mValue; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(short); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return "SMALLINT"; }
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      int v = (val is Int32) ? (int)val : Convert.ToInt32(val);
      if (binary)
        packet.WriteInteger((long)v, 2);
      else
        packet.WriteStringNoNull(v.ToString());
    }

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      if (nullVal)
        return new MyCatInt16(true);

      if (length == -1)
        return new MyCatInt16((short)packet.ReadInteger(2));
      else
        return new MyCatInt16(Int16.Parse(packet.ReadString(length)));
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      packet.Position += 2;
    }

    #endregion

    internal static void SetDSInfo(MyCatSchemaCollection sc)
    {
      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      MyCatSchemaRow row = sc.AddRow();
      row["TypeName"] = "SMALLINT";
      row["ProviderDbType"] = MyCatDbType.Int16;
      row["ColumnSize"] = 0;
      row["CreateFormat"] = "SMALLINT";
      row["CreateParameters"] = null;
      row["DataType"] = "System.Int16";
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
