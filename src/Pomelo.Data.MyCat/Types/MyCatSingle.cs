// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using Pomelo.Data.MyCat;
using System.Globalization;

namespace Pomelo.Data.Types
{
  internal struct MyCatSingle : IMyCatValue
  {
    private float mValue;
    private bool isNull;

    public MyCatSingle(bool isNull)
    {
      this.isNull = isNull;
      mValue = 0.0f;
    }

    public MyCatSingle(float val)
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
      get { return MyCatDbType.Float; }
    }

    object IMyCatValue.Value
    {
      get { return mValue; }
    }

    public float Value
    {
      get { return mValue; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(float); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return "FLOAT"; }
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      Single v = (val is Single) ? (Single)val : Convert.ToSingle(val);
      if (binary)
        packet.Write(BitConverter.GetBytes(v));
      else
        packet.WriteStringNoNull(v.ToString("R",
   CultureInfo.InvariantCulture));
    }

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      if (nullVal)
        return new MyCatSingle(true);

      if (length == -1)
      {
        byte[] b = new byte[4];
        packet.Read(b, 0, 4);
        return new MyCatSingle(BitConverter.ToSingle(b, 0));
      }
      return new MyCatSingle(Single.Parse(packet.ReadString(length),
     CultureInfo.InvariantCulture));
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      packet.Position += 4;
    }

    #endregion

    internal static void SetDSInfo(MyCatSchemaCollection sc)
    {
      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      MyCatSchemaRow row = sc.AddRow();
      row["TypeName"] = "FLOAT";
      row["ProviderDbType"] = MyCatDbType.Float;
      row["ColumnSize"] = 0;
      row["CreateFormat"] = "FLOAT";
      row["CreateParameters"] = null;
      row["DataType"] = "System.Single";
      row["IsAutoincrementable"] = false;
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