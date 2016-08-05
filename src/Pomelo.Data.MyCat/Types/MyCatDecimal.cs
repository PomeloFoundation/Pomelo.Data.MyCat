// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using Pomelo.Data.MyCat;
using System.Globalization;

namespace Pomelo.Data.Types
{

  public struct MyCatDecimal : IMyCatValue
  {
    private byte precision;
    private byte scale;
    private string mValue;
    private bool isNull;

    internal MyCatDecimal(bool isNull)
    {
      this.isNull = isNull;
      mValue = null;
      precision = scale = 0;
    }

    internal MyCatDecimal(string val)
    {
      this.isNull = false;
      precision = scale = 0;
      mValue = val;
    }

    #region IMyCatValue Members

    public bool IsNull
    {
      get { return isNull; }
    }

    MyCatDbType IMyCatValue.MyCatDbType
    {
      get { return MyCatDbType.Decimal; }
    }

    public byte Precision
    {
      get { return precision; }
      set { precision = value; }
    }

    public byte Scale
    {
      get { return scale; }
      set { scale = value; }
    }


    object IMyCatValue.Value
    {
      get { return this.Value; }
    }

    public decimal Value
    {
      get { return Convert.ToDecimal(mValue, CultureInfo.InvariantCulture); }
    }

    public double ToDouble()
    {
      return Double.Parse(mValue);
    }

    public override string ToString()
    {
      return mValue;
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(decimal); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return "DECIMAL"; }
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      decimal v = (val is decimal) ? (decimal)val : Convert.ToDecimal(val);
      string valStr = v.ToString(CultureInfo.InvariantCulture);
      if (binary)
        packet.WriteLenString(valStr);
      else
        packet.WriteStringNoNull(valStr);
    }

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      if (nullVal)
        return new MyCatDecimal(true);

      string s = String.Empty;
      if (length == -1)
        s = packet.ReadLenString();
      else
        s = packet.ReadString(length);
      return new MyCatDecimal(s);
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      int len = (int)packet.ReadFieldLength();
      packet.Position += len;
    }

    #endregion

    internal static void SetDSInfo(MyCatSchemaCollection sc)
    {
      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      MyCatSchemaRow row = sc.AddRow();
      row["TypeName"] = "DECIMAL";
      row["ProviderDbType"] = MyCatDbType.NewDecimal;
      row["ColumnSize"] = 0;
      row["CreateFormat"] = "DECIMAL({0},{1})";
      row["CreateParameters"] = "precision,scale";
      row["DataType"] = "System.Decimal";
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
