// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.Globalization;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{

  internal struct MyCatDouble : IMyCatValue
  {
    private double mValue;
    private bool isNull;

    public MyCatDouble(bool isNull)
    {
      this.isNull = isNull;
      mValue = 0.0;
    }

    public MyCatDouble(double val)
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
      get { return MyCatDbType.Double; }
    }

    object IMyCatValue.Value
    {
      get { return mValue; }
    }

    public double Value
    {
      get { return mValue; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(double); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return "DOUBLE"; }
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      double v = (val is double) ? (double)val : Convert.ToDouble(val);
      if (binary)
        packet.Write(BitConverter.GetBytes(v));
      else
        packet.WriteStringNoNull(v.ToString("R", CultureInfo.InvariantCulture));
    }

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length,
    bool nullVal)
    {
      if (nullVal)
        return new MyCatDouble(true);

      if (length == -1)
      {
        byte[] b = new byte[8];
        packet.Read(b, 0, 8);
        return new MyCatDouble(BitConverter.ToDouble(b, 0));
      }
      string s = packet.ReadString(length);
      double d;
      try
      {
        d = Double.Parse(s, CultureInfo.InvariantCulture);
      }
      catch (OverflowException)
      {
        // MySQL server < 5.5 can return values not compatible with
        // Double.Parse(), i.e out of range for double.

        if (s.StartsWith("-", StringComparison.Ordinal))
          d = double.MinValue;
        else
          d = double.MaxValue;
      }
      return new MyCatDouble(d);
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
      row["TypeName"] = "DOUBLE";
      row["ProviderDbType"] = MyCatDbType.Double;
      row["ColumnSize"] = 0;
      row["CreateFormat"] = "DOUBLE";
      row["CreateParameters"] = null;
      row["DataType"] = "System.Double";
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