// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{
  internal struct MyCatTimeSpan : IMyCatValue
  {
    private TimeSpan mValue;
    private bool isNull;

    public MyCatTimeSpan(bool isNull)
    {
      this.isNull = isNull;
      mValue = TimeSpan.MinValue;
    }

    public MyCatTimeSpan(TimeSpan val)
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
      get { return MyCatDbType.Time; }
    }

    object IMyCatValue.Value
    {
      get { return mValue; }
    }

    public TimeSpan Value
    {
      get { return mValue; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(TimeSpan); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return "TIME"; }
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      if (!(val is TimeSpan))
        throw new MyCatException("Only TimeSpan objects can be serialized by MyCatTimeSpan");

      TimeSpan ts = (TimeSpan)val;
      bool negative = ts.TotalMilliseconds < 0;
      ts = ts.Duration();

      if (binary)
      {
        if (ts.Milliseconds > 0)
          packet.WriteByte(12);
        else
          packet.WriteByte(8);

        packet.WriteByte((byte)(negative ? 1 : 0));        
        packet.WriteInteger(ts.Days, 4);
        packet.WriteByte((byte)ts.Hours);
        packet.WriteByte((byte)ts.Minutes);
        packet.WriteByte((byte)ts.Seconds);
        if (ts.Milliseconds > 0)
        {
          long mval = ts.Milliseconds*1000;
          packet.WriteInteger(mval, 4);          
        }
      }
      else
      {
        String s = String.Format("'{0}{1} {2:00}:{3:00}:{4:00}.{5:0000000}'",
            negative ? "-" : "", ts.Days, ts.Hours, ts.Minutes, ts.Seconds, ts.Ticks % 10000000);
			
        packet.WriteStringNoNull(s);
      }
    }


    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      if (nullVal) return new MyCatTimeSpan(true);

      if (length >= 0)
      {
        string value = packet.ReadString(length);
        ParseMyCat(value);
        return this;
      }

      long bufLength = packet.ReadByte();
      int negate = 0;
      if (bufLength > 0)
        negate = packet.ReadByte();

      isNull = false;
      if (bufLength == 0)
        isNull = true;
      else if (bufLength == 5)
        mValue = new TimeSpan(packet.ReadInteger(4), 0, 0, 0);
      else if (bufLength == 8)
        mValue = new TimeSpan(packet.ReadInteger(4),
             packet.ReadByte(), packet.ReadByte(), packet.ReadByte());
      else
        mValue = new TimeSpan(packet.ReadInteger(4),
             packet.ReadByte(), packet.ReadByte(), packet.ReadByte(),
             packet.ReadInteger(4) / 1000000);

      if (negate == 1)
        mValue = mValue.Negate();
      return this;
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      int len = packet.ReadByte();
      packet.Position += len;
    }

    #endregion

    internal static void SetDSInfo(MyCatSchemaCollection sc)
    {
      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      MyCatSchemaRow row = sc.AddRow();
      row["TypeName"] = "TIME";
      row["ProviderDbType"] = MyCatDbType.Time;
      row["ColumnSize"] = 0;
      row["CreateFormat"] = "TIME";
      row["CreateParameters"] = null;
      row["DataType"] = "System.TimeSpan";
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

    public override string ToString()
    {
      return String.Format("{0} {1:00}:{2:00}:{3:00}",
        mValue.Days, mValue.Hours, mValue.Minutes, mValue.Seconds);
    }

    private void ParseMyCat(string s)
    {

      string[] parts = s.Split(':', '.');
      int hours = Int32.Parse(parts[0]);
      int mins = Int32.Parse(parts[1]);
      int secs = Int32.Parse(parts[2]);
      int nanoseconds = 0;

      if (parts.Length > 3)
      {
        //if the data is saved in MyCat as Time(3) the division by 1000 always returns 0, but handling the data as Time(6) the result is the expected
        parts[3] = parts[3].PadRight(7, '0');
        nanoseconds = int.Parse(parts[3]);
      }


      if (hours < 0 || parts[0].StartsWith("-", StringComparison.Ordinal))
      {
        mins *= -1;
        secs *= -1;
        nanoseconds *= -1;
      }
      int days = hours / 24;
      hours = hours - (days * 24);
      mValue = new TimeSpan(days, hours, mins, secs).Add(new TimeSpan(nanoseconds));
      isNull = false;
    }
  }
}
