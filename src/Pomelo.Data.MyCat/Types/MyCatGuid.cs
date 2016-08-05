// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using Pomelo.Data.MyCat;


namespace Pomelo.Data.Types
{

  internal struct MyCatGuid : IMyCatValue
  {
    Guid mValue;
    private bool isNull;
    private byte[] bytes;
    private bool oldGuids;

    public MyCatGuid(byte[] buff)
    {
      oldGuids = false;
      mValue = new Guid(buff);
      isNull = false;
      bytes = buff;
    }

    public byte[] Bytes
    {
      get { return bytes; }
    }

    public bool OldGuids
    {
      get { return oldGuids; }
      set { oldGuids = value; }
    }

    #region IMyCatValue Members

    public bool IsNull
    {
      get { return isNull; }
    }

    MyCatDbType IMyCatValue.MyCatDbType
    {
      get { return MyCatDbType.Guid; }
    }

    object IMyCatValue.Value
    {
      get { return mValue; }
    }

    public Guid Value
    {
      get { return mValue; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(Guid); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get { return OldGuids ? "BINARY(16)" : "CHAR(36)"; }
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      Guid guid = Guid.Empty;
      string valAsString = val as string;
      byte[] valAsByte = val as byte[];

      if (val is Guid)
        guid = (Guid)val;
      else
      {
        try
        {
          if (valAsString != null)
            guid = new Guid(valAsString);
          else if (valAsByte != null)
            guid = new Guid(valAsByte);
        }
        catch (Exception ex)
        {
          throw new MyCatException(Resources.DataNotInSupportedFormat, ex);
        }
      }

      if (OldGuids)
        WriteOldGuid(packet, guid, binary);
      else
      {
        guid.ToString("D");

        if (binary)
          packet.WriteLenString(guid.ToString("D"));
        else
          packet.WriteStringNoNull("'" + MyCatHelper.EscapeString(guid.ToString("D")) + "'");
      }
    }

    private void WriteOldGuid(MyCatPacket packet, Guid guid, bool binary)
    {
      byte[] bytes = guid.ToByteArray();

      if (binary)
      {
        packet.WriteLength(bytes.Length);
        packet.Write(bytes);
      }
      else
      {
        packet.WriteStringNoNull("_binary ");
        packet.WriteByte((byte)'\'');
        EscapeByteArray(bytes, bytes.Length, packet);
        packet.WriteByte((byte)'\'');
      }
    }

    private static void EscapeByteArray(byte[] bytes, int length, MyCatPacket packet)
    {
      for (int x = 0; x < length; x++)
      {
        byte b = bytes[x];
        if (b == '\0')
        {
          packet.WriteByte((byte)'\\');
          packet.WriteByte((byte)'0');
        }

        else if (b == '\\' || b == '\'' || b == '\"')
        {
          packet.WriteByte((byte)'\\');
          packet.WriteByte(b);
        }
        else
          packet.WriteByte(b);
      }
    }

    private MyCatGuid ReadOldGuid(MyCatPacket packet, long length)
    {
      if (length == -1)
        length = (long)packet.ReadFieldLength();

      byte[] buff = new byte[length];
      packet.Read(buff, 0, (int)length);
      MyCatGuid g = new MyCatGuid(buff);
      g.OldGuids = OldGuids;
      return g;
    }

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      MyCatGuid g = new MyCatGuid();
      g.isNull = true;
      g.OldGuids = OldGuids;
      if (!nullVal)
      {
        if (OldGuids)
          return ReadOldGuid(packet, length);
        string s = String.Empty;
        if (length == -1)
          s = packet.ReadLenString();
        else
          s = packet.ReadString(length);
        g.mValue = new Guid(s);
        g.isNull = false;
      }
      return g;
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      int len = (int)packet.ReadFieldLength();
      packet.Position += len;
    }

    #endregion

    public static void SetDSInfo(MyCatSchemaCollection sc)
    {
      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      MyCatSchemaRow row = sc.AddRow();
      row["TypeName"] = "GUID";
      row["ProviderDbType"] = MyCatDbType.Guid;
      row["ColumnSize"] = 0;
      row["CreateFormat"] = "BINARY(16)";
      row["CreateParameters"] = null;
      row["DataType"] = "System.Guid";
      row["IsAutoincrementable"] = false;
      row["IsBestMatch"] = true;
      row["IsCaseSensitive"] = false;
      row["IsFixedLength"] = true;
      row["IsFixedPrecisionScale"] = true;
      row["IsLong"] = false;
      row["IsNullable"] = true;
      row["IsSearchable"] = false;
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
