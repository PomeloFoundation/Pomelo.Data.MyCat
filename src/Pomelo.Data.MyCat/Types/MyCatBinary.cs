// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.Text;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{

    internal struct MyCatBinary : IMyCatValue
    {
        private MyCatDbType type;
        private byte[] mValue;
        private bool isNull;

        public MyCatBinary(MyCatDbType type, bool isNull)
        {
            this.type = type;
            this.isNull = isNull;
            mValue = null;
        }

        public MyCatBinary(MyCatDbType type, byte[] val)
        {
            this.type = type;
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
            get { return type; }
        }

        object IMyCatValue.Value
        {
            get { return mValue; }
        }

        public byte[] Value
        {
            get { return mValue; }
        }

        Type IMyCatValue.SystemType
        {
            get { return typeof(byte[]); }
        }

        string IMyCatValue.MyCatTypeName
        {
            get
            {
                switch (type)
                {
                    case MyCatDbType.TinyBlob: return "TINY_BLOB";
                    case MyCatDbType.MediumBlob: return "MEDIUM_BLOB";
                    case MyCatDbType.LongBlob: return "LONG_BLOB";
                    case MyCatDbType.Blob:
                    default:
                        return "BLOB";
                }
            }
        }

        void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
        {
            byte[] buffToWrite = (val as byte[]);
            if (buffToWrite == null)
            {
                char[] valAsChar = (val as Char[]);
                if (valAsChar != null)
                    buffToWrite = packet.Encoding.GetBytes(valAsChar);
                else
                {
                    string s = val.ToString();
                    if (length == 0)
                        length = s.Length;
                    else
                        s = s.Substring(0, length);
                    buffToWrite = packet.Encoding.GetBytes(s);
                }
            }

            // we assume zero length means write all of the value
            if (length == 0)
                length = buffToWrite.Length;

            if (buffToWrite == null)
                throw new MyCatException("Only byte arrays and strings can be serialized by MyCatBinary");

            if (binary)
            {
                packet.WriteLength(length);
                packet.Write(buffToWrite, 0, length);
            }
            else
            {
                packet.WriteStringNoNull("X");
                packet.WriteStringNoNull("\'");
                packet.WriteStringNoNull(ToHexString(buffToWrite));
                packet.WriteStringNoNull("\'");
            }
        }

        public static string ToHexString(byte[] bytes)
        {
            var byteStr = new StringBuilder();
            if (bytes != null || bytes.Length > 0)
            {
                foreach (var item in bytes)
                {
                    byteStr.Append(string.Format("{0:x2}", item));
                }
            }
            return byteStr.ToString();
        }

        private static void EscapeByteArray(byte[] bytes, int length, MyCatPacket packet)
        {
            for (int x = 0; x < Math.Min(length, bytes.Length); x++)
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

        IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
        {
            MyCatBinary b;
            if (nullVal)
                b = new MyCatBinary(type, true);
            else
            {
                if (length == -1)
                    length = (long)packet.ReadFieldLength();

                byte[] newBuff = new byte[length];
                packet.Read(newBuff, 0, (int)length);
                b = new MyCatBinary(type, newBuff);
            }
            return b;
        }

        void IMyCatValue.SkipValue(MyCatPacket packet)
        {
            int len = (int)packet.ReadFieldLength();
            packet.Position += len;
        }

        #endregion

        public static void SetDSInfo(MyCatSchemaCollection sc)
        {
            string[] types = new string[] { "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY" };
            MyCatDbType[] dbtype = new MyCatDbType[] { MyCatDbType.Blob,
                MyCatDbType.TinyBlob, MyCatDbType.MediumBlob, MyCatDbType.LongBlob, MyCatDbType.Binary, MyCatDbType.VarBinary };
            long[] sizes = new long[] { 65535L, 255L, 16777215L, 4294967295L, 255L, 65535L };
            string[] format = new string[] { null, null, null, null, "binary({0})", "varbinary({0})" };
            string[] parms = new string[] { null, null, null, null, "length", "length" };

            // we use name indexing because this method will only be called
            // when GetSchema is called for the DataSourceInformation 
            // collection and then it wil be cached.
            for (int x = 0; x < types.Length; x++)
            {
                MyCatSchemaRow row = sc.AddRow();
                row["TypeName"] = types[x];
                row["ProviderDbType"] = dbtype[x];
                row["ColumnSize"] = sizes[x];
                row["CreateFormat"] = format[x];
                row["CreateParameters"] = parms[x];
                row["DataType"] = "System.Byte[]";
                row["IsAutoincrementable"] = false;
                row["IsBestMatch"] = true;
                row["IsCaseSensitive"] = false;
                row["IsFixedLength"] = x < 4 ? false : true;
                row["IsFixedPrecisionScale"] = false;
                row["IsLong"] = sizes[x] > 255;
                row["IsNullable"] = true;
                row["IsSearchable"] = false;
                row["IsSearchableWithLike"] = false;
                row["IsUnsigned"] = DBNull.Value;
                row["MaximumScale"] = DBNull.Value;
                row["MinimumScale"] = DBNull.Value;
                row["IsConcurrencyType"] = DBNull.Value;
                row["IsLiteralSupported"] = false;
                row["LiteralPrefix"] = "0x";
                row["LiteralSuffix"] = DBNull.Value;
                row["NativeDataType"] = DBNull.Value;
            }
        }
    }
}
