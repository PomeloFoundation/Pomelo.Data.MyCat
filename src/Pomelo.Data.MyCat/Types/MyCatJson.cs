// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.IO;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{
    internal struct MyCatJson : IMyCatValue
    {
        private string mValue;
        private bool isNull;

        public MyCatJson(bool isNull)
        {
            this.isNull = isNull;
            mValue = String.Empty;
        }

        public MyCatJson(string val)
        {
            this.isNull = false;
            mValue = val;
        }

        #region IMyCatValue Members

        public bool IsNull
        {
            get { return isNull; }
        }

        object IMyCatValue.Value
        {
            get { return mValue; }
        }

        public string Value
        {
            get { return mValue; }
        }

        Type IMyCatValue.SystemType
        {
            get { return typeof(string); }
        }

        MyCatDbType IMyCatValue.MyCatDbType
        {
            get { return MyCatDbType.JSON; }
        }

        public string MyCatTypeName
        {
            get
            {
                return "JSON";
            }
        }

        void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
        {
            string v = val.ToString();
            if (length > 0)
            {
                length = Math.Min(length, v.Length);
                v = v.Substring(0, length);
            }

            if (binary)
                packet.WriteLenString(v);
            else
                packet.WriteStringNoNull("'" + MyCatHelper.EscapeString(v) + "'");
        }

        void IMyCatValue.SkipValue(MyCatPacket packet)
        {
            int len = (int)packet.ReadFieldLength();
            packet.Position += len;
        }

        #endregion

        internal static void SetDSInfo(MyCatSchemaCollection sc)
        {
            MyCatSchemaRow row = sc.AddRow();
            row["TypeName"] = "JSON";
            row["ProviderDbType"] = MyCatDbType.JSON;
            row["ColumnSize"] = 0;
            row["CreateFormat"] = "JSON";
            row["CreateParameters"] = null;
            row["DataType"] = "System.String";
            row["IsAutoincrementable"] = false;
            row["IsBestMatch"] = true;
            row["IsCaseSensitive"] = false;
            row["IsFixedLength"] = false;
            row["IsFixedPrecisionScale"] = true;
            row["IsLong"] = false;
            row["IsNullable"] = true;
            row["IsSearchable"] = true;
            row["IsSearchableWithLike"] = true;
            row["IsUnsigned"] = false;
            row["MaximumScale"] = 0;
            row["MinimumScale"] = 0;
            row["IsConcurrencyType"] = DBNull.Value;
            row["IsLiteralSupported"] = false;
            row["LiteralPrefix"] = null;
            row["LiteralSuffix"] = null;
            row["NativeDataType"] = null;
        }

        public IMyCatValue ReadValue(MyCatPacket packet, long length, bool isNull)
        {
            if (isNull)
                return new MyCatJson(true);

            string s = String.Empty;
            if (length == -1)
                s = packet.ReadLenString();
            else
                s = packet.ReadString(length);
            MyCatJson str = new MyCatJson(s);
            return str;
        }
    }
}