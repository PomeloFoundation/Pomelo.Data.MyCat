// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using Pomelo.Data.MyCat;
using System.Globalization;

namespace Pomelo.Data.Types
{
  internal class MetaData
  {
    public static bool IsNumericType(string typename)
    {
#if NETSTANDARD1_3
            string lowerType = typename.ToLower();
#else
             string lowerType = typename.ToLower(CultureInfo.InvariantCulture);
#endif

            switch (lowerType)
      {
        case "int":
        case "integer":
        case "numeric":
        case "decimal":
        case "dec":
        case "fixed":
        case "tinyint":
        case "mediumint":
        case "bigint":
        case "real":
        case "double":
        case "float":
        case "serial":
        case "smallint": return true;
      }
      return false;
    }

    public static bool IsTextType(string typename)
    {
#if NETSTANDARD1_3
            string lowerType = typename.ToLower();
#else
             string lowerType = typename.ToLower(CultureInfo.InvariantCulture);
#endif
            switch (lowerType)
      {
        case "varchar":
        case "char":
        case "text":
        case "longtext":
        case "tinytext":
        case "mediumtext":
        case "nchar":
        case "nvarchar":
        case "enum":
        case "set":
          return true;
      }
      return false;
    }

    public static bool SupportScale(string typename)
    {
#if NETSTANDARD1_3
            string lowerType = typename.ToLower();
#else
             string lowerType = typename.ToLower(CultureInfo.InvariantCulture);
#endif
            switch (lowerType)
      {
        case "numeric":
        case "decimal":
        case "dec":
        case "real": return true;
      }
      return false;
    }

    public static MyCatDbType NameToType(string typeName, bool unsigned,
       bool realAsFloat, MyCatConnection connection)
    {
      switch (StringUtility.ToUpperInvariant(typeName))
      {
        case "CHAR": return MyCatDbType.String;
        case "VARCHAR": return MyCatDbType.VarChar;
        case "DATE": return MyCatDbType.Date;
        case "DATETIME": return MyCatDbType.DateTime;
        case "NUMERIC":
        case "DECIMAL":
        case "DEC":
        case "FIXED":
          if (connection.driver.Version.isAtLeast(5, 0, 3))
            return MyCatDbType.NewDecimal;
          else
            return MyCatDbType.Decimal;
        case "YEAR":
          return MyCatDbType.Year;
        case "TIME":
          return MyCatDbType.Time;
        case "TIMESTAMP":
          return MyCatDbType.Timestamp;
        case "SET": return MyCatDbType.Set;
        case "ENUM": return MyCatDbType.Enum;
        case "BIT": return MyCatDbType.Bit;

        case "TINYINT":
          return unsigned ? MyCatDbType.UByte : MyCatDbType.Byte;
        case "BOOL":
        case "BOOLEAN":
          return MyCatDbType.Byte;
        case "SMALLINT":
          return unsigned ? MyCatDbType.UInt16 : MyCatDbType.Int16;
        case "MEDIUMINT":
          return unsigned ? MyCatDbType.UInt24 : MyCatDbType.Int24;
        case "INT":
        case "INTEGER":
          return unsigned ? MyCatDbType.UInt32 : MyCatDbType.Int32;
        case "SERIAL":
          return MyCatDbType.UInt64;
        case "BIGINT":
          return unsigned ? MyCatDbType.UInt64 : MyCatDbType.Int64;
        case "FLOAT": return MyCatDbType.Float;
        case "DOUBLE": return MyCatDbType.Double;
        case "REAL": return
           realAsFloat ? MyCatDbType.Float : MyCatDbType.Double;
        case "TEXT":
          return MyCatDbType.Text;
        case "BLOB":
          return MyCatDbType.Blob;
        case "LONGBLOB":
          return MyCatDbType.LongBlob;
        case "LONGTEXT":
          return MyCatDbType.LongText;
        case "MEDIUMBLOB":
          return MyCatDbType.MediumBlob;
        case "MEDIUMTEXT":
          return MyCatDbType.MediumText;
        case "TINYBLOB":
          return MyCatDbType.TinyBlob;
        case "TINYTEXT":
          return MyCatDbType.TinyText;
        case "BINARY":
          return MyCatDbType.Binary;
        case "VARBINARY":
          return MyCatDbType.VarBinary;
      }
      throw new MyCatException("Unhandled type encountered");
    }

  }
}
