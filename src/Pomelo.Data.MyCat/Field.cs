// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System.Text;
using Pomelo.Data.Common;
using Pomelo.Data.Types;
using System.Globalization;
using System.Text.RegularExpressions;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Pomelo.Data.MyCat
{
  internal enum ColumnFlags : int
  {
    NOT_NULL = 1,
    PRIMARY_KEY = 2,
    UNIQUE_KEY = 4,
    MULTIPLE_KEY = 8,
    BLOB = 16,
    UNSIGNED = 32,
    ZERO_FILL = 64,
    BINARY = 128,
    ENUM = 256,
    AUTO_INCREMENT = 512,
    TIMESTAMP = 1024,
    SET = 2048,
    NUMBER = 32768
  } ;

  /// <summary>
  /// Summary description for Field.
  /// </summary>
  internal class MyCatField
  {
    #region Fields

    // public fields
    public string CatalogName;
    public int ColumnLength;
    public string ColumnName;
    public string OriginalColumnName;
    public string TableName;
    public string RealTableName;
    public string DatabaseName;
    public Encoding Encoding;
    public int maxLength;

    // protected fields
    protected ColumnFlags colFlags;
    protected int charSetIndex;
    protected byte precision;
    protected byte scale;
    protected MyCatDbType mySqlDbType;
    protected DBVersion connVersion;
    protected Driver driver;
    protected bool binaryOk;
    protected List<Type> typeConversions = new List<Type>();

    #endregion

    public MyCatField(Driver driver)
    {
      this.driver = driver;
      connVersion = driver.Version;
      maxLength = 1;
      binaryOk = true;
    }

    #region Properties

    public int CharacterSetIndex
    {
      get { return charSetIndex; }
      set { charSetIndex = value; SetFieldEncoding(); }
    }

    public MyCatDbType Type
    {
      get { return mySqlDbType; }
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

    public int MaxLength
    {
      get { return maxLength; }
      set { maxLength = value; }
    }

    public ColumnFlags Flags
    {
      get { return colFlags; }
    }

    public bool IsAutoIncrement
    {
      get { return (colFlags & ColumnFlags.AUTO_INCREMENT) > 0; }
    }

    public bool IsNumeric
    {
      get { return (colFlags & ColumnFlags.NUMBER) > 0; }
    }

    public bool AllowsNull
    {
      get { return (colFlags & ColumnFlags.NOT_NULL) == 0; }
    }

    public bool IsUnique
    {
      get { return (colFlags & ColumnFlags.UNIQUE_KEY) > 0; }
    }

    public bool IsPrimaryKey
    {
      get { return (colFlags & ColumnFlags.PRIMARY_KEY) > 0; }
    }

    public bool IsBlob
    {
      get
      {
        return (mySqlDbType >= MyCatDbType.TinyBlob &&
        mySqlDbType <= MyCatDbType.Blob) ||
        (mySqlDbType >= MyCatDbType.TinyText &&
        mySqlDbType <= MyCatDbType.Text) ||
        (colFlags & ColumnFlags.BLOB) > 0;
      }
    }

    public bool IsBinary
    {
      get
      {
        return binaryOk && (CharacterSetIndex == 63);
      }
    }

    public bool IsUnsigned
    {
      get { return (colFlags & ColumnFlags.UNSIGNED) > 0; }
    }

    public bool IsTextField
    {
      get
      {
        return Type == MyCatDbType.VarString || Type == MyCatDbType.VarChar ||
                    Type == MyCatDbType.String || (IsBlob && !IsBinary);
      }
    }

    public int CharacterLength
    {
      get { return ColumnLength / MaxLength; }
    }

    public List<Type> TypeConversions
    {
      get { return typeConversions; }
    }

    #endregion

    public void SetTypeAndFlags(MyCatDbType type, ColumnFlags flags)
    {
      colFlags = flags;
      mySqlDbType = type;

      if (String.IsNullOrEmpty(TableName) && String.IsNullOrEmpty(RealTableName) &&
        IsBinary && driver.Settings.FunctionsReturnString)
      {
        CharacterSetIndex = driver.ConnectionCharSetIndex;
      }

      // if our type is an unsigned number, then we need
      // to bump it up into our unsigned types
      // we're trusting that the server is not going to set the UNSIGNED
      // flag unless we are a number
      if (IsUnsigned)
      {
        switch (type)
        {
          case MyCatDbType.Byte:
            mySqlDbType = MyCatDbType.UByte;
            return;
          case MyCatDbType.Int16:
            mySqlDbType = MyCatDbType.UInt16;
            return;
          case MyCatDbType.Int24:
            mySqlDbType = MyCatDbType.UInt24;
            return;
          case MyCatDbType.Int32:
            mySqlDbType = MyCatDbType.UInt32;
            return;
          case MyCatDbType.Int64:
            mySqlDbType = MyCatDbType.UInt64;
            return;
        }
      }

      if (IsBlob)
      {
        // handle blob to UTF8 conversion if requested.  This is only activated
        // on binary blobs
        if (IsBinary && driver.Settings.TreatBlobsAsUTF8)
        {
          bool convertBlob = false;
          Regex includeRegex = driver.Settings.GetBlobAsUTF8IncludeRegex();
          Regex excludeRegex = driver.Settings.GetBlobAsUTF8ExcludeRegex();
          if (includeRegex != null && includeRegex.IsMatch(ColumnName))
            convertBlob = true;
          else if (includeRegex == null && excludeRegex != null &&
            !excludeRegex.IsMatch(ColumnName))
            convertBlob = true;

          if (convertBlob)
          {
            binaryOk = false;
            Encoding = System.Text.Encoding.GetEncoding("UTF-8");
            charSetIndex = -1;  // lets driver know we are in charge of encoding
            maxLength = 4;
          }
        }

        if (!IsBinary)
        {
          if (type == MyCatDbType.TinyBlob)
            mySqlDbType = MyCatDbType.TinyText;
          else if (type == MyCatDbType.MediumBlob)
            mySqlDbType = MyCatDbType.MediumText;
          else if (type == MyCatDbType.Blob)
            mySqlDbType = MyCatDbType.Text;
          else if (type == MyCatDbType.LongBlob)
            mySqlDbType = MyCatDbType.LongText;
        }
      }

      // now determine if we really should be binary
      if (driver.Settings.RespectBinaryFlags)
        CheckForExceptions();

      if (Type == MyCatDbType.String && CharacterLength == 36 && !driver.Settings.OldGuids)
        mySqlDbType = MyCatDbType.Guid;

      if (!IsBinary) return;

      if (driver.Settings.RespectBinaryFlags)
      {
        if (type == MyCatDbType.String)
          mySqlDbType = MyCatDbType.Binary;
        else if (type == MyCatDbType.VarChar ||
             type == MyCatDbType.VarString)
          mySqlDbType = MyCatDbType.VarBinary;
      }

      if (CharacterSetIndex == 63)
        CharacterSetIndex = driver.ConnectionCharSetIndex;

      if (Type == MyCatDbType.Binary && ColumnLength == 16 && driver.Settings.OldGuids)
        mySqlDbType = MyCatDbType.Guid;
    }

    public void AddTypeConversion(Type t)
    {
      if (TypeConversions.Contains(t)) return;
      TypeConversions.Add(t);
    }

    private void CheckForExceptions()
    {
      string colName = String.Empty;
      if (OriginalColumnName != null)
        colName = StringUtility.ToUpperInvariant(OriginalColumnName);
      if (colName.StartsWith("CHAR(", StringComparison.Ordinal))
        binaryOk = false;
    }

    public IMyCatValue GetValueObject()
    {
      IMyCatValue v = GetIMyCatValue(Type);
      if (v is MyCatByte && ColumnLength == 1 && driver.Settings.TreatTinyAsBoolean)
      {
        MyCatByte b = (MyCatByte)v;
        b.TreatAsBoolean = true;
        v = b;
      }
      else if (v is MyCatGuid)
      {
        MyCatGuid g = (MyCatGuid)v;
        g.OldGuids = driver.Settings.OldGuids;
        v = g;
      }
      return v;
    }

    public static IMyCatValue GetIMyCatValue(MyCatDbType type)
    {
      switch (type)
      {
        case MyCatDbType.Byte:
          return new MyCatByte();
        case MyCatDbType.UByte:
          return new MyCatUByte();
        case MyCatDbType.Int16:
          return new MyCatInt16();
        case MyCatDbType.UInt16:
          return new MyCatUInt16();
        case MyCatDbType.Int24:
        case MyCatDbType.Int32:
        case MyCatDbType.Year:
          return new MyCatInt32(type, true);
        case MyCatDbType.UInt24:
        case MyCatDbType.UInt32:
          return new MyCatUInt32(type, true);
        case MyCatDbType.Bit:
          return new MyCatBit();
        case MyCatDbType.Int64:
          return new MyCatInt64();
        case MyCatDbType.UInt64:
          return new MyCatUInt64();
        case MyCatDbType.Time:
          return new MyCatTimeSpan();
        case MyCatDbType.Date:
        case MyCatDbType.DateTime:
        case MyCatDbType.Newdate:
        case MyCatDbType.Timestamp:
          return new MyCatDateTime(type, true);
        case MyCatDbType.Decimal:
        case MyCatDbType.NewDecimal:
          return new MyCatDecimal();
        case MyCatDbType.Float:
          return new MyCatSingle();
        case MyCatDbType.Double:
          return new MyCatDouble();
        case MyCatDbType.Set:
        case MyCatDbType.Enum:
        case MyCatDbType.String:
        case MyCatDbType.VarString:
        case MyCatDbType.VarChar:
        case MyCatDbType.Text:
        case MyCatDbType.TinyText:
        case MyCatDbType.MediumText:
        case MyCatDbType.LongText:
        case (MyCatDbType)Field_Type.NULL:
          return new MyCatString(type, true);
        case MyCatDbType.Geometry:        
          return new MyCatGeometry(type, true);
        case MyCatDbType.Blob:
        case MyCatDbType.MediumBlob:
        case MyCatDbType.LongBlob:
        case MyCatDbType.TinyBlob:
        case MyCatDbType.Binary:
        case MyCatDbType.VarBinary:
          return new MyCatBinary(type, true);
        case MyCatDbType.Guid:
          return new MyCatGuid();
        case MyCatDbType.JSON:
            return new MyCatJson();
        default:
          throw new MyCatException("Unknown data type");
      }
    }

    private void SetFieldEncoding()
    {
      Dictionary<int,string> charSets = driver.CharacterSets;
      DBVersion version = driver.Version;

      if (charSets == null || charSets.Count == 0 || CharacterSetIndex == -1) return;
      if (!charSets.ContainsKey(CharacterSetIndex) || charSets[CharacterSetIndex] == null) return;

      CharacterSet cs = CharSetMap.GetCharacterSet(version, (string)charSets[CharacterSetIndex]);
      // starting with 6.0.4 utf8 has a maxlen of 4 instead of 3.  The old
      // 3 byte utf8 is utf8mb3
      if (cs.name.ToLower() == "utf-8" &&
        version.Major >= 6)
        MaxLength = 4;
      else
        MaxLength = cs.byteCount;
      Encoding = CharSetMap.GetEncoding(version, (string)charSets[CharacterSetIndex]);
    }
  }
}
