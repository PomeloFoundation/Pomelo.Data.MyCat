// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.Globalization;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{
  //Bytes structure is:
  //SRID       [0 - 3]
  //Byte order [4]
  //WKB type   [5 - 8]
  //X          [9 - 16]
  //Y          [17 - 24]
  //The byte order may be either 1 or 0 to indicate little-endian or
  //big-endian storage. The little-endian and big-endian byte orders
  //are also known as Network Data Representation (NDR) and External
  //Data Representation (XDR), respectively.

  //The WKB type is a code that indicates the geometry type. Values
  //from 1 through 7 indicate Point, LineString, Polygon, MultiPoint,
  //MultiLineString, MultiPolygon, and GeometryCollection.

  public struct MyCatGeometry : IMyCatValue
  {
    private MyCatDbType _type;
    private Double _xValue; 
    private Double _yValue;
    private int _srid;
    private byte[] _valBinary;
    private bool _isNull;

    private const int GEOMETRY_LENGTH = 25;

    public Double? XCoordinate
    {
      get { return _xValue; }
    }

    public Double? YCoordinate
    {
      get { return _yValue; }
    }

    public int? SRID
    {
      get { return _srid; }
    }

    public MyCatGeometry(bool isNull):this(MyCatDbType.Geometry, isNull)
    {
    }

    public MyCatGeometry(Double xValue, Double yValue)
      : this(MyCatDbType.Geometry, xValue, yValue, 0)
    { }

    public MyCatGeometry(Double xValue, Double yValue, int srid)
      : this(MyCatDbType.Geometry, xValue, yValue, srid)
    { }


    internal MyCatGeometry(MyCatDbType type, bool isNull)
    {
      this._type = type;
      isNull = true;
      _xValue = 0;
      _yValue = 0;
      _srid = 0;
      _valBinary = null;
      this._isNull = isNull;
    }


    internal MyCatGeometry(MyCatDbType type, Double xValue, Double yValue, int srid)
    {
      this._type = type;
      this._xValue = xValue;
      this._yValue = yValue;      
      this._isNull = false;
      this._srid = srid;
      this._valBinary = new byte[GEOMETRY_LENGTH];

      byte[] sridBinary = BitConverter.GetBytes(srid);

      for (int i = 0; i < sridBinary.Length; i++)
        _valBinary[i] = sridBinary[i];

      long xVal = BitConverter.DoubleToInt64Bits(xValue);
      long yVal = BitConverter.DoubleToInt64Bits(yValue);

      _valBinary[4] = 1;
      _valBinary[5] = 1;
     
      for (int i = 0; i < 8; i++)
        {
          _valBinary[i + 9] = (byte)(xVal & 0xff);
          xVal >>= 8;
        }

      for (int i = 0; i < 8; i++)
      {
        _valBinary[i + 17] = (byte)(yVal & 0xff);
        yVal >>= 8;
      }
    }

    public MyCatGeometry(MyCatDbType type, byte[] val)
    {

      if (val == null) 
        throw new ArgumentNullException("val");

      byte[] buffValue = new byte[val.Length];
 
      for (int i = 0; i < val.Length; i++)                  
           buffValue[i] = val[i];

      var xIndex = val.Length == GEOMETRY_LENGTH ? 9 : 5;
      var yIndex = val.Length == GEOMETRY_LENGTH ? 17 : 13;

      _valBinary = buffValue;
      _xValue = BitConverter.ToDouble(val, xIndex);
      _yValue = BitConverter.ToDouble(val, yIndex);
      this._srid = val.Length == GEOMETRY_LENGTH ? BitConverter.ToInt32(val, 0) : 0;
      this._isNull = false;
      this._type = type;
    }

    #region IMyCatValue Members

   
    MyCatDbType IMyCatValue.MyCatDbType
    {
      get { return _type; }
    }

    public bool IsNull
    {
      get { return _isNull; }
    }


    object IMyCatValue.Value
    {
      get { return _valBinary; }
    }

    public byte[] Value
    {
      get { return _valBinary; }
    }

    Type IMyCatValue.SystemType
    {
      get { return typeof(byte[]); }
    }

    string IMyCatValue.MyCatTypeName
    {
      get
      {
         return "GEOMETRY";
      }      
    }

    void IMyCatValue.WriteValue(MyCatPacket packet, bool binary, object val, int length)
    {
      byte[] buffToWrite = null;
     
      try
      {
        buffToWrite = ((MyCatGeometry)val)._valBinary;        
      }
      catch 
      {
        buffToWrite = val as Byte[];
      }

      if (buffToWrite == null)
      {
        MyCatGeometry v = new MyCatGeometry(0, 0);
        MyCatGeometry.TryParse(val.ToString(), out v);
        buffToWrite = v._valBinary;
      }

      byte[] result = new byte[GEOMETRY_LENGTH];
     
      for (int i = 0; i < buffToWrite.Length; i++)
      {
       if (buffToWrite.Length < GEOMETRY_LENGTH)
         result[i + 4] = buffToWrite[i];
       else
        result[i] = buffToWrite[i];
      }
      
        packet.WriteStringNoNull("_binary ");
        packet.WriteByte((byte)'\'');
        EscapeByteArray(result, GEOMETRY_LENGTH, packet);
        packet.WriteByte((byte)'\'');      
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

    IMyCatValue IMyCatValue.ReadValue(MyCatPacket packet, long length, bool nullVal)
    {
      MyCatGeometry g;
      if (nullVal)
        g = new MyCatGeometry(_type, true);
      else
      {
        if (length == -1)
          length = (long)packet.ReadFieldLength();

        byte[] newBuff = new byte[length];
        packet.Read(newBuff, 0, (int)length);
        g = new MyCatGeometry(_type, newBuff);        
      }
      return g;
    }

    void IMyCatValue.SkipValue(MyCatPacket packet)
    {
      int len = (int)packet.ReadFieldLength();
      packet.Position += len;
    }

    #endregion


    /// <summary>Returns the Well-Known Text representation of this value</summary>
    /// POINT({0} {1})", longitude, latitude
    /// http://dev.mysql.com/doc/refman/4.1/en/gis-wkt-format.html
    public override string ToString()
    {
      if (!this._isNull)
        return _srid != 0 ? string.Format(CultureInfo.InvariantCulture.NumberFormat, "SRID={2};POINT({0} {1})", _xValue, _yValue, _srid) : string.Format(CultureInfo.InvariantCulture.NumberFormat, "POINT({0} {1})", _xValue, _yValue);      

      return String.Empty;
    }

    /// <summary>
    /// Get value from WKT format
    /// SRID=0;POINT (x y) or POINT (x y)
    /// </summary>
    /// <param name="value">WKT string format</param>    
    public static MyCatGeometry Parse(string value)
    {
      if (String.IsNullOrEmpty(value))
        throw new ArgumentNullException("value");

      if (!(value.Contains("SRID") || value.Contains("POINT(") || value.Contains("POINT (")))
        throw new FormatException("String does not contain a valid geometry value");

      MyCatGeometry result = new MyCatGeometry(0,0);
      MyCatGeometry.TryParse(value, out result);

      return result;
    }

    /// <summary>
    /// Try to get value from WKT format
    /// SRID=0;POINT (x y) or POINT (x y)
    /// </summary>
    /// <param name="value">WKT string format</param>    
    public static bool TryParse(string value, out MyCatGeometry mySqlGeometryValue)
    {
      string[] arrayResult = new string[0];
      string strResult = string.Empty;
      bool hasX = false;
      bool hasY = false;
      Double xVal = 0;
      Double yVal = 0;
      int sridValue = 0;

      try
      {
        if (value.Contains(";"))
          arrayResult = value.Split(';');
        else
          strResult = value;

        if (arrayResult.Length > 1 || strResult != String.Empty)
        {
          string point = strResult != String.Empty ? strResult : arrayResult[1];
          point = point.Replace("POINT (", "").Replace("POINT(", "").Replace(")", "");
          var coord = point.Split(' ');
          if (coord.Length > 1)
          {
            hasX = Double.TryParse(coord[0], out xVal);
            hasY = Double.TryParse(coord[1], out yVal);
          }
          if (arrayResult.Length >= 1)
            Int32.TryParse(arrayResult[0].Replace("SRID=", ""), out sridValue);
        }
        if (hasX && hasY)
        {
          mySqlGeometryValue = new MyCatGeometry(xVal, yVal, sridValue);
          return true;
        }
      }
      catch
      {  }
      
      mySqlGeometryValue = new MyCatGeometry(true);
      return false; 
    }
    
    public static void SetDSInfo(MyCatSchemaCollection dsTable)
    {
      // we use name indexing because this method will only be called
      // when GetSchema is called for the DataSourceInformation 
      // collection and then it wil be cached.
      MyCatSchemaRow row = dsTable.AddRow();
      row["TypeName"] = "GEOMETRY";
      row["ProviderDbType"] = MyCatDbType.Geometry;
      row["ColumnSize"] = GEOMETRY_LENGTH;
      row["CreateFormat"] = "GEOMETRY";
      row["CreateParameters"] = DBNull.Value; ;
      row["DataType"] = "System.Byte[]";
      row["IsAutoincrementable"] = false;
      row["IsBestMatch"] = true;
      row["IsCaseSensitive"] = false;
      row["IsFixedLength"] = false;
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
      row["LiteralPrefix"] = DBNull.Value;
      row["LiteralSuffix"] = DBNull.Value;
      row["NativeDataType"] = DBNull.Value;
    }

    public string GetWKT()
    {
      if (!this._isNull)
        return string.Format(CultureInfo.InvariantCulture.NumberFormat, "POINT({0} {1})", _xValue, _yValue);

      return String.Empty;
    }
 }
}
