// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.ComponentModel;
#if !NETSTANDARD1_3
using System.ComponentModel.Design.Serialization;
#endif
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using ParameterDirection = System.Data.ParameterDirection;

namespace Pomelo.Data.MyCat
{
#if !NETSTANDARD1_3
  [TypeConverter(typeof(MyCatParameterConverter))]
#endif

#if NET451
  public sealed partial class MyCatParameter : DbParameter, IDbDataParameter
#else
    public sealed partial class MyCatParameter : DbParameter
#endif
    {
    private DbType dbType;

    /// <summary>
    /// Initializes a new instance of the <see cref="MyCatParameter"/> class with the parameter name, the <see cref="MyCatDbType"/>, the size, and the source column name.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to map. </param>
    /// <param name="dbType">One of the <see cref="MyCatDbType"/> values. </param>
    /// <param name="size">The length of the parameter. </param>
    /// <param name="sourceColumn">The name of the source column. </param>
    public MyCatParameter(string parameterName, MyCatDbType dbType, int size, string sourceColumn) : this(parameterName, dbType)
    {
      Size = size;
      Direction = ParameterDirection.Input;
      SourceColumn = sourceColumn;
#if NET451
      SourceVersion = DataRowVersion.Current;
#endif
        }


#if NET451

    /// <summary>
    /// Initializes a new instance of the <see cref="MyCatParameter"/> class with the parameter name, the type of the parameter, the size of the parameter, a <see cref="ParameterDirection"/>, the precision of the parameter, the scale of the parameter, the source column, a <see cref="DataRowVersion"/> to use, and the value of the parameter.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to map. </param>
    /// <param name="dbType">One of the <see cref="MyCatDbType"/> values. </param>
    /// <param name="size">The length of the parameter. </param>
    /// <param name="direction">One of the <see cref="ParameterDirection"/> values. </param>
    /// <param name="isNullable">true if the value of the field can be null, otherwise false. </param>
    /// <param name="precision">The total number of digits to the left and right of the decimal point to which <see cref="MyCatParameter.Value"/> is resolved.</param>
    /// <param name="scale">The total number of decimal places to which <see cref="MyCatParameter.Value"/> is resolved. </param>
    /// <param name="sourceColumn">The name of the source column. </param>
    /// <param name="sourceVersion">One of the <see cref="DataRowVersion"/> values. </param>
    /// <param name="value">An <see cref="Object"/> that is the value of the <see cref="MyCatParameter"/>. </param>
    /// <exception cref="ArgumentException"/>
    public MyCatParameter(string parameterName, MyCatDbType dbType, int size, ParameterDirection direction,
                          bool isNullable, byte precision, byte scale, string sourceColumn,
                          DataRowVersion sourceVersion,
                          object value)
      : this(parameterName, dbType, size, sourceColumn)
    {
      Direction = direction;
      SourceVersion = sourceVersion;
      IsNullable = isNullable;
      Precision = precision;
      Scale = scale;
      Value = value;
    }

    internal MyCatParameter(string name, MyCatDbType type, ParameterDirection dir, string col, DataRowVersion ver, object val)
      : this(name, type)
    {
      Direction = dir;
      SourceColumn = col;
      SourceVersion = ver;
      Value = val;
    }

    partial void Init()
    {

      SourceVersion = DataRowVersion.Current;

        Direction = ParameterDirection.Input;
    }

    /// <summary>
    /// Gets or sets the <see cref="DataRowVersion"/> to use when loading <see cref="Value"/>.
    /// </summary>
    [Category("Data")]
    public override DataRowVersion SourceVersion { get; set; }
#endif
            /// <summary>
            /// Gets or sets the name of the source column that is mapped to the <see cref="DataSet"/> and used for loading or returning the <see cref="Value"/>.
            /// </summary>
        [Category("Data")]
    public override String SourceColumn { get; set; }

    /// <summary>
    /// Resets the <b>DbType</b> property to its original settings. 
    /// </summary>
    public override void ResetDbType()
    {
      inferType = true;
    }

    /// <summary>
    /// Sets or gets a value which indicates whether the source column is nullable. 
    /// This allows <see cref="DbCommandBuilder"/> to correctly generate Update statements 
    /// for nullable columns. 
    /// </summary>
    public override bool SourceColumnNullMapping { get; set; }

    /// <summary>
    /// Gets or sets the <see cref="DbType"/> of the parameter.
    /// </summary>
    public override DbType DbType
    {
      get { return dbType; }
      set
      {
        SetDbType(value);
        inferType = false;
      }
    }

    partial void SetDbTypeFromMyCatDbType()
    {
      switch (mySqlDbType)
      {
        case MyCatDbType.NewDecimal:
        case MyCatDbType.Decimal:
          dbType = DbType.Decimal;
          break;
        case MyCatDbType.Byte:
          dbType = DbType.SByte;
          break;
        case MyCatDbType.UByte:
          dbType = DbType.Byte;
          break;
        case MyCatDbType.Int16:
          dbType = DbType.Int16;
          break;
        case MyCatDbType.UInt16:
          dbType = DbType.UInt16;
          break;
        case MyCatDbType.Int24:
        case MyCatDbType.Int32:
          dbType = DbType.Int32;
          break;
        case MyCatDbType.UInt24:
        case MyCatDbType.UInt32:
          dbType = DbType.UInt32;
          break;
        case MyCatDbType.Int64:
          dbType = DbType.Int64;
          break;
        case MyCatDbType.UInt64:
          dbType = DbType.UInt64;
          break;
        case MyCatDbType.Bit:
          dbType = DbType.UInt64;
          break;
        case MyCatDbType.Float:
          dbType = DbType.Single;
          break;
        case MyCatDbType.Double:
          dbType = DbType.Double;
          break;
        case MyCatDbType.Timestamp:
        case MyCatDbType.DateTime:
          dbType = DbType.DateTime;
          break;
        case MyCatDbType.Date:
        case MyCatDbType.Newdate:
        case MyCatDbType.Year:
          dbType = DbType.Date;
          break;
        case MyCatDbType.Time:
          dbType = DbType.Time;
          break;
        case MyCatDbType.Enum:
        case MyCatDbType.Set:
        case MyCatDbType.VarChar:
          dbType = DbType.String;
          break;
        case MyCatDbType.TinyBlob:
        case MyCatDbType.MediumBlob:
        case MyCatDbType.LongBlob:
        case MyCatDbType.Blob:
          dbType = DbType.Object;
          break;
        case MyCatDbType.String:
          dbType = DbType.StringFixedLength;
          break;
        case MyCatDbType.Guid:
          dbType = DbType.Guid;
          break;
      }
    }


    private void SetDbType(DbType db_type)
    {
      dbType = db_type;
      switch (dbType)
      {
        case DbType.Guid:
          mySqlDbType = MyCatDbType.Guid;
          break;

        case DbType.AnsiString:
        case DbType.String:
          mySqlDbType = MyCatDbType.VarChar;
          break;

        case DbType.AnsiStringFixedLength:
        case DbType.StringFixedLength:
          mySqlDbType = MyCatDbType.String;
          break;

        case DbType.Boolean:
        case DbType.Byte:
          mySqlDbType = MyCatDbType.UByte;
          break;

        case DbType.SByte:
          mySqlDbType = MyCatDbType.Byte;
          break;

        case DbType.Date:
          mySqlDbType = MyCatDbType.Date;
          break;
        case DbType.DateTime:
          mySqlDbType = MyCatDbType.DateTime;
          break;

        case DbType.Time:
          mySqlDbType = MyCatDbType.Time;
          break;
        case DbType.Single:
          mySqlDbType = MyCatDbType.Float;
          break;
        case DbType.Double:
          mySqlDbType = MyCatDbType.Double;
          break;

        case DbType.Int16:
          mySqlDbType = MyCatDbType.Int16;
          break;
        case DbType.UInt16:
          mySqlDbType = MyCatDbType.UInt16;
          break;

        case DbType.Int32:
          mySqlDbType = MyCatDbType.Int32;
          break;
        case DbType.UInt32:
          mySqlDbType = MyCatDbType.UInt32;
          break;

        case DbType.Int64:
          mySqlDbType = MyCatDbType.Int64;
          break;
        case DbType.UInt64:
          mySqlDbType = MyCatDbType.UInt64;
          break;

        case DbType.Decimal:
        case DbType.Currency:
          mySqlDbType = MyCatDbType.Decimal;
          break;

        case DbType.Object:
        case DbType.VarNumeric:
        case DbType.Binary:
        default:
          mySqlDbType = MyCatDbType.Blob;
          break;
      }

      if (dbType == DbType.Object)
      {
        var value = this.paramValue as byte[];
        if (value != null && value.Length == GEOMETRY_LENGTH)
          mySqlDbType = MyCatDbType.Geometry;
      }

      ValueObject = MyCatField.GetIMyCatValue(mySqlDbType);
    }
  }

#if !NETSTANDARD1_3
  internal class MyCatParameterConverter : TypeConverter
  {

    public override bool CanConvertTo(ITypeDescriptorContext context, Type destinationType)
    {
      if (destinationType == typeof(InstanceDescriptor))
      {
        return true;
      }

      // Always call the base to see if it can perform the conversion.
      return base.CanConvertTo(context, destinationType);
    }

    public override object ConvertTo(ITypeDescriptorContext context,
                                     CultureInfo culture, object value, Type destinationType)
    {
      if (destinationType == typeof(InstanceDescriptor))
      {
        ConstructorInfo ci = typeof(MyCatParameter).GetConstructor(
            new Type[]
                            {
                                typeof (string), typeof (MyCatDbType), typeof (int), typeof (ParameterDirection),
                                typeof (bool), typeof (byte), typeof (byte), typeof (string), typeof (DataRowVersion),
                                typeof (object)
                            });
        MyCatParameter p = (MyCatParameter)value;
        return new InstanceDescriptor(ci, new object[]
                                                          {
                                                              p.ParameterName, p.DbType, p.Size, p.Direction,
                                                              p.IsNullable,
                                                              p.Precision,
                                                              p.Scale,
                                                              p.SourceColumn, p.SourceVersion, p.Value
                                                          });
      }

      // Always call base, even if you can't convert.
      return base.ConvertTo(context, culture, value, destinationType);
    }
  }
#endif

                                                          }
