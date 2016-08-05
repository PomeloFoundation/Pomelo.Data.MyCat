// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using Pomelo.Data.Types;
using System.ComponentModel;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Collections;
using System.Data;
using System.Data.Common;

namespace Pomelo.Data.MyCat
{
    /// <summary>
    /// Represents a parameter to a <see cref="MyCatCommand"/>, and optionally, its mapping to <see cref="DataSet"/> columns. This class cannot be inherited.
    /// </summary>
    public sealed partial class MyCatParameter : ICloneable
    {
        private const int UNSIGNED_MASK = 0x8000;
        private object paramValue;
        private string paramName;
        private MyCatDbType mySqlDbType;
        private bool inferType = true;
        private const int GEOMETRY_LENGTH = 25;

        #region Constructors

        public MyCatParameter()
        {
            Init();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MyCatParameter"/> class with the parameter name and a value of the new MyCatParameter.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to map. </param>
        /// <param name="value">An <see cref="Object"/> that is the value of the <see cref="MyCatParameter"/>. </param>
        public MyCatParameter(string parameterName, object value) : this()
        {
            ParameterName = parameterName;
            Value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MyCatParameter"/> class with the parameter name and the data type.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to map. </param>
        /// <param name="dbType">One of the <see cref="MyCatDbType"/> values. </param>
        public MyCatParameter(string parameterName, MyCatDbType dbType) : this(parameterName, null)
        {
            MyCatDbType = dbType;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MyCatParameter"/> class with the parameter name, the <see cref="MyCatDbType"/>, and the size.
        /// </summary>
        /// <param name="parameterName">The name of the parameter to map. </param>
        /// <param name="dbType">One of the <see cref="MyCatDbType"/> values. </param>
        /// <param name="size">The length of the parameter. </param>
        public MyCatParameter(string parameterName, MyCatDbType dbType, int size) : this(parameterName, dbType)
        {
            Size = size;
        }

        partial void Init();

        #endregion

        #region Properties

        [Category("Misc")]
        public override String ParameterName
        {
            get { return paramName; }
            set { SetParameterName(value); }
        }

        internal MyCatParameterCollection Collection { get; set; }
        internal Encoding Encoding { get; set; }

        internal bool TypeHasBeenSet
        {
            get { return inferType == false; }
        }


        internal string BaseName
        {
            get
            {
                if (ParameterName.StartsWith("@", StringComparison.Ordinal) || ParameterName.StartsWith("?", StringComparison.Ordinal))
                    return ParameterName.Substring(1);
                return ParameterName;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.
        /// As of MyCat version 4.1 and earlier, input-only is the only valid choice.
        /// </summary>
        [Category("Data")]
        public override ParameterDirection Direction { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the parameter accepts null values.
        /// </summary>
        [Browsable(false)]
        public override Boolean IsNullable { get; set; }

        /// <summary>
        /// Gets or sets the MyCatDbType of the parameter.
        /// </summary>
        [Category("Data")]
#if !NETSTANDARD1_3
        [System.Data.Common.DbProviderSpecificTypeProperty(true)]
#endif
        public MyCatDbType MyCatDbType
        {
            get { return mySqlDbType; }
            set
            {
                SetMyCatDbType(value);
                inferType = false;
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of digits used to represent the <see cref="Value"/> property.
        /// </summary>
        [Category("Data")]
        public override byte Precision { get; set; }

        /// <summary>
        /// Gets or sets the number of decimal places to which <see cref="Value"/> is resolved.
        /// </summary>
        [Category("Data")]
        public override byte Scale { get; set; }


        /// <summary>
        /// Gets or sets the maximum size, in bytes, of the data within the column.
        /// </summary>
        [Category("Data")]
        public override int Size { get; set; }


        /// <summary>
        /// Gets or sets the value of the parameter.
        /// </summary>
        [TypeConverter(typeof(StringConverter))]
        [Category("Data")]
        public override object Value
        {
            get { return paramValue; }
            set
            {
                paramValue = value;
                byte[] valueAsByte = value as byte[];
                string valueAsString = value as string;

                if (valueAsByte != null)
                    Size = valueAsByte.Length;
                else if (valueAsString != null)
                    Size = valueAsString.Length;
                if (inferType)
                    SetTypeFromValue();
            }
        }

        private IMyCatValue _valueObject;
        internal IMyCatValue ValueObject
        {
            get { return _valueObject; }
            private set
            {
                _valueObject = value;
            }
        }

        /// <summary>
        /// Returns the possible values for this parameter if this parameter is of type
        /// SET or ENUM.  Returns null otherwise.
        /// </summary>
        public IList PossibleValues { get; internal set; }

        #endregion

        private void SetParameterName(string name)
        {
            if (Collection != null)
                Collection.ParameterNameChanged(this, paramName, name);
            paramName = name;
        }

        /// <summary>
        /// Overridden. Gets a string containing the <see cref="ParameterName"/>.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return paramName;
        }

        internal int GetPSType()
        {
            switch (mySqlDbType)
            {
                case MyCatDbType.Bit:
                    return (int)MyCatDbType.Int64 | UNSIGNED_MASK;
                case MyCatDbType.UByte:
                    return (int)MyCatDbType.Byte | UNSIGNED_MASK;
                case MyCatDbType.UInt64:
                    return (int)MyCatDbType.Int64 | UNSIGNED_MASK;
                case MyCatDbType.UInt32:
                    return (int)MyCatDbType.Int32 | UNSIGNED_MASK;
                case MyCatDbType.UInt24:
                    return (int)MyCatDbType.Int32 | UNSIGNED_MASK;
                case MyCatDbType.UInt16:
                    return (int)MyCatDbType.Int16 | UNSIGNED_MASK;
                default:
                    return (int)mySqlDbType;
            }
        }

        internal void Serialize(MyCatPacket packet, bool binary, MyCatConnectionStringBuilder settings)
        {
            if (!binary && (paramValue == null || paramValue == DBNull.Value))
                packet.WriteStringNoNull("NULL");
            else
            {
                if (ValueObject.MyCatDbType == MyCatDbType.Guid)
                {
                    MyCatGuid g = (MyCatGuid)ValueObject;
                    g.OldGuids = settings.OldGuids;
                    ValueObject = g;
                }
                if (ValueObject.MyCatDbType == MyCatDbType.Geometry)
                {
                    MyCatGeometry v = (MyCatGeometry)ValueObject;
                    if (v.IsNull && Value != null)
                    {
                        MyCatGeometry.TryParse(Value.ToString(), out v);
                    }
                    ValueObject = v;
                }
                ValueObject.WriteValue(packet, binary, paramValue, Size);
            }
        }

        partial void SetDbTypeFromMyCatDbType();

        private void SetMyCatDbType(MyCatDbType mysql_dbtype)
        {
            mySqlDbType = mysql_dbtype;
            ValueObject = MyCatField.GetIMyCatValue(mySqlDbType);
            SetDbTypeFromMyCatDbType();
        }

        private void SetTypeFromValue()
        {
            if (paramValue == null || paramValue == DBNull.Value) return;

            if (paramValue is Guid)
                MyCatDbType = MyCatDbType.Guid;
            else if (paramValue is TimeSpan)
                MyCatDbType = MyCatDbType.Time;
            else if (paramValue is bool)
                MyCatDbType = MyCatDbType.Byte;
            else
            {
                Type t = paramValue.GetType();
                switch (t.Name)
                {
                    case "SByte": MyCatDbType = MyCatDbType.Byte; break;
                    case "Byte": MyCatDbType = MyCatDbType.UByte; break;
                    case "Int16": MyCatDbType = MyCatDbType.Int16; break;
                    case "UInt16": MyCatDbType = MyCatDbType.UInt16; break;
                    case "Int32": MyCatDbType = MyCatDbType.Int32; break;
                    case "UInt32": MyCatDbType = MyCatDbType.UInt32; break;
                    case "Int64": MyCatDbType = MyCatDbType.Int64; break;
                    case "UInt64": MyCatDbType = MyCatDbType.UInt64; break;
                    case "DateTime": MyCatDbType = MyCatDbType.DateTime; break;
                    case "String": MyCatDbType = MyCatDbType.VarChar; break;
                    case "Single": MyCatDbType = MyCatDbType.Float; break;
                    case "Double": MyCatDbType = MyCatDbType.Double; break;

                    case "Decimal": MyCatDbType = MyCatDbType.Decimal; break;
                    case "Object":
                    default:
#if NETSTANDARD1_3
            if (t.GetTypeInfo().BaseType == typeof(Enum))
#else
                        if (t.BaseType == typeof(Enum))
#endif
                            MyCatDbType = MyCatDbType.Int32;
                        else
                            MyCatDbType = MyCatDbType.Blob;
                        break;
                }
            }
        }

        #region ICloneable

        public MyCatParameter Clone()
        {
#if NETSTANDARD1_3
        MyCatParameter clone = new MyCatParameter(paramName, mySqlDbType);
#else
            MyCatParameter clone = new MyCatParameter(paramName, mySqlDbType, Direction, SourceColumn, SourceVersion, paramValue);
#endif
            // if we have not had our type set yet then our clone should not either
            clone.inferType = inferType;
            return clone;
        }

        object ICloneable.Clone()
        {
            return this.Clone();
        }

        #endregion

        // this method is pretty dumb but we want it to be fast.  it doesn't return size based
        // on value and type but just on the value.
        internal long EstimatedSize()
        {
            if (Value == null || Value == DBNull.Value)
                return 4; // size of NULL
            if (Value is byte[])
                return (Value as byte[]).Length;
            if (Value is string)
                return (Value as string).Length * 4; // account for UTF-8 (yeah I know)
            if (Value is decimal || Value is float)
                return 64;
            return 32;
        }

    }

}
