// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using Pomelo.Data.Types;
using System;
using System.Collections;
using System.Data;
using System.Data.Common;

namespace Pomelo.Data.MyCat
{
    public sealed partial class MyCatDataReader : DbDataReader
    {
        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.  This method is not 
        /// supported currently and always returns 0.
        /// </summary>
        public override int Depth
        {
            get { return 0; }
        }

        public MyCatGeometry GetMyCatGeometry(int i)
        {
            try
            {
                IMyCatValue v = GetFieldValue(i, false);
                if (v is MyCatGeometry || v is MyCatBinary)
                    return new MyCatGeometry(MyCatDbType.Geometry, (Byte[])v.Value);
            }
            catch
            {
                Throw(new Exception("Can't get MyCatGeometry from value"));
            }
            return new MyCatGeometry(true);
        }

        public MyCatGeometry GetMyCatGeometry(string column)
        {
            return GetMyCatGeometry(GetOrdinal(column));
        }
#if NET451
    /// <summary>
    /// Returns a DataTable that describes the column metadata of the MyCatDataReader.
    /// </summary>
    /// <returns></returns>
    public override DataTable GetSchemaTable()
    {
      // Only Results from SQL SELECT Queries 
      // get a DataTable for schema of the result
      // otherwise, DataTable is null reference
      if (FieldCount == 0) return null;

      DataTable dataTableSchema = new DataTable("SchemaTable");

      dataTableSchema.Columns.Add("ColumnName", typeof(string));
      dataTableSchema.Columns.Add("ColumnOrdinal", typeof(int));
      dataTableSchema.Columns.Add("ColumnSize", typeof(int));
      dataTableSchema.Columns.Add("NumericPrecision", typeof(int));
      dataTableSchema.Columns.Add("NumericScale", typeof(int));
      dataTableSchema.Columns.Add("IsUnique", typeof(bool));
      dataTableSchema.Columns.Add("IsKey", typeof(bool));
      DataColumn dc = dataTableSchema.Columns["IsKey"];
      dc.AllowDBNull = true; // IsKey can have a DBNull
      dataTableSchema.Columns.Add("BaseCatalogName", typeof(string));
      dataTableSchema.Columns.Add("BaseColumnName", typeof(string));
      dataTableSchema.Columns.Add("BaseSchemaName", typeof(string));
      dataTableSchema.Columns.Add("BaseTableName", typeof(string));
      dataTableSchema.Columns.Add("DataType", typeof(Type));
      dataTableSchema.Columns.Add("AllowDBNull", typeof(bool));
      dataTableSchema.Columns.Add("ProviderType", typeof(int));
      dataTableSchema.Columns.Add("IsAliased", typeof(bool));
      dataTableSchema.Columns.Add("IsExpression", typeof(bool));
      dataTableSchema.Columns.Add("IsIdentity", typeof(bool));
      dataTableSchema.Columns.Add("IsAutoIncrement", typeof(bool));
      dataTableSchema.Columns.Add("IsRowVersion", typeof(bool));
      dataTableSchema.Columns.Add("IsHidden", typeof(bool));
      dataTableSchema.Columns.Add("IsLong", typeof(bool));
      dataTableSchema.Columns.Add("IsReadOnly", typeof(bool));

      int ord = 1;
      for (int i = 0; i < FieldCount; i++)
      {
        MyCatField f = resultSet.Fields[i];
        DataRow r = dataTableSchema.NewRow();
        r["ColumnName"] = f.ColumnName;
        r["ColumnOrdinal"] = ord++;
        r["ColumnSize"] = f.IsTextField ? f.ColumnLength / f.MaxLength : f.ColumnLength;
        int prec = f.Precision;
        int pscale = f.Scale;
        if (prec != -1)
          r["NumericPrecision"] = (short)prec;
        if (pscale != -1)
          r["NumericScale"] = (short)pscale;
        r["DataType"] = GetFieldType(i);
        r["ProviderType"] = (int)f.Type;
        r["IsLong"] = f.IsBlob && f.ColumnLength > 255;
        r["AllowDBNull"] = f.AllowsNull;
        r["IsReadOnly"] = false;
        r["IsRowVersion"] = false;
        r["IsUnique"] = false;
        r["IsKey"] = f.IsPrimaryKey;
        r["IsAutoIncrement"] = f.IsAutoIncrement;
        r["BaseSchemaName"] = f.DatabaseName;
        r["BaseCatalogName"] = null;
        r["BaseTableName"] = f.RealTableName;
        r["BaseColumnName"] = f.OriginalColumnName;

        dataTableSchema.Rows.Add(r);
      }

      return dataTableSchema;
    }
#endif

        /// <summary>
        /// Returns an <see cref="IEnumerator"/> that iterates through the <see cref="MyCatDataReader"/>. 
        /// </summary>
        /// <returns></returns>
        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this, (commandBehavior & CommandBehavior.CloseConnection) != 0);
        }
    }
}
