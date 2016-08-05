// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Pomelo.Data.MyCat
{
  public class MyCatSchemaCollection 
  {
    private List<SchemaColumn> columns = new List<SchemaColumn>();
    private List<MyCatSchemaRow> rows = new List<MyCatSchemaRow>();
#if NET451
    private DataTable _table = null;
#endif

        public MyCatSchemaCollection()
    {
      Mapping = new Dictionary<string,int>( StringComparer.OrdinalIgnoreCase );
      LogicalMappings = new Dictionary<int, int>();
    }

    public MyCatSchemaCollection(string name) : this()
    {
      Name = name;
    }

#if NET451
    public MyCatSchemaCollection(DataTable dt) : this()
    {
      // cache the original datatable to avoid the overhead of creating again whenever possible.
      _table = dt;
      int i = 0;
      foreach (DataColumn dc in dt.Columns)
      {
        columns.Add(new SchemaColumn() { Name = dc.ColumnName, Type = dc.DataType });
        Mapping.Add(dc.ColumnName, i++);
        LogicalMappings[columns.Count - 1] = columns.Count - 1;
      }

      foreach (DataRow dr in dt.Rows)
      {
        MyCatSchemaRow row = new MyCatSchemaRow(this);
        for (i = 0; i < columns.Count; i++)
        {
          row[i] = dr[i];
        }
        rows.Add(row);
      }
    }
#endif

        internal Dictionary<string, int> Mapping;
    internal Dictionary<int, int> LogicalMappings;
    public string Name { get; set; }
    public IList<SchemaColumn> Columns { get { return columns; } }
    public IList<MyCatSchemaRow> Rows { get { return rows; } }

    internal SchemaColumn AddColumn(string name, Type t)
    {
      SchemaColumn c = new SchemaColumn();
      c.Name = name;
      c.Type = t;
      columns.Add(c);
      Mapping.Add(name, columns.Count-1);
      LogicalMappings[columns.Count - 1] = columns.Count - 1;
      return c;
    }

    internal int ColumnIndex(string name)
    {
      int index = -1;
      for (int i = 0; i < columns.Count; i++)
      {
        SchemaColumn c = columns[i];
        if (String.Compare(c.Name, name, StringComparison.OrdinalIgnoreCase) != 0) continue;
        index = i;
        break;
      }
      return index;
    }

    internal void RemoveColumn(string name)
    {
      int index = ColumnIndex(name);
      if (index == -1)
        throw new InvalidOperationException();
      columns.RemoveAt(index);
      for (int i = index; i < Columns.Count; i++)
        LogicalMappings[i] = LogicalMappings[i] + 1;
    }

    internal bool ContainsColumn(string name)
    {
      return ColumnIndex(name) >= 0;
    }

    internal MyCatSchemaRow AddRow()
    {
      MyCatSchemaRow r = new MyCatSchemaRow(this);
      rows.Add(r);
      return r;
    }

    internal MyCatSchemaRow NewRow()
    {
      MyCatSchemaRow r = new MyCatSchemaRow(this);
      return r;
    }

#if NET451
    internal DataTable AsDataTable()
    {
      if (_table != null) return _table;
      DataTable dt = new DataTable(Name);
      foreach (SchemaColumn col in Columns)
        dt.Columns.Add(col.Name, col.Type);
      foreach (MyCatSchemaRow row in Rows)
      {
        DataRow newRow = dt.NewRow();
        for (int i = 0; i < dt.Columns.Count; i++)
          newRow[i] = row[i] == null ? DBNull.Value : row[i];
        dt.Rows.Add(newRow);
      }
      return dt;
    }
#endif
    }

    public class MyCatSchemaRow
  {
    private Dictionary<int,object> data;

    public MyCatSchemaRow(MyCatSchemaCollection c)
    {
      Collection = c;
      InitMetadata();
    }

    internal void InitMetadata()
    {
      data = new Dictionary<int, object>();
    }

    internal MyCatSchemaCollection Collection { get; private set; }

    internal object this[string s]
    {
      get { return GetValueForName(s); }
      set { SetValueForName(s, value); }
    }

    internal object this[int i]
    {
      get {
        int idx = Collection.LogicalMappings[i];
        if (!data.ContainsKey(idx))
          data[idx] = null;
        return data[ idx ];
      }
      set { data[ Collection.LogicalMappings[ i ] ] = value; }
    }

    private void SetValueForName(string colName, object value)
    {
      int index = Collection.Mapping[colName];
      this[index] = value;
    }

    private object GetValueForName(string colName)
    {
      int index = Collection.Mapping[colName];
      if (!data.ContainsKey(index))
        data[index] = null;
      return this[index];
    }

    internal void CopyRow(MyCatSchemaRow row)
    {
      if (Collection.Columns.Count != row.Collection.Columns.Count)
        throw new InvalidOperationException("column count doesn't match");
      for (int i = 0; i < Collection.Columns.Count; i++)
        row[i] = this[i];
    }
  }

  public class SchemaColumn
  {
    public string Name { get; set; }
    public Type Type { get; set; }
  }
}
