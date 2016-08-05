// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.ComponentModel;
using System.Data.Common;

namespace Pomelo.Data.MyCat
{
  // [Editor("Pomelo.Data.MyCat.Design.DBParametersEditor,MyCat.Design", typeof(System.Drawing.Design.UITypeEditor))]
  // [ListBindable(true)]
  public sealed partial class MyCatParameterCollection : DbParameterCollection
  {
    /// <summary>
    /// Adds a <see cref="MyCatParameter"/> to the <see cref="MyCatParameterCollection"/> with the parameter name, the data type, the column length, and the source column name.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="dbType">One of the <see cref="MyCatDbType"/> values. </param>
    /// <param name="size">The length of the column.</param>
    /// <param name="sourceColumn">The name of the source column.</param>
    /// <returns>The newly added <see cref="MyCatParameter"/> object.</returns>
    public MyCatParameter Add(string parameterName, MyCatDbType dbType, int size, string sourceColumn)
    {
      return Add(new MyCatParameter(parameterName, dbType, size, sourceColumn));
    }


    #region DbParameterCollection Implementation

    /// <summary>
    /// Adds an array of values to the end of the <see cref="MyCatParameterCollection"/>. 
    /// </summary>
    /// <param name="values"></param>
    public override void AddRange(Array values)
    {
      foreach (DbParameter p in values)
        Add(p);
    }

    /// <summary>
    /// Retrieve the parameter with the given name.
    /// </summary>
    /// <param name="parameterName"></param>
    /// <returns></returns>
    protected override DbParameter GetParameter(string parameterName)
    {
      return InternalGetParameter(parameterName);
    }

    protected override DbParameter GetParameter(int index)
    {
      return InternalGetParameter(index);
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
      InternalSetParameter(parameterName, value as MyCatParameter);
    }

    protected override void SetParameter(int index, DbParameter value)
    {
      InternalSetParameter(index, value as MyCatParameter);
    }

    /// <summary>
    /// Adds the specified <see cref="MyCatParameter"/> object to the <see cref="MyCatParameterCollection"/>.
    /// </summary>
    /// <param name="value">The <see cref="MyCatParameter"/> to add to the collection.</param>
    /// <returns>The index of the new <see cref="MyCatParameter"/> object.</returns>
    public override int Add(object value)
    {
      MyCatParameter parameter = value as MyCatParameter;
      if (parameter == null)
        throw new MyCatException("Only MyCatParameter objects may be stored");

      parameter = Add(parameter);
      return IndexOf(parameter);
    }

    /// <summary>
    /// Gets a value indicating whether a <see cref="MyCatParameter"/> with the specified parameter name exists in the collection.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="MyCatParameter"/> object to find.</param>
    /// <returns>true if the collection contains the parameter; otherwise, false.</returns>
    public override bool Contains(string parameterName)
    {
      return IndexOf(parameterName) != -1;
    }

    /// <summary>
    /// Gets a value indicating whether a MyCatParameter exists in the collection.
    /// </summary>
    /// <param name="value">The value of the <see cref="MyCatParameter"/> object to find. </param>
    /// <returns>true if the collection contains the <see cref="MyCatParameter"/> object; otherwise, false.</returns>
    /// <overloads>Gets a value indicating whether a <see cref="MyCatParameter"/> exists in the collection.</overloads>
    public override bool Contains(object value)
    {
      MyCatParameter parameter = value as MyCatParameter;
      if (null == parameter)
        throw new ArgumentException("Argument must be of type DbParameter", "value");
      return items.Contains(parameter);
    }

    /// <summary>
    /// Copies MyCatParameter objects from the MyCatParameterCollection to the specified array.
    /// </summary>
    /// <param name="array"></param>
    /// <param name="index"></param>
    public override void CopyTo(Array array, int index)
    {
      items.ToArray().CopyTo(array, index);
    }

    /// <summary>
    /// Returns an enumerator that iterates through the <see cref="MyCatParameterCollection"/>. 
    /// </summary>
    /// <returns></returns>
    public override IEnumerator GetEnumerator()
    {
      return items.GetEnumerator();
    }

    /// <summary>
    /// Inserts a MyCatParameter into the collection at the specified index.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="value"></param>
    public override void Insert(int index, object value)
    {
      MyCatParameter parameter = value as MyCatParameter;
      if (parameter == null)
        throw new MyCatException("Only MyCatParameter objects may be stored");
      InternalAdd(parameter, index);
    }
#if NET451
    /// <summary>
    /// Gets a value that indicates whether the <see cref="MyCatParameterCollection"/>
    /// has a fixed size. 
    /// </summary>
    public override bool IsFixedSize
    {
      get { return (items as IList).IsFixedSize; }
    }

    /// <summary>
    /// Gets a value that indicates whether the <see cref="MyCatParameterCollection"/>
    /// is read-only. 
    /// </summary>
    public override bool IsReadOnly
    {
      get { return (items as IList).IsReadOnly; }
    }

    /// <summary>
    /// Gets a value that indicates whether the <see cref="MyCatParameterCollection"/>
    /// is synchronized. 
    /// </summary>
    public override bool IsSynchronized
    {
      get { return (items as IList).IsSynchronized; }
    }
#endif
    /// <summary>
    /// Removes the specified MyCatParameter from the collection.
    /// </summary>
    /// <param name="value"></param>
    public override void Remove(object value)
    {
      MyCatParameter p = (value as MyCatParameter);
      p.Collection = null;
      int index = IndexOf(p);
      items.Remove(p);

      indexHashCS.Remove(p.ParameterName);
      indexHashCI.Remove(p.ParameterName);
      AdjustHashes(index, false);
    }

    /// <summary>
    /// Removes the specified <see cref="MyCatParameter"/> from the collection using the parameter name.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="MyCatParameter"/> object to retrieve. </param>
    public override void RemoveAt(string parameterName)
    {
      DbParameter p = GetParameter(parameterName);
      Remove(p);
    }

    /// <summary>
    /// Removes the specified <see cref="MyCatParameter"/> from the collection using a specific index.
    /// </summary>
    /// <param name="index">The zero-based index of the parameter. </param>
    /// <overloads>Removes the specified <see cref="MyCatParameter"/> from the collection.</overloads>
    public override void RemoveAt(int index)
    {
      object o = items[index];
      Remove(o);
    }

    /// <summary>
    /// Gets an object that can be used to synchronize access to the 
    /// <see cref="MyCatParameterCollection"/>. 
    /// </summary>
    public override object SyncRoot
    {
      get { return (items as IList).SyncRoot; }
    }

#endregion

  }
}
