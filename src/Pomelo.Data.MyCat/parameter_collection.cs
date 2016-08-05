// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.ComponentModel;
using System.Collections.Generic;


namespace Pomelo.Data.MyCat
{
  /// <summary>
  /// Represents a collection of parameters relevant to a <see cref="MyCatCommand"/> as well as their respective mappings to columns in a <see cref="System.Data.DataSet"/>. This class cannot be inherited.
  /// </summary>
  /// <include file='docs/MyCatParameterCollection.xml' path='MyDocs/MyMembers[@name="Class"]/*'/>
  public sealed partial class MyCatParameterCollection
  {
    List<MyCatParameter> items = new List<MyCatParameter>();
    private Dictionary<string,int> indexHashCS;
    private Dictionary<string,int> indexHashCI;
    //turns to true if any parameter is unnamed
    internal bool containsUnnamedParameters;

    internal MyCatParameterCollection(MyCatCommand cmd)
    {
      indexHashCS = new Dictionary<string, int>();
      indexHashCI = new Dictionary<string,int>(StringComparer.CurrentCultureIgnoreCase);
      containsUnnamedParameters = false;
      Clear();
    }

    /// <summary>
    /// Gets the number of MyCatParameter objects in the collection.
    /// </summary>
    public override int Count
    {
      get { return items.Count; }
    }

    #region Public Methods

    /// <summary>
    /// Gets the <see cref="MyCatParameter"/> at the specified index.
    /// </summary>
    /// <overloads>Gets the <see cref="MyCatParameter"/> with a specified attribute.
    /// [C#] In C#, this property is the indexer for the <see cref="MyCatParameterCollection"/> class.
    /// </overloads>
    public new MyCatParameter this[int index]
    {
      get { return InternalGetParameter(index); }
      set { InternalSetParameter(index, value); }
    }

    /// <summary>
    /// Gets the <see cref="MyCatParameter"/> with the specified name.
    /// </summary>
    public new MyCatParameter this[string name]
    {
      get { return InternalGetParameter(name); }
      set { InternalSetParameter(name, value); }
    }

    /// <summary>
    /// Adds the specified <see cref="MyCatParameter"/> object to the <see cref="MyCatParameterCollection"/>.
    /// </summary>
    /// <param name="value">The <see cref="MyCatParameter"/> to add to the collection.</param>
    /// <returns>The newly added <see cref="MyCatParameter"/> object.</returns>
    public MyCatParameter Add(MyCatParameter value)
    {
      return InternalAdd(value, -1);
    }

    /// <summary>
    /// Adds a <see cref="MyCatParameter"/> to the <see cref="MyCatParameterCollection"/> given the specified parameter name and value.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="value">The <see cref="MyCatParameter.Value"/> of the <see cref="MyCatParameter"/> to add to the collection.</param>
    /// <returns>The newly added <see cref="MyCatParameter"/> object.</returns>
    [Obsolete("Add(String parameterName, Object value) has been deprecated.  Use AddWithValue(String parameterName, Object value)")]
    public MyCatParameter Add(string parameterName, object value)
    {
      return Add(new MyCatParameter(parameterName, value));
    }

    public MyCatParameter AddWithValue(string parameterName, object value)
    {
      return Add(new MyCatParameter(parameterName, value));
    }

    /// <summary>
    /// Adds a <see cref="MyCatParameter"/> to the <see cref="MyCatParameterCollection"/> given the parameter name and the data type.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="dbType">One of the <see cref="MyCatDbType"/> values. </param>
    /// <returns>The newly added <see cref="MyCatParameter"/> object.</returns>
    public MyCatParameter Add(string parameterName, MyCatDbType dbType)
    {
      return Add(new MyCatParameter(parameterName, dbType));
    }

    /// <summary>
    /// Adds a <see cref="MyCatParameter"/> to the <see cref="MyCatParameterCollection"/> with the parameter name, the data type, and the column length.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="dbType">One of the <see cref="MyCatDbType"/> values. </param>
    /// <param name="size">The length of the column.</param>
    /// <returns>The newly added <see cref="MyCatParameter"/> object.</returns>
    public MyCatParameter Add(string parameterName, MyCatDbType dbType, int size)
    {
      return Add(new MyCatParameter(parameterName, dbType, size));
    }

    #endregion

    /// <summary>
    /// Removes all items from the collection.
    /// </summary>
    public override void Clear()
    {
      foreach (MyCatParameter p in items)
        p.Collection = null;
      items.Clear();
      indexHashCS.Clear();
      indexHashCI.Clear();
    }

    void CheckIndex(int index)
    {
      if (index < 0 || index >= Count)
        throw new IndexOutOfRangeException("Parameter index is out of range.");
    }

    private MyCatParameter InternalGetParameter(int index)
    {
      CheckIndex(index);
      return items[index];
    }

    private MyCatParameter InternalGetParameter(string parameterName)
    {
      int index = IndexOf(parameterName);
      if (index < 0)
      {
        // check to see if the user has added the parameter without a
        // parameter marker.  If so, kindly tell them what they did.
        if (parameterName.StartsWith("@", StringComparison.Ordinal) ||
                    parameterName.StartsWith("?", StringComparison.Ordinal))
        {
          string newParameterName = parameterName.Substring(1);
          index = IndexOf(newParameterName);
          if (index != -1)
            return items[index];
        }
        throw new ArgumentException("Parameter '" + parameterName + "' not found in the collection.");
      }
      return items[index];
    }

    private void InternalSetParameter(string parameterName, MyCatParameter value)
    {
      int index = IndexOf(parameterName);
      if (index < 0)
        throw new ArgumentException("Parameter '" + parameterName + "' not found in the collection.");
      InternalSetParameter(index, value);
    }

    private void InternalSetParameter(int index, MyCatParameter value)
    {
      MyCatParameter newParameter = value as MyCatParameter;
      if (newParameter == null)
        throw new ArgumentException(Resources.NewValueShouldBeMyCatParameter);

      CheckIndex(index);
      MyCatParameter p = (MyCatParameter)items[index];

      // first we remove the old parameter from our hashes
      indexHashCS.Remove(p.ParameterName);
      indexHashCI.Remove(p.ParameterName);

      // then we add in the new parameter
      items[index] = newParameter;
      indexHashCS.Add(value.ParameterName, index);
      indexHashCI.Add(value.ParameterName, index);
    }

    /// <summary>
    /// Gets the location of the <see cref="MyCatParameter"/> in the collection with a specific parameter name.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="MyCatParameter"/> object to retrieve. </param>
    /// <returns>The zero-based location of the <see cref="MyCatParameter"/> in the collection.</returns>
    public override int IndexOf(string parameterName)
    {
      int i = -1;
      if (!indexHashCS.TryGetValue(parameterName, out i) &&
        !indexHashCI.TryGetValue(parameterName, out i))
        return -1;
      return i;
    }

    /// <summary>
    /// Gets the location of a <see cref="MyCatParameter"/> in the collection.
    /// </summary>
    /// <param name="value">The <see cref="MyCatParameter"/> object to locate. </param>
    /// <returns>The zero-based location of the <see cref="MyCatParameter"/> in the collection.</returns>
    /// <overloads>Gets the location of a <see cref="MyCatParameter"/> in the collection.</overloads>
    public override int IndexOf(object value)
    {
      MyCatParameter parameter = value as MyCatParameter;
      if (null == parameter)
        throw new ArgumentException("Argument must be of type DbParameter", "value");
      return items.IndexOf(parameter);
    }

    internal void ParameterNameChanged(MyCatParameter p, string oldName, string newName)
    {
      int index = IndexOf(oldName);
      indexHashCS.Remove(oldName);
      indexHashCI.Remove(oldName);

      indexHashCS.Add(newName, index);
      indexHashCI.Add(newName, index);
    }

    private MyCatParameter InternalAdd(MyCatParameter value, int index)
    {
      if (value == null)
        throw new ArgumentException("The MyCatParameterCollection only accepts non-null MyCatParameter type objects.", "value");

      // if the parameter is unnamed, then assign a default name
      if (String.IsNullOrEmpty(value.ParameterName))
        value.ParameterName = String.Format("Parameter{0}", GetNextIndex());

      // make sure we don't already have a parameter with this name
      if (IndexOf(value.ParameterName) >= 0)
      {
        throw new MyCatException(
            String.Format(Resources.ParameterAlreadyDefined, value.ParameterName));
      }
      else
      {
        string inComingName = value.ParameterName;
        if (inComingName[0] == '@' || inComingName[0] == '?')
          inComingName = inComingName.Substring(1, inComingName.Length - 1);
        if (IndexOf(inComingName) >= 0)
          throw new MyCatException(
              String.Format(Resources.ParameterAlreadyDefined, value.ParameterName));
      }

      if (index == -1)
      {
        items.Add(value);
        index = items.Count - 1;
      }
      else
      {
        items.Insert(index, value);
        AdjustHashes(index, true);
      }

      indexHashCS.Add(value.ParameterName, index);
      indexHashCI.Add(value.ParameterName, index);

      value.Collection = this;
      return value;
    }

    private int GetNextIndex()
    {
      int index = Count+1;

      while (true)
      {
        string name = "Parameter" + index.ToString();
        if (!indexHashCI.ContainsKey(name)) break;
        index++;
      }
      return index;
    }

    private static void AdjustHash(Dictionary<string,int> hash, string parameterName, int keyIndex, bool addEntry)
    {
      if (!hash.ContainsKey(parameterName)) return;
      int index = (int)hash[parameterName];
      if (index < keyIndex) return;
      hash[parameterName] = addEntry ? ++index : --index;
    }

    /// <summary>
    /// This method will update all the items in the index hashes when
    /// we insert a parameter somewhere in the middle
    /// </summary>
    /// <param name="keyIndex"></param>
    /// <param name="addEntry"></param>
    private void AdjustHashes(int keyIndex, bool addEntry)
    {
      for (int i = 0; i < Count; i++)
      {
        string name = (items[i] as MyCatParameter).ParameterName;
        AdjustHash(indexHashCI, name, keyIndex, addEntry);
        AdjustHash(indexHashCS, name, keyIndex, addEntry);
      }
    }

    private MyCatParameter GetParameterFlexibleInternal(string baseName)
    {
      int index = IndexOf(baseName);
      if (-1 == index)
        index = IndexOf("?" + baseName);
      if (-1 == index)
        index = IndexOf("@" + baseName);
      if (-1 != index)
        return this[index];
      return null;
    }

    internal MyCatParameter GetParameterFlexible(string parameterName, bool throwOnNotFound)
    {
      string baseName = parameterName;
      MyCatParameter p = GetParameterFlexibleInternal(baseName);
      if (p != null) return p;

      if (parameterName.StartsWith("@", StringComparison.Ordinal) || parameterName.StartsWith("?", StringComparison.Ordinal))
        baseName = parameterName.Substring(1);
      p = GetParameterFlexibleInternal(baseName);
      if (p != null) return p;

      if (throwOnNotFound)
        throw new ArgumentException("Parameter '" + parameterName + "' not found in the collection.");
      return null;
    }
  }
}
