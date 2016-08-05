// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;

namespace Pomelo.Data.Common
{
  internal class Cache<KeyType, ValueType>
  {
    private int _capacity;
    private Queue<KeyType> _keyQ;
    private Dictionary<KeyType, ValueType> _contents;

    public Cache(int initialCapacity, int capacity)
    {
      _capacity = capacity;
      _contents = new Dictionary<KeyType, ValueType>(initialCapacity);

      if (capacity > 0)
        _keyQ = new Queue<KeyType>(initialCapacity);
    }

    public ValueType this[KeyType key]
    {
      get
      {
        ValueType val;
        if (_contents.TryGetValue(key, out val))
          return val;
        else
          return default(ValueType);
      }
      set { InternalAdd(key, value); }
    }

    public void Add(KeyType key, ValueType value)
    {
      InternalAdd(key, value);
    }

    private void InternalAdd(KeyType key, ValueType value)
    {
      if (!_contents.ContainsKey(key))
      {

        if (_capacity > 0)
        {
          _keyQ.Enqueue(key);

          if (_keyQ.Count > _capacity)
            _contents.Remove(_keyQ.Dequeue());
        }
      }

      _contents[key] = value;
    }
  }
}
