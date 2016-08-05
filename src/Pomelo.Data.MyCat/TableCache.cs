// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.Data;
using System.Collections.Generic;

using System.Diagnostics;
using System.Text;
using System.Globalization;

namespace Pomelo.Data.MyCat
{
  internal class TableCache
  {
    private static BaseTableCache cache;

    static TableCache()
    {
      cache = new BaseTableCache(480 /* 8 hour max by default */);
    }

    public static void AddToCache(string commandText, ResultSet resultSet)
    {
      cache.AddToCache(commandText, resultSet);
    }

    public static ResultSet RetrieveFromCache(string commandText, int cacheAge)
    {
      return (ResultSet)cache.RetrieveFromCache(commandText, cacheAge);
    }

    public static void RemoveFromCache(string commandText)
    {
      cache.RemoveFromCache(commandText);
    }

    public static void DumpCache()
    {
      cache.Dump();
    }
  }

  public class BaseTableCache
  {
    protected int MaxCacheAge;
    private Dictionary<string, CacheEntry> cache = new Dictionary<string, CacheEntry>();

    public BaseTableCache(int maxCacheAge)
    {
      MaxCacheAge = maxCacheAge;
    }

    public virtual void AddToCache(string commandText, object resultSet)
    {
      CleanCache();
      CacheEntry entry = new CacheEntry();
      entry.CacheTime = DateTime.Now;
      entry.CacheElement = resultSet;
      lock (cache)
      {
        if (cache.ContainsKey(commandText)) return;
        cache.Add(commandText, entry);
      }
    }

    public virtual object RetrieveFromCache(string commandText, int cacheAge)
    {
      CleanCache();
      lock (cache)
      {
        if (!cache.ContainsKey(commandText)) return null;
        CacheEntry entry = cache[commandText];
        if (DateTime.Now.Subtract(entry.CacheTime).TotalSeconds > cacheAge) return null;
        return entry.CacheElement;
      }
    }

    public void RemoveFromCache(string commandText)
    {
      lock (cache)
      {
        if (!cache.ContainsKey(commandText)) return;
        cache.Remove(commandText);
      }
    }

    public virtual void Dump()
    {
      lock (cache)
        cache.Clear();
    }

    protected virtual void CleanCache()
    {
      DateTime now = DateTime.Now;
      List<string> keysToRemove = new List<string>();

      lock (cache)
      {
        foreach (string key in cache.Keys)
        {
          TimeSpan diff = now.Subtract(cache[key].CacheTime);
          if (diff.TotalSeconds > MaxCacheAge)
            keysToRemove.Add(key);
        }

        foreach (string key in keysToRemove)
          cache.Remove(key);
      }
    }

    private struct CacheEntry
    {
      public DateTime CacheTime;
      public object CacheElement;
    }
  }

}