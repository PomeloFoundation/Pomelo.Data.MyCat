// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;

namespace Pomelo.Data.MyCat
{
  /// <summary>
  /// Interceptor is the base class for the "manager" classes such as ExceptionInterceptor,
  /// CommandInterceptor, etc
  /// </summary>
  internal abstract class Interceptor
  {
    protected MyCatConnection connection;

    protected void LoadInterceptors(string interceptorList)
    {
      if (String.IsNullOrEmpty(interceptorList)) return;

      string[] interceptors = interceptorList.Split('|');
      foreach (string interceptorType in interceptors)
      {
        if (String.IsNullOrEmpty(interceptorType)) continue;

        string type = ResolveType(interceptorType);
        Type t = Type.GetType(type);
        object interceptorObject = Activator.CreateInstance(t);
        AddInterceptor(interceptorObject);
      }
    }

    protected abstract void AddInterceptor(object o);

    protected virtual string ResolveType(string nameOrType)
    {
      return nameOrType;
    }
  }
}
