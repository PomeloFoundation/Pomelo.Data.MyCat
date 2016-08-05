// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;

namespace Pomelo.Data.MyCat
{
  /// <summary>
  /// BaseExceptionInterceptor is the base class that should be used for all userland 
  /// exception interceptors
  /// </summary>
  public abstract class BaseExceptionInterceptor
  {
    public abstract Exception InterceptException(Exception exception);

    protected MyCatConnection ActiveConnection { get; private set; }

    public virtual void Init(MyCatConnection connection)
    {
      ActiveConnection = connection;
    }
  }

  /// <summary>
  /// StandardExceptionInterceptor is the standard interceptor that simply throws the exception.
  /// It is the default action.
  /// </summary>
  internal sealed class StandardExceptionInterceptor : BaseExceptionInterceptor
  {
    public override Exception InterceptException(Exception exception)
    {
      return exception;
    }
  }

  /// <summary>
  /// ExceptionInterceptor is the "manager" class that keeps the list of registered interceptors
  /// for the given connection.
  /// </summary>
  internal sealed class ExceptionInterceptor : Interceptor
  {
    List<BaseExceptionInterceptor> interceptors = new List<BaseExceptionInterceptor>();

    public ExceptionInterceptor(MyCatConnection connection) 
    {
      this.connection = connection;

      LoadInterceptors(connection.Settings.ExceptionInterceptors);

      // we always have the standard interceptor
      interceptors.Add(new StandardExceptionInterceptor());

    }

    protected override void AddInterceptor(object o)
    {
      if (o == null)
        throw new ArgumentException(String.Format("Unable to instantiate ExceptionInterceptor"));

      if (!(o is BaseExceptionInterceptor))
        throw new InvalidOperationException(String.Format(Resources.TypeIsNotExceptionInterceptor,
          o.GetType()));
      BaseExceptionInterceptor ie = o as BaseExceptionInterceptor;
      ie.Init(connection);
      interceptors.Insert(0, (BaseExceptionInterceptor)o);
    }

    public void Throw(Exception exception)
    {
      Exception e = exception;
      foreach (BaseExceptionInterceptor ie in interceptors)
      {
        e = ie.InterceptException(e);
      }
      throw e;
    }

    protected override string ResolveType(string nameOrType)
    {
#if !NETSTANDARD1_3

            if (MyCatConfiguration.Settings != null && MyCatConfiguration.Settings.ExceptionInterceptors != null)
      {
        foreach (InterceptorConfigurationElement e in MyCatConfiguration.Settings.ExceptionInterceptors)
          if (String.Compare(e.Name, nameOrType, true) == 0)
            return e.Type;
      }
#endif
      return base.ResolveType(nameOrType);
    }
  }
}
