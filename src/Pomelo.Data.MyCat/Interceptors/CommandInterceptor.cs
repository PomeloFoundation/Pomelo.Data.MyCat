// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;

namespace Pomelo.Data.MyCat
{
  /// <summary>
  /// BaseCommandInterceptor is the base class that should be used for all userland 
  /// command interceptors
  /// </summary>
  public abstract class BaseCommandInterceptor
  {
    protected MyCatConnection ActiveConnection { get; private set; }

    public virtual bool ExecuteScalar(string sql, ref object returnValue)
    {
      return false;
    }

    public virtual bool ExecuteNonQuery(string sql, ref int returnValue)
    {
      return false;
    }

    public virtual bool ExecuteReader(string sql, CommandBehavior behavior, ref MyCatDataReader returnValue)
    {
      return false;
    }

    public virtual void Init(MyCatConnection connection)
    {
      ActiveConnection = connection;
    }
  }

  /// <summary>
  /// CommandInterceptor is the "manager" class that keeps the list of registered interceptors
  /// for the given connection.
  /// </summary>
  internal sealed class CommandInterceptor : Interceptor
  {
    bool insideInterceptor = false;
    List<BaseCommandInterceptor> interceptors = new List<BaseCommandInterceptor>();

    public CommandInterceptor(MyCatConnection connection)
    {
      this.connection = connection;

      LoadInterceptors(connection.Settings.CommandInterceptors);
    }

    public bool ExecuteScalar(string sql, ref object returnValue)
    {
      if (insideInterceptor) return false;
      insideInterceptor = true;

      bool handled = false;

      foreach (BaseCommandInterceptor bci in interceptors)
        handled |= bci.ExecuteScalar(sql, ref returnValue);

      insideInterceptor = false;
      return handled;
    }

    public bool ExecuteNonQuery(string sql, ref int returnValue)
    {
      if (insideInterceptor) return false;
      insideInterceptor = true;

      bool handled = false;

      foreach (BaseCommandInterceptor bci in interceptors)
        handled |= bci.ExecuteNonQuery(sql, ref returnValue);

      insideInterceptor = false;
      return handled;
    }

    public bool ExecuteReader(string sql, CommandBehavior behavior, ref MyCatDataReader returnValue)
    {
      if (insideInterceptor) return false;
      insideInterceptor = true;

      bool handled = false;

      foreach (BaseCommandInterceptor bci in interceptors)
        handled |= bci.ExecuteReader(sql, behavior, ref returnValue);

      insideInterceptor = false;
      return handled;
    }

    protected override void AddInterceptor(object o)
    {
      if (o == null)
        throw new ArgumentException(String.Format("Unable to instantiate CommandInterceptor"));

      if (!(o is BaseCommandInterceptor))
        throw new InvalidOperationException(String.Format(Resources.TypeIsNotCommandInterceptor,
          o.GetType()));
      BaseCommandInterceptor ie = o as BaseCommandInterceptor;
      ie.Init(connection);
      interceptors.Insert(0, (BaseCommandInterceptor)o);
    }

    protected override string ResolveType(string nameOrType)
    {
#if !NETSTANDARD1_3

            if (MyCatConfiguration.Settings != null && MyCatConfiguration.Settings.CommandInterceptors != null)
      {
        foreach (InterceptorConfigurationElement e in MyCatConfiguration.Settings.CommandInterceptors)
          if (String.Compare(e.Name, nameOrType, true) == 0)
            return e.Type;
      }
#endif
            return base.ResolveType(nameOrType);
    }

  }

}
