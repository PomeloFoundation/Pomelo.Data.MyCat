// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.


using System;
using System.Data.Common;
using System.Reflection;

namespace Pomelo.Data.MyCat
{
  /// <summary>
  /// DBProviderFactory implementation for MysqlClient.
  /// </summary>
 
  public sealed class MyCatClientFactory : DbProviderFactory, IServiceProvider
  {
    /// <summary>
    /// Gets an instance of the <see cref="MyCatClientFactory"/>. 
    /// This can be used to retrieve strongly typed data objects. 
    /// </summary>
    public static MyCatClientFactory Instance = new MyCatClientFactory();
    private Type dbServicesType;
    //private FieldInfo mySqlDbProviderServicesInstance;


#if NET451
        /// <summary>
        /// Returns a strongly typed <see cref="DbCommandBuilder"/> instance. 
        /// </summary>
        /// <returns>A new strongly typed instance of <b>DbCommandBuilder</b>.</returns>
        public override DbCommandBuilder CreateCommandBuilder()
    {
      return new MyCatCommandBuilder();
    }
        /// <summary>
        /// Returns a strongly typed <see cref="DbDataAdapter"/> instance. 
        /// </summary>
        /// <returns>A new strongly typed instance of <b>DbDataAdapter</b>. </returns>
        public override DbDataAdapter CreateDataAdapter()
        {
            return new MyCatDataAdapter();
        }
#endif 

        /// <summary>
        /// Returns a strongly typed <see cref="DbCommand"/> instance. 
        /// </summary>
        /// <returns>A new strongly typed instance of <b>DbCommand</b>.</returns>
        public override DbCommand CreateCommand()
    {
      return new MyCatCommand();
    }

    /// <summary>
    /// Returns a strongly typed <see cref="DbConnection"/> instance. 
    /// </summary>
    /// <returns>A new strongly typed instance of <b>DbConnection</b>.</returns>
    public override DbConnection CreateConnection()
    {
      return new MyCatConnection();
    }



    /// <summary>
    /// Returns a strongly typed <see cref="DbParameter"/> instance. 
    /// </summary>
    /// <returns>A new strongly typed instance of <b>DbParameter</b>.</returns>
    public override DbParameter CreateParameter()
    {
      return new MyCatParameter();
    }

    /// <summary>
    /// Returns a strongly typed <see cref="DbConnectionStringBuilder"/> instance. 
    /// </summary>
    /// <returns>A new strongly typed instance of <b>DbConnectionStringBuilder</b>.</returns>
    public override DbConnectionStringBuilder CreateConnectionStringBuilder()
    {
      return new MyCatConnectionStringBuilder();
    }

    #region IServiceProvider Members

    /// <summary>
    /// Provide a simple caching layer
    /// </summary>
    private Type DbServicesType
    {
      get
      {
        if (dbServicesType == null)
        {
          // Get the type this way so we don't have to reference System.Data.Entity
          // from our core provider
          dbServicesType = Type.GetType(
              @"System.Data.Common.DbProviderServices, System.Data.Entity, 
                        Version=3.5.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                                                                                false);
        }
        return dbServicesType;
      }
    }

    private FieldInfo MyCatDbProviderServicesInstance
    {
      get
      {
        /*if (mySqlDbProviderServicesInstance == null)
        {
          string fullName = Assembly.GetExecutingAssembly().FullName;
          string assemblyName = fullName.Replace("Pomelo.Data", "Pomelo.Data.Entity");
          string assemblyEf5Name = fullName.Replace("Pomelo.Data", "Pomelo.Data.Entity.EF5");
          fullName = String.Format("Pomelo.Data.MyCat.MyCatProviderServices, {0}", assemblyEf5Name);

          Type providerServicesType = Type.GetType(fullName, false);
          if (providerServicesType == null)
          {
            fullName = String.Format("Pomelo.Data.MyCat.MyCatProviderServices, {0}", assemblyName);
            providerServicesType = Type.GetType(fullName, false);
            if (providerServicesType == null)
              throw new DllNotFoundException(fullName);
          }
          mySqlDbProviderServicesInstance = providerServicesType.GetField("Instance",
              BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance);
        }
        return mySqlDbProviderServicesInstance;*/
          return null;
      }
    }

    public object GetService(Type serviceType)
    {
      // DbProviderServices is the only service we offer up right now
      if (serviceType != DbServicesType) return null;

      if (MyCatDbProviderServicesInstance == null) return null;

      return MyCatDbProviderServicesInstance.GetValue(null);
    }

    #endregion
  }
}

