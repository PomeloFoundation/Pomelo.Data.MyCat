// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if SUPPORT_REPLICATION

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text;

namespace Pomelo.Data.MyCat
{
  /// <summary>
  /// Used to define a Replication configurarion element in configuration file
  /// </summary>
  public sealed class ReplicationConfigurationElement : ConfigurationElement
  {
    [ConfigurationProperty("ServerGroups", IsRequired = true)]
    [ConfigurationCollection(typeof(ReplicationServerGroupConfigurationElement), AddItemName = "Group")]
    public GenericConfigurationElementCollection<ReplicationServerGroupConfigurationElement> ServerGroups
    {
      get { return (GenericConfigurationElementCollection<ReplicationServerGroupConfigurationElement>)this["ServerGroups"]; }
    }
  }

  /// <summary>
  /// Used to define a Replication server group in configuration file
  /// </summary>
  public sealed class ReplicationServerGroupConfigurationElement : ConfigurationElement
  {
    [ConfigurationProperty("name", IsRequired = true)]
    public string Name
    {
      get { return (string)this["name"]; }
      set { this["name"] = value; }
    }

    [ConfigurationProperty("groupType", IsRequired = false)]
    public string GroupType
    {
      get { return (string)this["groupType"]; }
      set { this["groupType"] = value; }
    }

    [ConfigurationProperty("retryTime", IsRequired = false, DefaultValue = 60)]
    public int RetryTime
    {
      get { return (int)this["retryTime"]; }
      set { this["retryTime"] = value; }
    }

    [ConfigurationProperty("Servers")]
    [ConfigurationCollection(typeof(ReplicationServerConfigurationElement), AddItemName = "Server")]
    public GenericConfigurationElementCollection<ReplicationServerConfigurationElement> Servers
    {
      get { return (GenericConfigurationElementCollection<ReplicationServerConfigurationElement>)this["Servers"]; }
    }
  }

  /// <summary>
  /// Defines a Replication server in configuration file
  /// </summary>
  public sealed class ReplicationServerConfigurationElement : ConfigurationElement
  {
    [ConfigurationProperty("name", IsRequired = true)]
    public string Name
    {
      get { return (string)this["name"]; }
      set { this["name"] = value; }
    }

    [ConfigurationProperty("IsMaster", IsRequired = false, DefaultValue = false)]
    public bool IsMaster
    {
      get { return (bool)this["IsMaster"]; }
      set { this["IsMaster"] = value; }
    }

    [ConfigurationProperty("connectionstring", IsRequired = true)]
    public string ConnectionString
    {
      get { return (string)this["connectionstring"]; }
      set { this["connectionstring"] = value; }
    }
  }
}
#endif
