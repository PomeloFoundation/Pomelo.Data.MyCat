// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if SUPPORT_REPLICATION
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using Pomelo.Data.MyCat;


namespace Pomelo.Data.MyCat.Replication
{
  /// <summary>
  /// Manager for Replication & Load Balancing features
  /// </summary>
  internal static class ReplicationManager
  {
    private static List<ReplicationServerGroup> groups = new List<ReplicationServerGroup>();
    private static Object thisLock = new Object();
    //private static Dictionary<string, ReplicationServerSelector> selectors = new Dictionary<string, ReplicationServerSelector>();

    static ReplicationManager()
    {
      Groups = groups;

      // load up our selectors
      if (MyCatConfiguration.Settings == null) return;

      foreach (var group in MyCatConfiguration.Settings.Replication.ServerGroups)
      {
        ReplicationServerGroup g = AddGroup(group.Name, group.GroupType, group.RetryTime);
        foreach (var server in group.Servers)
          g.AddServer(server.Name, server.IsMaster, server.ConnectionString);
      }
    }

    /// <summary>
    /// Returns Replication Server Group List
    /// </summary>
    internal static IList<ReplicationServerGroup> Groups { get; private set; }

    /// <summary>
    /// Adds a Default Server Group to the list
    /// </summary>
    /// <param name="name">Group name</param>
    /// <param name="retryTime">Time between reconnections for failed servers</param>
    /// <returns>Replication Server Group added</returns>
    internal static ReplicationServerGroup AddGroup(string name, int retryTime)
    {
      return AddGroup( name, null, retryTime);
    }

    /// <summary>
    /// Adds a Server Group to the list
    /// </summary>
    /// <param name="name">Group name</param>
    /// <param name="groupType">ServerGroup type reference</param>
    /// <param name="retryTime">Time between reconnections for failed servers</param>
    /// <returns>Server Group added</returns>
    internal static ReplicationServerGroup AddGroup(string name, string groupType, int retryTime)
    {
      if (string.IsNullOrEmpty(groupType))
        groupType = "Pomelo.Data.MyCat.Replication.ReplicationRoundRobinServerGroup";
      Type t = Type.GetType(groupType);
      ReplicationServerGroup g = (ReplicationServerGroup)Activator.CreateInstance(t, name, retryTime) as ReplicationServerGroup;
      groups.Add(g);
      return g;
    }

    /// <summary>
    /// Gets the next server from a replication group
    /// </summary>
    /// <param name="groupName">Group name</param>
    /// <param name="isMaster">True if the server to return must be a master</param>
    /// <returns>Replication Server defined by the Load Balancing plugin</returns>
    internal static ReplicationServer GetServer(string groupName, bool isMaster)
    {
      ReplicationServerGroup group = GetGroup(groupName);
      return group.GetServer(isMaster);
    }

    /// <summary>
    /// Gets a Server Group by name
    /// </summary>
    /// <param name="groupName">Group name</param>
    /// <returns>Server Group if found, otherwise throws an MyCatException</returns>
    internal static ReplicationServerGroup GetGroup(string groupName)
    {
      ReplicationServerGroup group = null;
      foreach (ReplicationServerGroup g in groups)
      {
        if (String.Compare(g.Name, groupName, StringComparison.OrdinalIgnoreCase) != 0) continue;
        group = g;
        break;
      }
      if (group == null)
        throw new MyCatException(String.Format(Resources.ReplicationGroupNotFound, groupName));
      return group;
    }

    /// <summary>
    /// Validates if the replication group name exists
    /// </summary>
    /// <param name="groupName">Group name to validate</param>
    /// <returns>True if replication group name is found, otherwise false</returns>
    internal static bool IsReplicationGroup(string groupName)
    {
      foreach (ReplicationServerGroup g in groups)
        if (String.Compare(g.Name, groupName, StringComparison.OrdinalIgnoreCase) == 0) return true;
      return false;
    }

    /// <summary>
    /// Assigns a new server driver to the connection object
    /// </summary>
    /// <param name="groupName">Group name</param>
    /// <param name="master">True if the server connection to assign must be a master</param>
    /// <param name="connection">MyCatConnection object where the new driver will be assigned</param>
    internal static void GetNewConnection(string groupName, bool master, MyCatConnection connection)
    {
      do
      {
        lock (thisLock)
        {
          if (!IsReplicationGroup(groupName)) return;

          ReplicationServerGroup group = GetGroup(groupName);
          ReplicationServer server = group.GetServer(master, connection.Settings);

          if (server == null)
            throw new MyCatException(Properties.Resources.Replication_NoAvailableServer);

          try
          {
            bool isNewServer = false;
            if (connection.driver == null || !connection.driver.IsOpen)
            {
              isNewServer = true;
            }
            else
            { 
              MyCatConnectionStringBuilder msb = new MyCatConnectionStringBuilder(server.ConnectionString);
              if (!msb.Equals(connection.driver.Settings))
              {
                isNewServer = true;
              }
            }
            if (isNewServer)
            {
              Driver driver = Driver.Create(new MyCatConnectionStringBuilder(server.ConnectionString));
              connection.driver = driver;
            }
            return;
          }
          catch (MyCatException ex)
          {
            connection.driver = null;
            server.IsAvailable = false;
            MyCatTrace.LogError(ex.Number, ex.ToString());
            if (ex.Number == 1042)
            {
              // retry to open a failed connection and update its status
              group.HandleFailover(server, ex);
            }
            else
              throw;
          }
        }
      } while (true);
    }
  }
}
#endif
