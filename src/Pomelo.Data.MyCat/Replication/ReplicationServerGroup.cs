// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if SUPPORT_REPLICATION
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Pomelo.Data.MyCat.Replication
{
  /// <summary>
  /// Base class used to implement load balancing features
  /// </summary>
  public abstract class ReplicationServerGroup
  {
    protected List<ReplicationServer> servers = new List<ReplicationServer>();

    /// <param name="name">Group name</param>
    /// <param name="retryTime"></param>
    public ReplicationServerGroup(string name, int retryTime)
    {
      Servers = servers;
      Name = name;
      RetryTime = retryTime;
    }

    /// <summary>
    /// Group name
    /// </summary>
    public string Name { get; protected set; }
    /// <summary>
    /// Retry time between connections to failed servers
    /// </summary>
    public int RetryTime { get; protected set; }
    /// <summary>
    /// Servers list in the group
    /// </summary>
    protected IList<ReplicationServer> Servers { get; private set; }

    /// <summary>
    /// Adds a server into the group
    /// </summary>
    /// <param name="name">Server name</param>
    /// <param name="isMaster">True if the server to add is master, False for slave server</param>
    /// <param name="connectionString">Connection string used by this server</param>
    /// <returns></returns>
    internal protected ReplicationServer AddServer(string name, bool isMaster, string connectionString)
    {
      ReplicationServer server = new ReplicationServer(name, isMaster, connectionString);
      servers.Add(server);
      return server;
    }

    /// <summary>
    /// Removes a server from group
    /// </summary>
    /// <param name="name">Server name</param>
    internal protected void RemoveServer(string name)
    {
      ReplicationServer serverToRemove = GetServer(name);
      if (serverToRemove == null)
        throw new MyCatException(String.Format(Resources.ReplicationServerNotFound, name));
      servers.Remove(serverToRemove);
    }

    /// <summary>
    /// Gets a server by name
    /// </summary>
    /// <param name="name">Server name</param>
    /// <returns>Replication server</returns>
    internal protected ReplicationServer GetServer(string name)
    {
      foreach (var server in servers)
        if (String.Compare(name, server.Name, StringComparison.OrdinalIgnoreCase) == 0) return server;
      return null;
    }

    /// <summary>
    /// Must be implemented. Defines the next server for a custom load balancing implementation.
    /// </summary>
    /// <param name="isMaster">Defines if the server to return is a master or any</param>
    /// <returns>Next server based on the load balancing implementation.
    ///   Null if no available server is found.
    /// </returns>
    internal protected abstract ReplicationServer GetServer(bool isMaster);

    internal protected virtual ReplicationServer GetServer(bool isMaster, MyCatConnectionStringBuilder settings)
    {
      return GetServer(isMaster);
    }

    /// <summary>
    /// Handles a failed connection to a server.
    /// This method can be overrided to implement a custom failover handling
    /// </summary>
    /// <param name="server">The failed server</param>
    internal protected virtual void HandleFailover(ReplicationServer server)
    {
      BackgroundWorker worker = new BackgroundWorker();
      worker.DoWork += delegate(object sender, DoWorkEventArgs e)
      {
        bool isRunning = false;
        ReplicationServer server1 = e.Argument as ReplicationServer;
#if !RT
        System.Timers.Timer timer = new System.Timers.Timer(RetryTime * 1000.0);

        System.Timers.ElapsedEventHandler elapsedEvent = delegate(object sender1, System.Timers.ElapsedEventArgs e1)
        {
          if (isRunning) return;
          try
          {
            isRunning = true;
            using (MyCatConnection connectionFailed = new MyCatConnection(server.ConnectionString))
            {
              connectionFailed.Open();
              server1.IsAvailable = true;
              timer.Stop();
            }
          }
          catch
          {
            MyCatTrace.LogWarning(0,
              string.Format(Properties.Resources.Replication_ConnectionAttemptFailed, server1.Name));
          }
          finally
          {
            isRunning = false;
          }
        };
        timer.Elapsed += elapsedEvent;
        timer.Start();
        elapsedEvent(sender, null);
#else
              Windows.UI.Xaml.DispatcherTimer timer = new Windows.UI.Xaml.DispatcherTimer();
              TimeSpan ts = new TimeSpan(RetryTime * 1000);
              System.EventHandler<object> elapsedEvent = (TickSender, TickEventArgs) =>
              {
                  if (isRunning) return;
                  try
                  {
                      isRunning = true;
                      using (MyCatConnection connectionFailed = new MyCatConnection(server.ConnectionString))
                      {
                          connectionFailed.Open();
                          server1.IsAvailable = true;
                          timer.Stop();
                      }
                  }
                  catch
                  {
                      MyCatTrace.LogWarning(0,
                        string.Format(Properties.Resources.Replication_ConnectionAttemptFailed, server1.Name));
                  }
                  finally
                  {
                      isRunning = false;
                  }
              };
              timer.Tick += elapsedEvent;
              elapsedEvent(sender, null);
              timer.Start();
#endif
      };

      worker.RunWorkerAsync(server);
    }

    /// <summary>
    /// Handles a failed connection to a server.
    /// </summary>
    /// <param name="server">The failed server</param>
    /// <param name="exception">Exception that caused the failover</param>
    internal protected virtual void HandleFailover(ReplicationServer server, Exception exception)
    {
      HandleFailover(server);
    }
  }
}
#endif
