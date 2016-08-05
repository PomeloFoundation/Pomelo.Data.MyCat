// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if SUPPORT_REPLICATION
using System;
using System.Collections.Generic;
using System.Text;

namespace Pomelo.Data.MyCat.Replication
{
  /// <summary>
  /// Class that implements Round Robing Load Balancing technique
  /// </summary>
  public class ReplicationRoundRobinServerGroup : ReplicationServerGroup
  {
    private int nextServer;

    public ReplicationRoundRobinServerGroup(string name, int retryTime) : base(name, retryTime)
    {
      nextServer = -1;
    }

    /// <summary>
    /// Gets an available server based on Round Robin load balancing
    /// </summary>
    /// <param name="isMaster">True if the server to return must be a master</param>
    /// <returns>Next available server</returns>
    internal protected override ReplicationServer GetServer(bool isMaster)
    {
      for (int i = 0; i < Servers.Count; i++)
      {
        nextServer++;
        if (nextServer == Servers.Count)
          nextServer = 0;
        ReplicationServer s = Servers[nextServer];
        if (!s.IsAvailable) continue;
        if (isMaster && !s.IsMaster) continue;
        return s;
      }
      return null;
    }
  }
}
#endif
