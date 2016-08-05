// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if SUPPORT_REPLICATION
using System;
using System.Collections.Generic;
using System.Text;

namespace Pomelo.Data.MyCat.Replication
{
  /// <summary>
  /// Represents a server in Replication environment
  /// </summary>
  public class ReplicationServer
  {
    public ReplicationServer(string name, bool isMaster, string connectionString)
    {
      Name = name;
      IsMaster = isMaster;
      ConnectionString = connectionString;
      IsAvailable = true;
    }

    /// <summary>
    /// Server name
    /// </summary>
    public string Name { get; private set; }
    /// <summary>
    /// Defines if the server is master (True) or slave
    /// </summary>
    public bool IsMaster { get; private set; }
    /// <summary>
    /// Connection string used to connect to the server
    /// </summary>
    public string ConnectionString { get; internal set; }
    /// <summary>
    /// Defines if the server is available to be considered in load balancing
    /// </summary>
    public bool IsAvailable { get; set; }
  }
}
#endif
