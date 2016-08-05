// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if NET451
using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;

namespace Pomelo.Data.MyCat
{
  //[ToolboxBitmap(typeof(MyCatCommand), "MyCatClient.resources.command.bmp")]
  [DesignerCategory("Code")]
  public sealed partial class MyCatCommand : DbCommand
  {
    partial void Constructor()
    {
      UpdatedRowSource = UpdateRowSource.Both;
    }

    partial void PartialClone(MyCatCommand clone)
    {
      clone.UpdatedRowSource = UpdatedRowSource;
    }

    /// <summary>
    /// Gets or sets how command results are applied to the DataRow when used by the 
    /// Update method of the DbDataAdapter. 
    /// </summary>
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the command object should be visible in a Windows Form Designer control. 
    /// </summary>
    [Browsable(false)]
    public override bool DesignTimeVisible { get; set; }

    protected override DbParameter CreateDbParameter()
    {
      return new MyCatParameter();
    }

    protected override DbConnection DbConnection
    {
      get { return Connection; }
      set { Connection = (MyCatConnection)value; }
    }

    protected override DbParameterCollection DbParameterCollection
    {
      get { return Parameters; }
    }

    protected override DbTransaction DbTransaction
    {
      get { return Transaction; }
      set { Transaction = (MyCatTransaction)value; }
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
      return ExecuteReader(behavior);
    }
  }
}
#endif
