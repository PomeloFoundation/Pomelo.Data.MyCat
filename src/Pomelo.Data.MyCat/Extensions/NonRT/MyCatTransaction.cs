// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System.Data.Common;

namespace Pomelo.Data.MyCat
{
  public sealed partial class MyCatTransaction : DbTransaction
  {
    protected override DbConnection DbConnection
    {
      get { return conn; }
    }
  }
}
