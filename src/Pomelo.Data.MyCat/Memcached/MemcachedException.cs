// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

namespace Pomelo.Data.MyCat.Memcached
{
  using System;
  using System.Collections.Generic;
  using System.Text;

  /// <summary>
  /// The base exception class for all Memcached exceptions.
  /// </summary>
  public class MemcachedException : Exception
  {
    public MemcachedException(string msg)
      : base(msg)
    {
    }

    public MemcachedException(string msg, Exception e)
      : base(msg, e)
    {
    }
  }
}
