// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{
  /// <summary>
  /// Summary description for MyCatConversionException.
  /// </summary>
  
  public class MyCatConversionException : Exception
  {
    /// <summary>Ctor</summary>
    public MyCatConversionException(string msg)
      : base(msg)
    {
    }
  }
}
