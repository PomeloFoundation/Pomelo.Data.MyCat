// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.IO;

namespace Pomelo.Data.Common
{
  internal class Platform
  {
    private static bool inited;
    private static bool isMono;

    /// <summary>
    /// By creating a private ctor, we keep the compiler from creating a default ctor
    /// </summary>
    private Platform()
    {
    }

    public static bool IsWindows()
    {
#if NETSTANDARD1_3
      return true;
#else
      OperatingSystem os = Environment.OSVersion;
      switch (os.Platform)
      {
        case PlatformID.Win32NT:
        case PlatformID.Win32S:
        case PlatformID.Win32Windows:
          return true;
      }
      return false;
#endif
    }

    public static char DirectorySeparatorChar
    {
      get
      {
#if NETSTANDARD1_3
                return '\\';
#else
          return Path.DirectorySeparatorChar;
#endif
      }
    }

    public static bool IsMono()
    {
      if (!inited)
        Init();
      return isMono;
    }

    private static void Init()
    {
      inited = true;
      Type t = Type.GetType("Mono.Runtime");
      isMono = t != null;
    }
  }
}
