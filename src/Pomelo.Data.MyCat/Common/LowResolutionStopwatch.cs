// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;

namespace Pomelo.Data.Common
{
  /// <summary>
  /// This class is modeled after .NET Stopwatch. It provides better
  /// performance (no system calls).It is however less precise than
  /// .NET Stopwatch, measuring in milliseconds. It is adequate to use
  /// when high-precision is not required (e.g for measuring IO timeouts),
  /// but not for other tasks.
  /// </summary>
  class LowResolutionStopwatch
  {
    long millis;
    long startTime;
    public static readonly long Frequency = 1000; // measure in milliseconds
    public static readonly bool isHighResolution = false;

    public LowResolutionStopwatch()
    {
      millis = 0;
    }
    public long ElapsedMilliseconds
    {
      get { return millis; }
    }
    public void Start()
    {
      startTime = Environment.TickCount;
    }

    public void Stop()
    {
      long now = Environment.TickCount;
      // Calculate time different, handle possible overflow
      long elapsed = (now < startTime) ? Int32.MaxValue - startTime + now : now - startTime;
      millis += elapsed;
    }

    public void Reset()
    {
      millis = 0;
      startTime = 0;
    }

    public TimeSpan Elapsed
    {
      get
      {
        return new TimeSpan(0, 0, 0, 0, (int)millis);
      }
    }

    public static LowResolutionStopwatch StartNew()
    {
      LowResolutionStopwatch sw = new LowResolutionStopwatch();
      sw.Start();
      return sw;
    }

    public static long GetTimestamp()
    {
      return Environment.TickCount;
    }

    bool IsRunning()
    {
      return (startTime != 0);
    }
  }
}
