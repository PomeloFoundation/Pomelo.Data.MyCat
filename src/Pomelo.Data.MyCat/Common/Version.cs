// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using Pomelo.Data.MyCat;


namespace Pomelo.Data.Common
{
  /// <summary>
  /// Summary description for Version.
  /// </summary>
  internal struct DBVersion
  {
    private int major;
    private int minor;
    private int build;
    private string srcString;

    public DBVersion(string s, int major, int minor, int build)
    {
      this.major = major;
      this.minor = minor;
      this.build = build;
      srcString = s;
    }

    public int Major
    {
      get { return major; }
    }

    public int Minor
    {
      get { return minor; }
    }

    public int Build
    {
      get { return build; }
    }

    public static DBVersion Parse(string versionString)
    {
      int start = 0;
      int index = versionString.IndexOf('.', start);
      if (index == -1)
        throw new MyCatException(Resources.BadVersionFormat);
      string val = versionString.Substring(start, index - start).Trim();
      int major = Convert.ToInt32(val, System.Globalization.NumberFormatInfo.InvariantInfo);

      start = index + 1;
      index = versionString.IndexOf('.', start);
      if (index == -1)
        throw new MyCatException(Resources.BadVersionFormat);
      val = versionString.Substring(start, index - start).Trim();
      int minor = Convert.ToInt32(val, System.Globalization.NumberFormatInfo.InvariantInfo);

      start = index + 1;
      int i = start;
      while (i < versionString.Length && Char.IsDigit(versionString, i))
        i++;
      val = versionString.Substring(start, i - start).Trim();
      int build = Convert.ToInt32(val, System.Globalization.NumberFormatInfo.InvariantInfo);

      return new DBVersion(versionString, major, minor, build);
    }

    public bool isAtLeast(int majorNum, int minorNum, int buildNum)
    {
      if (major > majorNum) return true;
      if (major == majorNum && minor > minorNum) return true;
      if (major == majorNum && minor == minorNum && build >= buildNum) return true;
      return false;
    }

    public override string ToString()
    {
      return srcString;
    }

  }
}
