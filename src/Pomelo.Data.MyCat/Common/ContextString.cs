// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections;
using System.Text;

namespace Pomelo.Data.Common
{
  internal class ContextString
  {
    string contextMarkers;
    bool escapeBackslash;

    // Create a private ctor so the compiler doesn't give us a default one
    public ContextString(string contextMarkers, bool escapeBackslash)
    {
      this.contextMarkers = contextMarkers;
      this.escapeBackslash = escapeBackslash;
    }

    public string ContextMarkers
    {
      get { return contextMarkers; }
      set { contextMarkers = value; }
    }

    public int IndexOf(string src, string target)
    {
      return IndexOf(src, target, 0);
    }

    public int IndexOf(string src, string target, int startIndex)
    {
      int index = src.IndexOf(target, startIndex);
      while (index != -1)
      {
        if (!IndexInQuotes(src, index, startIndex)) break;
        index = src.IndexOf(target, index + 1);
      }
      return index;
    }

    private bool IndexInQuotes(string src, int index, int startIndex)
    {
      char contextMarker = Char.MinValue;
      bool escaped = false;

      for (int i = startIndex; i < index; i++)
      {
        char c = src[i];

        int contextIndex = contextMarkers.IndexOf(c);

        // if we have found the closing marker for our open marker, then close the context
        if (contextIndex > -1 && contextMarker == contextMarkers[contextIndex] && !escaped)
          contextMarker = Char.MinValue;

        // if we have found a context marker and we are not in a context yet, then start one
        else if (contextMarker == Char.MinValue && contextIndex > -1 && !escaped)
          contextMarker = c;

        else if (c == '\\' && escapeBackslash)
          escaped = !escaped;
      }
      return contextMarker != Char.MinValue || escaped;
    }

    public int IndexOf(string src, char target)
    {
      char contextMarker = Char.MinValue;
      bool escaped = false;
      int pos = 0;

      foreach (char c in src)
      {
        int contextIndex = contextMarkers.IndexOf(c);

        // if we have found the closing marker for our open marker, then close the context
        if (contextIndex > -1 && contextMarker == contextMarkers[contextIndex] && !escaped)
          contextMarker = Char.MinValue;

        // if we have found a context marker and we are not in a context yet, then start one
        else if (contextMarker == Char.MinValue && contextIndex > -1 && !escaped)
          contextMarker = c;

        else if (contextMarker == Char.MinValue && c == target)
          return pos;
        else if (c == '\\' && escapeBackslash)
          escaped = !escaped;
        pos++;
      }
      return -1;
    }
  }
}
