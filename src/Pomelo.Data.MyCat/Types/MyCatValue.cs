// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Data;
using System.Globalization;
using Pomelo.Data.MyCat;

namespace Pomelo.Data.Types
{
  internal interface IMyCatValue
  {
    bool IsNull { get; }
    MyCatDbType MyCatDbType { get; }
    object Value { get; /*set;*/ }
    Type SystemType { get; }
    string MyCatTypeName { get; }

    void WriteValue(MyCatPacket packet, bool binary, object value, int length);
    IMyCatValue ReadValue(MyCatPacket packet, long length, bool isNull);
    void SkipValue(MyCatPacket packet);
  }
}
