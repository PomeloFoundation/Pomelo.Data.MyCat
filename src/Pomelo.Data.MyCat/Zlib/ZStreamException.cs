// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;

namespace zlib
{	
//	
	class ZStreamException:System.IO.IOException
	{
		public ZStreamException():base()
		{
		}
		public ZStreamException(System.String s):base(s)
		{
		}
	}
}