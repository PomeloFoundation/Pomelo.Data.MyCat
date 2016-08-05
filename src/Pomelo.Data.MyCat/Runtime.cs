// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;

namespace MyCat.Web.Security
{
    internal static class Runtime
    {
        private static bool inited = false;
        private static bool isMono;

        public static bool IsMono
        {
            get
            {
                if (!inited)
                    Init();
                return isMono;
            }
        }

        private static void Init()
        {
            Type t = Type.GetType("Mono.Runtime");
            isMono = t != null;
            inited = true;
        }
    }
}
