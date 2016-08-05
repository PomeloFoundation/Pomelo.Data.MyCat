// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if NET451
using System;
using System.Collections.Generic;
using System.Text;
using System.Security;
using System.Security.Permissions;
using System.Net;


namespace Pomelo.Data.MyCat
{
  public sealed class MyCatSecurityPermission : MarshalByRefObject
  {
    private MyCatSecurityPermission()
    {
    }

    public static PermissionSet CreatePermissionSet(bool includeReflectionPermission)
    {
      PermissionSet permissionsSet = new PermissionSet(null);
      permissionsSet.AddPermission(new SecurityPermission(SecurityPermissionFlag.Execution));
      permissionsSet.AddPermission(new SocketPermission(PermissionState.Unrestricted));
      permissionsSet.AddPermission(new SecurityPermission(SecurityPermissionFlag.UnmanagedCode));
      permissionsSet.AddPermission(new DnsPermission(PermissionState.Unrestricted));
      permissionsSet.AddPermission(new FileIOPermission(PermissionState.Unrestricted));
      permissionsSet.AddPermission(new EnvironmentPermission(PermissionState.Unrestricted));

      if (includeReflectionPermission) permissionsSet.AddPermission(new ReflectionPermission(PermissionState.Unrestricted));

      return permissionsSet;
    }
  }
}
#endif
