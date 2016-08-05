// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;


namespace Pomelo.Data.MyCat.Authentication
{
    internal class AuthenticationPluginManager
    {
      static Dictionary<string, PluginInfo> plugins = new Dictionary<string, PluginInfo>();

      static AuthenticationPluginManager()
      {
        plugins["mysql_native_password"] = new PluginInfo("Pomelo.Data.MyCat.Authentication.MyCatNativePasswordPlugin");
        plugins["sha256_password"] = new PluginInfo("Pomelo.Data.MyCat.Authentication.Sha256AuthenticationPlugin");
#if !NETSTANDARD1_3
        plugins["authentication_windows_client"] = new PluginInfo("Pomelo.Data.MyCat.Authentication.MyCatWindowsAuthenticationPlugin");
        if (MyCatConfiguration.Settings != null && MyCatConfiguration.Settings.AuthenticationPlugins != null)
        {
          foreach (AuthenticationPluginConfigurationElement e in MyCatConfiguration.Settings.AuthenticationPlugins)
            plugins[e.Name] = new PluginInfo(e.Type);
        }
#endif
        }

        public static MyCatAuthenticationPlugin GetPlugin(string method)
      {
        if (!plugins.ContainsKey(method))
          throw new MyCatException(String.Format(Resources.AuthenticationMethodNotSupported, method));
        return CreatePlugin(method);
      }

      private static MyCatAuthenticationPlugin CreatePlugin(string method)
      {
        PluginInfo pi = plugins[method];

        try
        {
          Type t = Type.GetType(pi.Type);
          MyCatAuthenticationPlugin o = (MyCatAuthenticationPlugin)Activator.CreateInstance(t);
          return o;
        }
        catch (Exception e)
        {
          throw new MyCatException(String.Format(Resources.UnableToCreateAuthPlugin, method), e);
        }
      }
    }

    struct PluginInfo
    {
      public string Type;
      public Assembly Assembly;

      public PluginInfo(string type)
      {
        Type = type;
        Assembly = null;
      }
    }
}
