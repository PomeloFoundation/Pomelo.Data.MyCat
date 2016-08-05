// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using Pomelo.Data.MyCat;

using System;
using System.IO;


namespace Pomelo.Data.Common
{
    /// <summary>
    /// Summary description for StreamCreator.
    /// </summary>
    internal class StreamCreator
    {
        string hostList;
        uint port;
        string pipeName;
        uint keepalive;
        DBVersion driverVersion;

        public StreamCreator(string hosts, uint port, string pipeName, uint keepalive, DBVersion driverVersion)
        {
            hostList = hosts;
            if (hostList == null || hostList.Length == 0)
                hostList = "localhost";
            this.port = port;
            this.pipeName = pipeName;
            this.keepalive = keepalive;
            this.driverVersion = driverVersion;
        }

        public static Stream GetStream(string server, uint port, string pipename, uint keepalive, DBVersion v, uint timeout)
        {
            MyCatConnectionStringBuilder settings = new MyCatConnectionStringBuilder();
            settings.Server = server;
            settings.Port = port;
            settings.PipeName = pipename;
            settings.Keepalive = keepalive;
            settings.ConnectionTimeout = timeout;
            return GetStream(settings);
        }

        public static Stream GetStream(MyCatConnectionStringBuilder settings)
        {
            switch (settings.ConnectionProtocol)
            {
                case MyCatConnectionProtocol.Tcp: return GetTcpStream(settings);
#if RT
        case MyCatConnectionProtocol.UnixSocket: throw new NotImplementedException();
        case MyCatConnectionProtocol.SharedMemory: throw new NotImplementedException();
#else
#if !NETSTANDARD1_3
        case MyCatConnectionProtocol.UnixSocket: return GetUnixSocketStream(settings);        
        case MyCatConnectionProtocol.SharedMemory: return GetSharedMemoryStream(settings);
#endif

#endif
#if !NETSTANDARD1_3
                case MyCatConnectionProtocol.NamedPipe: return GetNamedPipeStream(settings);
#endif
            }
            throw new InvalidOperationException(Resources.UnknownConnectionProtocol);
        }

        private static Stream GetTcpStream(MyCatConnectionStringBuilder settings)
        {
            MyNetworkStream s = MyNetworkStream.CreateStream(settings, false);
            return s;
        }

#if !NETSTANDARD1_3
    private static Stream GetUnixSocketStream(MyCatConnectionStringBuilder settings)
    {
      if (Platform.IsWindows())
        throw new InvalidOperationException(Resources.NoUnixSocketsOnWindows);

      MyNetworkStream s = MyNetworkStream.CreateStream(settings, true);
      return s;
    }

        private static Stream GetSharedMemoryStream(MyCatConnectionStringBuilder settings)
    {
      SharedMemoryStream str = new SharedMemoryStream(settings.SharedMemoryName);
      str.Open(settings.ConnectionTimeout);
      return str;
    }


    private static Stream GetNamedPipeStream(MyCatConnectionStringBuilder settings)
    {
      Stream stream = NamedPipeStream.Create(settings.PipeName, settings.Server, settings.ConnectionTimeout);
      return stream;
    }
#endif

    }
}
