// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using Pomelo.Data.MyCat;
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;

namespace Pomelo.Data.Common
{
    internal class MyNetworkStream : NetworkStream
    {
        /// <summary>
        /// Wrapper around NetworkStream.
        /// 
        /// MyNetworkStream is equivalent to NetworkStream, except 
        /// 1. It throws TimeoutException if read or write timeout occurs, instead 
        /// of IOException, to match behavior of other streams (named pipe and 
        /// shared memory). This property comes handy in TimedStream.
        ///
        /// 2. It implements workarounds for WSAEWOULDBLOCK errors, that can start 
        /// occuring after stream has times out. For a discussion about the CLR bug,
        /// refer to  http://tinyurl.com/lhgpyf. This error should never occur, as
        /// we're not using asynchronous operations, but apparerntly it does occur
        /// directly after timeout has expired.
        /// The workaround is hinted in the URL above and implemented like this:
        /// For each IO operation, if it throws WSAEWOULDBLOCK, we explicitely set
        /// the socket to Blocking and retry the operation once again.
        /// </summary>
        const int MaxRetryCount = 2;
        Socket socket;

        public MyNetworkStream(Socket socket, bool ownsSocket)
          : base(socket, ownsSocket)
        {
            this.socket = socket;
        }

        bool IsTimeoutException(SocketException e)
        {
            return (e.SocketErrorCode == SocketError.TimedOut);
        }

        bool IsWouldBlockException(SocketException e)
        {
            return (e.SocketErrorCode == SocketError.WouldBlock);
        }


        void HandleOrRethrowException(Exception e)
        {
            Exception currentException = e;
            while (currentException != null)
            {
                if (currentException is SocketException)
                {
                    SocketException socketException = (SocketException)currentException;
                    if (IsWouldBlockException(socketException))
                    {
                        // Workaround  for WSAEWOULDBLOCK
                        socket.Blocking = true;
                        // return to give the caller possibility to retry the call
                        return;
                    }
                    else if (IsTimeoutException(socketException))
                    {
                        return;
                        //throw new TimeoutException(socketException.Message, e);
                    }

                }
                currentException = currentException.InnerException;
            }
            throw (e);
        }


        public override int Read(byte[] buffer, int offset, int count)
        {
            int retry = 0;
            Exception exception = null;
            do
            {
                try
                {
                    return base.Read(buffer, offset, count);
                }
                catch (Exception e)
                {
                    exception = e;
                    HandleOrRethrowException(e);
                }
            }
            while (++retry < MaxRetryCount);
            if (exception.GetBaseException() is SocketException
              && IsTimeoutException((SocketException)exception.GetBaseException()))
                throw new TimeoutException(exception.Message, exception);
            throw exception;
        }

        public override int ReadByte()
        {
            int retry = 0;
            Exception exception = null;
            do
            {
                try
                {
                    return base.ReadByte();
                }
                catch (Exception e)
                {
                    exception = e;
                    HandleOrRethrowException(e);
                }
            }
            while (++retry < MaxRetryCount);
            throw exception;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            int retry = 0;
            Exception exception = null;
            do
            {
                try
                {
                    base.Write(buffer, offset, count);
                    return;
                }
                catch (Exception e)
                {
                    exception = e;
                    HandleOrRethrowException(e);
                }
            }
            while (++retry < MaxRetryCount);
            throw exception;
        }

        public override void Flush()
        {
            int retry = 0;
            Exception exception = null;
            do
            {
                try
                {
                    base.Flush();
                    return;
                }
                catch (Exception e)
                {
                    exception = e;
                    HandleOrRethrowException(e);
                }
            }
            while (++retry < MaxRetryCount);
            throw exception;
        }

        #region Create Code

        public static MyNetworkStream CreateStream(MyCatConnectionStringBuilder settings, bool unix)
        {
            MyNetworkStream stream = null;
            IPAddress[] ipHE = GetHostEntry(settings.Server);
            foreach (IPAddress address in ipHE)
            {
                try
                {
                    stream = CreateSocketStream(settings, address, unix);
                    if (stream != null) break;
                }
                catch (Exception ex)
                {
                    SocketException socketException = ex as SocketException;
                    // if the exception is a ConnectionRefused then we eat it as we may have other address
                    // to attempt
                    if (socketException == null) throw;
                    if (socketException.SocketErrorCode != SocketError.ConnectionRefused) throw;
                }
            }
            return stream;
        }

        private static IPHostEntry ParseIPAddress(string hostname)
        {
            IPHostEntry ipHE = null;
            IPAddress addr;
            if (IPAddress.TryParse(hostname, out addr))
            {
                ipHE = new IPHostEntry();
                ipHE.AddressList = new IPAddress[1];
                ipHE.AddressList[0] = addr;
            }
            return ipHE;
        }

        private static IPAddress[] GetHostEntry(string hostname)
        {
            IPHostEntry ipHE = ParseIPAddress(hostname);
            if (ipHE != null) return ipHE.AddressList;
#if NET451
            return Dns.GetHostAddresses(hostname);
#else
            return Dns.GetHostAddressesAsync(hostname).Result;
#endif
        }

        private static MyNetworkStream CreateSocketStream(MyCatConnectionStringBuilder settings, IPAddress ip, bool unix)
        {
            EndPoint endPoint;
            endPoint = new IPEndPoint(ip, (int)settings.Port);

            Socket socket = unix ?
                new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.IP) :
                new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            if (settings.Keepalive > 0)
            {
                SetKeepAlive(socket, settings.Keepalive);
            }

            try
            {
                
                socket.Connect(endPoint);
            }
            catch (Exception ex)
            {
#if !NETSTANDARD1_3
                socket.Close();
#else
                socket.Dispose();
#endif
                throw ex;
            }
            
            MyNetworkStream stream = new MyNetworkStream(socket, true);
            GC.SuppressFinalize(socket);
            GC.SuppressFinalize(stream);
            return stream;
        }



        /// <summary>
        /// Set keepalive + timeout on socket.
        /// </summary>
        /// <param name="s">socket</param>
        /// <param name="time">keepalive timeout, in seconds</param>
        private static void SetKeepAlive(Socket s, uint time)
        {
            uint on = 1;
            uint interval = 1000; // default interval = 1 sec

            uint timeMilliseconds;
            if (time > UInt32.MaxValue / 1000)
                timeMilliseconds = UInt32.MaxValue;
            else
                timeMilliseconds = time * 1000;

            // Use Socket.IOControl to implement equivalent of
            // WSAIoctl with  SOL_KEEPALIVE_VALS 

            // the native structure passed to WSAIoctl is
            //struct tcp_keepalive {
            //    ULONG onoff;
            //    ULONG keepalivetime;
            //    ULONG keepaliveinterval;
            //};
            // marshal the equivalent of the native structure into a byte array

            byte[] inOptionValues = new byte[12];
            BitConverter.GetBytes(on).CopyTo(inOptionValues, 0);
            BitConverter.GetBytes(timeMilliseconds).CopyTo(inOptionValues, 4);
            BitConverter.GetBytes(interval).CopyTo(inOptionValues, 8);
            try
            {
                // call WSAIoctl via IOControl
                s.IOControl(IOControlCode.KeepAliveValues, inOptionValues, null);
                return;
            }
            catch (NotImplementedException)
            {
                // Mono throws not implemented currently
            }
            // Fallback if Socket.IOControl is not available ( Compact Framework )
            // or not implemented ( Mono ). Keepalive option will still be set, but
            // with timeout is kept default.
            s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1);
        }

#endregion

    }
}