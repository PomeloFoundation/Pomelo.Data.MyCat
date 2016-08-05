// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System.IO;
using System;

using Pomelo.Data.Common;
using System.Security.Cryptography;
#if RT
using AliasText = Pomelo.Data.MyCat.RT;
#else
using AliasText = System.Text;
#endif

namespace Pomelo.Data.MyCat.Authentication
{
  public class MyCatNativePasswordPlugin : MyCatAuthenticationPlugin
  {
    public override string PluginName
    {
      get { return "mysql_native_password"; }
    }

    protected override void SetAuthData(byte[] data)
    {
      // if the data given to us is a null terminated string, we need to trim off the trailing zero
      if (data[data.Length - 1] == 0)
      {
        byte[] b = new byte[data.Length - 1];
        Buffer.BlockCopy(data, 0, b, 0, data.Length - 1);
        base.SetAuthData(b);
      }
      else
        base.SetAuthData(data);
    }

    protected override byte[] MoreData(byte[] data)
    {
      byte[] passBytes = GetPassword() as byte[];
      byte[] buffer = new byte[passBytes.Length - 1];
      Array.Copy(passBytes, 1, buffer, 0, passBytes.Length - 1);
      return buffer;
    }

    public override object GetPassword()
    {
      byte[] bytes = Get411Password(Settings.Password, AuthenticationData);
      if (bytes != null && bytes.Length == 1 && bytes[0] == 0) return null;
      return bytes;
    }

    /// <summary>
    /// Returns a byte array containing the proper encryption of the 
    /// given password/seed according to the new 4.1.1 authentication scheme.
    /// </summary>
    /// <param name="password"></param>
    /// <param name="seed"></param>
    /// <returns></returns>
    private byte[] Get411Password(string password, byte[] seedBytes)
    {
      // if we have no password, then we just return 1 zero byte
      if (password.Length == 0) return new byte[1];
#if !NETSTANDARD1_3
      SHA1 sha = new SHA1CryptoServiceProvider();
#else
      SHA1 sha = SHA1.Create();
#endif

#if !NETSTANDARD1_3
            byte[] firstHash = sha.ComputeHash(AliasText.Encoding.Default.GetBytes(password));

#else
            byte[] firstHash = sha.ComputeHash(AliasText.Encoding.UTF8.GetBytes(password));
#endif
            byte[] secondHash = sha.ComputeHash(firstHash);

      byte[] input = new byte[seedBytes.Length + secondHash.Length];
      Array.Copy(seedBytes, 0, input, 0, seedBytes.Length);
      Array.Copy(secondHash, 0, input, seedBytes.Length, secondHash.Length);
      byte[] thirdHash = sha.ComputeHash(input);

      byte[] finalHash = new byte[thirdHash.Length + 1];
      finalHash[0] = 0x14;
      Array.Copy(thirdHash, 0, finalHash, 1, thirdHash.Length);

      for (int i = 1; i < finalHash.Length; i++)
        finalHash[i] = (byte)(finalHash[i] ^ firstHash[i - 1]);
      return finalHash;
    }
  }
}
