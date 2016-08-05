// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
#if RT
using Windows.Security.Cryptography;
#else
using System.Security.Cryptography;
#endif
using System.Text;
using Pomelo.Data.Common;

#if BOUNCY_CASTLE_INCLUDED
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Crypto.Parameters;
#endif

namespace Pomelo.Data.MyCat.Authentication
{
  /// <summary>
  /// The implementation of the sha256_password authentication plugin.
  /// </summary>
  public class Sha256AuthenticationPlugin : MyCatAuthenticationPlugin
  {
#if BOUNCY_CASTLE_INCLUDED
    private RsaKeyParameters publicKey;
#endif
    private byte[] rawPubkey;

    public override string PluginName
    {
      get { return "sha256_password"; }
    }

    protected override byte[] MoreData(byte[] data)
    {
      rawPubkey = data;
      byte[] buffer = GetPassword() as byte[];
      return buffer;
    }

    public override object GetPassword()
    {
      if (Settings.SslMode != MyCatSslMode.None)
      {
        // send as clear text, since the channel is already encrypted
        byte[] passBytes = this.Encoding.GetBytes(Settings.Password);
        byte[] buffer = new byte[passBytes.Length + 1];
        Array.Copy(passBytes, 0, buffer, 0, passBytes.Length);
        buffer[passBytes.Length] = 0;
        return buffer;
      }
      else
      {
#if BOUNCY_CASTLE_INCLUDED
        // send RSA encrypted, since the channel is not protected
        if (rawPubkey != null)
        {
          publicKey = GenerateKeysFromPem(rawPubkey);
        }
        if (publicKey == null) return new byte[] { 0x01 }; //RequestPublicKey();
        else
        {
          byte[] bytes = GetRsaPassword(Settings.Password, AuthenticationData);
          if (bytes != null && bytes.Length == 1 && bytes[0] == 0) return null;
          return bytes;
        }
#else
        throw new NotImplementedException( "You can use sha256 plugin only in SSL connections in this implementation." );
#endif 
      }
    }

#if BOUNCY_CASTLE_INCLUDED
    private void RequestPublicKey()
    {
      RsaKeyParameters keys = GenerateKeysFromPem(rawPubkey);
      publicKey = keys;
    }

    private RsaKeyParameters GenerateKeysFromPem(byte[] rawData)
    {
      PemReader pem = new PemReader(new StreamReader(new MemoryStream( rawData )));
      RsaKeyParameters keyPair = (RsaKeyParameters)pem.ReadObject();
      return keyPair;
    }

    private byte[] GetRsaPassword(string password, byte[] seedBytes)
    {
      // Obfuscate the plain text password with the session scramble
      byte[] ofuscated = GetXor(this.Encoding.GetBytes(password), seedBytes);
      // Encrypt the password and send it to the server
      byte[] result = Encrypt(ofuscated, publicKey );
      return result;
    }

    private byte[] GetXor( byte[] src, byte[] pattern )
    {
      byte[] src2 = new byte[src.Length + 1];
      Array.Copy(src, 0, src2, 0, src.Length);
      src2[src.Length] = 0;
      byte[] result = new byte[src2.Length];
      for (int i = 0; i < src2.Length; i++)
      {
        result[ i ] = ( byte )( src2[ i ] ^ ( pattern[ i % pattern.Length ] ));
      }
      return result;
    }

    private byte[] Encrypt(byte[] data, RsaKeyParameters key)
    { 
      IBufferedCipher c = CipherUtilities.GetCipher("RSA/NONE/OAEPPadding");
      c.Init(true, key);
      byte[] result = c.DoFinal(data);
      return result;
    }
#endif
  }
}
