// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

using System;
using System.IO;
using Pomelo.Data.MyCat;


namespace Pomelo.Data.Common
{
    internal class BufferedStream : Stream
    {
        private byte[] writeBuffer;
        private byte[] readBuffer;
        private int writePos;
        private int readLength;
        private int readPos;
        private int bufferSize;
        private Stream baseStream;

        public BufferedStream(Stream stream)
        {
            baseStream = stream;
            bufferSize = 4096;
            readBuffer = new byte[bufferSize];
            writeBuffer = new byte[bufferSize];
        }

        #region Stream Implementation

        public override bool CanRead
        {
            get
            {
                if (baseStream != null) return baseStream.CanRead;
                return false;
            }
        }

        public override bool CanSeek
        {
            get
            {
                if (baseStream != null) return baseStream.CanSeek;
                return false;
            }
        }

        public override bool CanWrite
        {
            get
            {
                if (baseStream != null) return baseStream.CanWrite;
                return false;
            }
        }

        public override void Flush()
        {
            if (baseStream == null)
                throw new InvalidOperationException(Resources.ObjectDisposed);
            if (writePos == 0) return;

            baseStream.Write(writeBuffer, 0, writePos);
            baseStream.Flush();
            writePos = 0;
        }

        public override long Length
        {
            get
            {
                if (baseStream == null)
                    throw new InvalidOperationException(Resources.ObjectDisposed);
                Flush();
                return baseStream.Length;
            }

        }

        public override long Position
        {
            get
            {
                throw new Exception("The method or operation is not implemented.");
            }
            set
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException("buffer", Resources.ParameterCannotBeNull);
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset", Resources.OffsetCannotBeNegative);
            if (count < 0)
                throw new ArgumentOutOfRangeException("count", Resources.CountCannotBeNegative);
            if ((buffer.Length - offset) < count)
                throw new ArgumentException(Resources.OffsetMustBeValid);
            if (baseStream == null)
                throw new InvalidOperationException(Resources.ObjectDisposed);

            if ((readLength - readPos) == 0)
            {
                TryToFillReadBuffer();
                if (readLength == 0) return 0;
            }

            int inBuffer = readLength - readPos;
            int toRead = count;
            if (toRead > inBuffer)
                toRead = inBuffer;
            Buffer.BlockCopy(readBuffer, readPos, buffer, offset, toRead);
            readPos += toRead;
            count -= toRead;
            if (count > 0)
            {
                int read = baseStream.Read(buffer, offset + toRead, count);
                toRead += read;
                readPos = readLength = 0;
            }
            return toRead;
        }

        private void TryToFillReadBuffer()
        {
            int read = baseStream.Read(readBuffer, 0, bufferSize);
            readPos = 0;
            readLength = read;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public override void SetLength(long value)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException("buffer", Resources.ParameterCannotBeNull);
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset", Resources.OffsetCannotBeNegative);
            if (count < 0)
                throw new ArgumentOutOfRangeException("count", Resources.CountCannotBeNegative);
            if ((buffer.Length - offset) < count)
                throw new ArgumentException(Resources.OffsetMustBeValid);
            if (baseStream == null)
                throw new InvalidOperationException(Resources.ObjectDisposed);

            // if we don't have enough room in our current write buffer for the data
            // then flush the data
            int roomLeft = bufferSize - writePos;
            if (count > roomLeft)
                Flush();

            // if the data will not fit into a entire buffer, then there is no need to buffer it.
            // We just send it down
            if (count > bufferSize)
                baseStream.Write(buffer, offset, count);
            else
            {
                // if we get here then there is room in our buffer for the data.  We store it and 
                // adjust our internal lengths.
                Buffer.BlockCopy(buffer, offset, writeBuffer, writePos, count);
                writePos += count;
            }
        }

        #endregion

    }
}
