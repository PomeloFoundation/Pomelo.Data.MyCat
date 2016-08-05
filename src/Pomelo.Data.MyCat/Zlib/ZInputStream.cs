// Copyright (c) Pomelo Foundation. All rights reserved.
// Licensed under the MIT. See LICENSE in the project root for license information.

#if NET451
using System;

namespace zlib
{

	class ZInputStream : System.IO.BinaryReader
	{
		public long maxInput;

		private void InitBlock()
		{
			flush = zlibConst.Z_NO_FLUSH;
			buf = new byte[bufsize];
		}
		virtual public int FlushMode
		{
			get
			{
				return (flush);
			}

			set
			{
				this.flush = value;
			}

		}
		/// <summary> Returns the total number of bytes input so far.</summary>
		virtual public long TotalIn
		{
			get
			{
				return z.total_in;
			}

		}
		/// <summary> Returns the total number of bytes output so far.</summary>
		virtual public long TotalOut
		{
			get
			{
				return z.total_out;
			}

		}

		protected internal ZStream z = new ZStream();
		protected internal int bufsize = 512;
		protected internal int flush;
		protected internal byte[] buf, buf1 = new byte[1];
		protected internal bool compress;

		private System.IO.Stream in_Renamed = null;

		public ZInputStream(System.IO.Stream in_Renamed)
			: base(in_Renamed)
		{
			InitBlock();
			this.in_Renamed = in_Renamed;
			z.inflateInit();
			compress = false;
			z.next_in = buf;
			z.next_in_index = 0;
			z.avail_in = 0;
		}

		public ZInputStream(System.IO.Stream in_Renamed, int level)
			: base(in_Renamed)
		{
			InitBlock();
			this.in_Renamed = in_Renamed;
			z.deflateInit(level);
			compress = true;
			z.next_in = buf;
			z.next_in_index = 0;
			z.avail_in = 0;
		}

		/*public int available() throws IOException {
		return inf.finished() ? 0 : 1;
		}*/

		public override int Read()
		{
			if (read(buf1, 0, 1) == -1)
				return (-1);
			return (buf1[0] & 0xFF);
		}

		private bool nomoreinput = false;

		public int read(byte[] b, int off, int len)
		{
			if (len == 0)
				return (0);
			int err;
			z.next_out = b;
			z.next_out_index = off;
			z.avail_out = len;
			do
			{
				if ((z.avail_in == 0) && (!nomoreinput))
				{
					// if buffer is empty and more input is avaiable, refill it
					z.next_in_index = 0;

					int inToRead = bufsize;
					if (maxInput > 0)
					{
						if (TotalIn < maxInput)
							inToRead = (int)(Math.Min(maxInput - TotalIn, (long)bufsize));
						else
							z.avail_in = -1;
					}
					if (z.avail_in != -1)
						z.avail_in = SupportClass.ReadInput(in_Renamed, buf, 0, inToRead); //(bufsize<z.avail_out ? bufsize : z.avail_out));
					if (z.avail_in == -1)
					{
						z.avail_in = 0;
						nomoreinput = true;
					}
				}
				if (compress)
					err = z.deflate(flush);
				else
					err = z.inflate(flush);
				if (nomoreinput && (err == zlibConst.Z_BUF_ERROR))
					return (-1);
				if (err != zlibConst.Z_OK && err != zlibConst.Z_STREAM_END)
					throw new ZStreamException((compress ? "de" : "in") + "flating: " + z.msg);
				if (nomoreinput && (z.avail_out == len))
					return (-1);
			}
			while (z.avail_out > 0 && err == zlibConst.Z_OK);
			//while (z.avail_out == len && err == zlibConst.Z_OK);
			//System.err.print("("+(len-z.avail_out)+")");
			return (len - z.avail_out);
		}

		public long skip(long n)
		{
			int len = 512;
			if (n < len)
				len = (int)n;
			byte[] tmp = new byte[len];
			return ((long)SupportClass.ReadInput(BaseStream, tmp, 0, tmp.Length));
		}
        
		public override void Close()
		{
			in_Renamed.Close();
		}
	}
}
#endif
