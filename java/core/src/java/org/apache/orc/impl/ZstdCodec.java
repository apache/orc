package org.apache.orc.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;

public class ZstdCodec implements CompressionCodec, DirectDecompressionCodec {

	private static int DEFAULT_LEVEL = 9; 

	@Override
	public boolean isAvailable() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void directDecompress(ByteBuffer in, ByteBuffer out) throws IOException {
		Zstd.decompress(out, in);
	}

	public boolean directCompress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow, Options options) {
		Zstd.compress(out, in, DEFAULT_LEVEL);
		return true;
	}

	@Override
	public boolean compress(ByteBuffer in, ByteBuffer out,
			ByteBuffer overflow,
			Options options) {

		if(in.isDirect() && out.isDirect()) { 
			return directCompress(in, out, overflow, options);
		} else {
			int inBytes = in.remaining();
			// I should work on a patch for Snappy to support an overflow buffer
			// to prevent the extra buffer copy.
			byte[] compressed = new byte[Zstd.blockSizeMax()];
			byte[] toCompress = new byte[in.remaining()];
			
			System.arraycopy(in.array(), in.arrayOffset() + in.position(), toCompress, 0, toCompress.length);
			
			int outBytes = (int) Zstd.compress(compressed, toCompress, DEFAULT_LEVEL);
//			String log = "";
//			for (int i = 0; i < outBytes; i++)  {
//				log += ""+compressed[i]+" ";
//			}
//			System.out.println(log);
			//					  compressor.compress(in.array(), in.arrayOffset() + in.position(), inBytes,
			//							  compressed, 0, compressed.length);
			//System.out.println("compress.Block comp[" + outBytes + "] uncomp[" + inBytes + "]");
			if (outBytes < inBytes) {
				int remaining = out.remaining();
				if (remaining >= outBytes) {
					System.arraycopy(compressed, 0, out.array(), out.arrayOffset() + out.position(), outBytes);
					out.position(out.position() + outBytes);
				} else {
					System.arraycopy(compressed, 0, out.array(), out.arrayOffset() + out.position(), remaining);
					out.position(out.limit());
					System.arraycopy(compressed, remaining, overflow.array(), overflow.arrayOffset(), outBytes - remaining);
					overflow.position(outBytes - remaining);
				}
				return true;
			} else {
				return false;
			}
		}
	}

	

	public boolean compressV1(ByteBuffer in, ByteBuffer out,
			ByteBuffer overflow,
			Options options) {

		if(in.isDirect() && out.isDirect()) { 
			return directCompress(in, out, overflow, options);
		} else {
			int inBytes = in.remaining();
			// I should work on a patch for Snappy to support an overflow buffer
			// to prevent the extra buffer copy.
			byte[] compressed = new byte[Zstd.blockSizeMax()];
			int outBytes = (int) Zstd.compress(compressed, in.array(), DEFAULT_LEVEL);
			String log = "";
			for (int i = 0; i < outBytes; i++)  {
				log += ""+compressed[i]+" ";
			}
			System.out.println(log);
			//					  compressor.compress(in.array(), in.arrayOffset() + in.position(), inBytes,
			//							  compressed, 0, compressed.length);
			System.out.println("compress.Block comp[" + outBytes + "] uncomp[" + inBytes + "]");
			if (outBytes < inBytes) {
				int remaining = out.remaining();
				if (remaining >= outBytes) {
					System.arraycopy(compressed, 0, out.array(), out.arrayOffset() + out.position(), outBytes);
					out.position(out.position() + outBytes);
				} else {
					System.arraycopy(compressed, 0, out.array(), out.arrayOffset() + out.position(), remaining);
					out.position(out.limit());
					System.arraycopy(compressed, remaining, overflow.array(), overflow.arrayOffset(), outBytes - remaining);
					overflow.position(outBytes - remaining);
				}
				return true;
			} else {
				return false;
			}
		}
	}

	@Override
	public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {
		if(in.isDirect() && out.isDirect()) {
			directDecompress(in, out);
			return;
		}

		byte[] buf = new byte[1000];
		//int uncompressLen = 0;
		int read = -1;
		try (ZstdInputStream zis = new ZstdInputStream(new ByteArrayInputStream(in.array(), in.arrayOffset() + in.position(), in.remaining()))) {
			while ((read = zis.read(buf)) != -1) {
				//uncompressLen += read;
				System.arraycopy(buf, 0, out.array(), out.arrayOffset() + out.position(), read);
				out.position(out.position() + read);
			}
		}

//		System.out.println("decompress.Block comp[" + in.remaining() + "] uncomp[" + uncompressLen + "]");

		out.flip();
	}

	//@Override
	public void decompressV1(ByteBuffer in, ByteBuffer out) throws IOException {
		if(in.isDirect() && out.isDirect()) {
			directDecompress(in, out);
			return;
		}

		System.out.println("decompress.Block comp[" + in.remaining() + "]");

		byte[] comp = new byte[in.remaining()];
		byte[] uncomp = new byte[1000000];

		System.arraycopy(in.array(), in.arrayOffset() + in.position(), comp, 0, in.remaining());

		int inOffset = in.position();
		int uncompressLen = (int) Zstd.decompress(uncomp, comp);//Tah descomprimindo mais do que o necessario
		System.arraycopy(uncomp, 0, out.array(), out.arrayOffset() + out.position(), uncompressLen-3);
		String log = "";
		for (byte b : in.array()) {
			log += ""+b + " ";
		}
		System.out.println(log);
		System.out.println("decompress.Block uncomp[" + uncompressLen + "]");
		if (Zstd.isError(uncompressLen)) {
			System.out.println("ZSTD Error: " + Zstd.getErrorName(uncompressLen));
		}


		//	    int uncompressLen = decompressor.decompress(in.array(), 
		//	        										in.arrayOffset() + inOffset, 
		//	        										in.limit() - inOffset, 
		//	        										out.array(), 
		//	        										out.arrayOffset() + out.position(), 
		//	        										out.remaining());

		out.position(uncompressLen-3 + out.position());
		out.flip();
	}

	//#####___DONE___

	/**
	 * Return an options object that doesn't do anything
	 * @return a new options object
	 */
	@Override
	public Options createOptions() {
		return new Options() {

			@Override
			public Options setSpeed(SpeedModifier newValue) {
				return this;
			}

			@Override
			public Options setData(DataKind newValue) {
				return this;
			}
		};
	}	

	@Override
	public void reset() {
		// Nothing to do.
	}

	@Override
	public void close() {
		// Nothing to do.
	}

	@Override
	public CompressionKind getKind() {
		return CompressionKind.ZSTD;
	}

}
