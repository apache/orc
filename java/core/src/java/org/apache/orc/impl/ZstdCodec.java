package org.apache.orc.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

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
			
			byte[] compressed = new byte[Zstd.blockSizeMax()];
			byte[] toCompress = new byte[in.remaining()];
			
			System.arraycopy(in.array(), in.arrayOffset() + in.position(), toCompress, 0, toCompress.length);
			
			int outBytes = (int) Zstd.compress(compressed, toCompress, DEFAULT_LEVEL);
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
		int read = -1;
		try (ZstdInputStream zis = new ZstdInputStream(new ByteArrayInputStream(in.array(), in.arrayOffset() + in.position(), in.remaining()))) {
			while ((read = zis.read(buf)) != -1) {
				System.arraycopy(buf, 0, out.array(), out.arrayOffset() + out.position(), read);
				out.position(out.position() + read);
			}
		}

		out.flip();
	}

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
