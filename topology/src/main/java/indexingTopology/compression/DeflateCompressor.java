package indexingTopology.compression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;

public class DeflateCompressor implements Compressor{

    Deflater compressor;

    public DeflateCompressor(Deflater deflater) {
        this.compressor = deflater;
    }

    @Override
    public byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            compressor.setInput(data);
            compressor.finish();
            final byte[] buf = new byte[data.length];
            while (!compressor.finished()) {
                int count = compressor.deflate(buf);
                bos.write(buf, 0, count);
            }
        } finally {
            compressor.end();
        }
        return bos.toByteArray();
    }
}
