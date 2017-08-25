package indexingTopology.compression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class DeflateDecompressor implements Decompressor{

    Inflater decompressor;

    public DeflateDecompressor(Inflater decompressor) {
        this.decompressor = decompressor;
    }

    @Override
    public byte[] decompress(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            decompressor.setInput(data);
            final byte[] buf = new byte[data.length];
            while (!decompressor.finished()) {
                int count = decompressor.inflate(buf);
                bos.write(buf, 0, count);
            }
        } catch (DataFormatException e) {
            e.printStackTrace();
        } finally {
            decompressor.end();
        }
        return bos.toByteArray();
    }
}
