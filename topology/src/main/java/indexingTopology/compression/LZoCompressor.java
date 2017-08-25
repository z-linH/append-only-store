package indexingTopology.compression;

import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoCompressor;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzoOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class LZoCompressor implements Compressor{
    @Override
    public byte[] compress(byte[] data) throws IOException {
        LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(
                LzoAlgorithm.LZO1X, null);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        LzoOutputStream cs = new LzoOutputStream(os, compressor);
        cs.write(data);
        cs.close();

        return os.toByteArray();
    }
}
