package indexingTopology.compression;

import net.jpountz.lz4.LZ4Factory;

import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Created by robert on 26/7/17.
 */
public class CompressorFactory {

    public enum Algorithm {LZ4, Snappy, Deflate, GZip, BZip2, Lzo}
    static public Compressor compressor(Algorithm algorithm) {
        switch (algorithm) {
            case LZ4:
                return new Lz4Compressor(LZ4Factory.fastestInstance().fastCompressor());
            case Snappy:
                return new SnappyCompressor();
            case Deflate:
                return new DeflateCompressor(new Deflater(1));
            case GZip:
                return new GZipCompressor();
            case BZip2:
                return new BZipCompressor();
            case Lzo:
                return new LZoCompressor();
            default:
                return null;
        }
    }

    static public Decompressor decompressor(Algorithm algorithm) {
        switch (algorithm) {
            case LZ4:
                return new Lz4Decompressor(LZ4Factory.fastestInstance().safeDecompressor());
            case Snappy:
                return new SnappyDecompressor();
            case Deflate:
                return new DeflateDecompressor(new Inflater());
            case GZip:
                return new GZipDecompressor();
            case BZip2:
                return new BZipDecompressor();
            case Lzo:
                return new LZoDecompressor();
            default:
                return null;
        }
    }

}
