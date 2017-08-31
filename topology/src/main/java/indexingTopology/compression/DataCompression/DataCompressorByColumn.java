package indexingTopology.compression.DataCompression;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.compression.Compressor;
import indexingTopology.compression.CompressorFactory;

import java.io.*;
import java.text.ParseException;


/**
 * Create by zelin on 17-8-26
 **/
public class DataCompressorByColumn {

    private static final int len = 74;

    public static void main(String[] args) throws IOException, ParseException {
        DataInputStream in = new DataInputStream(new FileInputStream("/home/hadoop/123/16-2016-05-26--2016-05-28.csv-utf8"));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in,"UTF-8"));
        Compressor compressor = CompressorFactory.compressor(CompressorFactory.Algorithm.GZip);
        String stemp = bufferedReader.readLine();
        StringBuffer[] stringBuffers = new StringBuffer[74];
        for (int i = 0; i < len; i++) {
            stringBuffers[i] = new StringBuffer();
        }
        while((stemp = bufferedReader.readLine()) != null) {
            String str[] = stemp.split(",");
            //System.out.println(str.length);
            for (int i = 0; i < len; i++) {
                stringBuffers[i].append(str[i] + ",");
            }
        }
        System.out.println(stringBuffers[1]);
        DataSchema schema = new DataSchema();
        DataTuple dataTuple = schema.parseTupleColumn(stringBuffers[1].toString(), ",", Float.class);
        byte[] bytes = schema.dataColumnSerializeTupe(dataTuple, Float.class);
        System.out.println(bytes.length);
        byte[] compressed = compressor.compress(bytes);
        System.out.println(compressed.length);
        /*for (StringBuffer stringBuffer: stringBuffers) {
            System.out.print(stringBuffer.toString().split(",")[6827] + "   ");
        }*/
    }
}
