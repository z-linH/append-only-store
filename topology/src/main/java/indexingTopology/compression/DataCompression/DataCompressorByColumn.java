package indexingTopology.compression.DataCompression;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.compression.Compressor;
import indexingTopology.compression.CompressorFactory;

import java.io.*;
import java.text.ParseException;
import java.util.Arrays;


/**
 * Create by zelin on 17-8-26
 **/
public class DataCompressorByColumn {

    private static final int len = 74;

    private byte[] ArrayCombine(byte[] a,byte[] b){
        byte[] bytes = new byte[a.length+b.length];
        bytes = Arrays.copyOf(a , a.length + b.length);
        for(int i = 0; i < b.length; i++){
            bytes[a.length + i] = b[i];
        }
        return bytes;
    }

    public static void main(String[] args) throws IOException, ParseException {
        DataCompressorByColumn dataCompressorByColumn = new DataCompressorByColumn();
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
        System.out.println(stringBuffers[73]);
        DataSchema schema = new DataSchema();
        byte[] bytess = new byte[0];
        for(int i = 0; i < 74; i++){
            if(ColumnDataType.types[i].equals(Long.class.toString())){
                DataTuple dataTuple = schema.parseTupleColumn(stringBuffers[i].toString(), ",", Long.class);
                byte[] bytes = schema.dataColumnSerializeTupe(dataTuple, Long.class);
                bytess = dataCompressorByColumn.ArrayCombine(bytes, bytess);
            }else if(ColumnDataType.types[i].equals(Double.class.toString())) {
                DataTuple dataTuple = schema.parseTupleColumn(stringBuffers[i].toString(), ",", Double.class);
                byte[] bytes = schema.dataColumnSerializeTupe(dataTuple, Double.class);
                bytess = dataCompressorByColumn.ArrayCombine(bytes, bytess);
            }else if(ColumnDataType.types[i].equals(Byte.class.toString())) {
                DataTuple dataTuple = schema.parseTupleColumn(stringBuffers[i].toString(), ",", Byte.class);
                byte[] bytes = schema.dataColumnSerializeTupe(dataTuple, Byte.class);
                bytess = dataCompressorByColumn.ArrayCombine(bytes, bytess);
            }else if(ColumnDataType.types[i].equals(Float.class.toString())) {
                DataTuple dataTuple = schema.parseTupleColumn(stringBuffers[i].toString(), ",", Float.class);
                byte[] bytes = schema.dataColumnSerializeTupe(dataTuple, Float.class);
                bytess = dataCompressorByColumn.ArrayCombine(bytes, bytess);
            }else if(ColumnDataType.types[i].equals(Short.class.toString())) {
                DataTuple dataTuple = schema.parseTupleColumn(stringBuffers[i].toString(), ",", Float.class);
                byte[] bytes = schema.dataColumnSerializeTupe(dataTuple, Float.class);
                bytess = dataCompressorByColumn.ArrayCombine(bytes, bytess);
            }

        }
        System.out.println(ColumnDataType.types[7]);
        System.out.println(bytess.length);
        byte[] compressed = compressor.compress(bytess);
        System.out.println(compressed.length);
        /*for (StringBuffer stringBuffer: stringBuffers) {
            System.out.print(stringBuffer.toString().split(",")[6827] + "   ");
        }*/
    }
}
