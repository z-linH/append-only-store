package indexingTopology.compression.DataCompression;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.compression.Compressor;
import indexingTopology.compression.CompressorFactory;

import java.io.*;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class DataCompressor {
    public DataSchema init(){
        DataSchema schema = new DataSchema();
        schema.addFloatField("data_time");
        schema.addFloatField("DJClnV");
        schema.addFloatField("DJClnA");
        schema.addFloatField("FDJClnA");
        schema.addFloatField("DJSpeed");
        schema.addByteField("DJTemp");
        schema.addByteField("DJCTemp");
        schema.addByteField("FDJTemp");
        schema.addByteField("FDJCTemp");
        schema.addByteField("driver_status");
        schema.addByteField("gear");
        schema.addByteField("neutralg");
        schema.addByteField("reverseg");
        schema.addByteField("driveg");
        schema.addByteField("climbing_mode");
        schema.addByteField("power_mode");
        schema.addByteField("high_V_switch");
        schema.addByteField("capacity_contactor");
        schema.addByteField("capacity_charge_switch");
        schema.addByteField("battery_contactor");
        schema.addByteField("battery_charge_switch");
        schema.addByteField("aircon");
        schema.addByteField("charge_signal");
        schema.addByteField("FDJwork");
        schema.addByteField("ebraking");
        schema.addByteField("DJworkmode");
        schema.addByteField("FDJworkmode");
        schema.addByteField("VCU");
        schema.addShortField("sys_status");
        schema.addFloatField("engine_speed");
        schema.addByteField("engine_wtemp");
        schema.addByteField("engine_load");
        schema.addFloatField("NSGYW");
        schema.addFloatField("engine_target_throttle");
        schema.addFloatField("engine_real_throttle");
        schema.addShortField("VCLIFE");
        schema.addFloatField("tow_pedal");
        schema.addFloatField("brake_pedal");
        schema.addShortField("engine_oil_pressure");
        schema.addByteField("engine_air_temp");
        schema.addFloatField("engine_oil_gas");
        schema.addShortField("DCDC_start");
        schema.addShortField("DCAC_turn");
        schema.addShortField("DCAC_turn_mode");
        schema.addShortField("DCAC_air_compressor");
        schema.addIntField("a1");
        schema.addIntField("a2");
        schema.addIntField("a3");
        schema.addShortField("a4");
        schema.addByteField("a5");
        schema.addShortField("a6");
        schema.addShortField("a7");
        schema.addFloatField("a8");
        schema.addFloatField("version");
        schema.addFloatField("instant_gas");
        schema.addShortField("bi_DCDC_error");
        schema.addShortField("DCDC_error");
        schema.addShortField("lubro_pump_power_DCAC_error");
        schema.addShortField("air_pump_power_DCAC_error");
        schema.addFloatField("battery_total_V");
        schema.addFloatField("charge_discharge_A");
        schema.addFloatField("soc");
        schema.addByteField("statusFlag1");
        schema.addByteField("statusFlag2");
        schema.addByteField("statusFlag3");
        schema.addFloatField("battery_max_mono_V");
        schema.addByteField("battery_max_mono_V_no");
        schema.addByteField("battery_max_mono_V_pos");
        schema.addByteField("battery_max_temp");
        schema.addByteField("battery_max_temp_no");
        schema.addByteField("battery_max_temp_pos");
        schema.addFloatField("battery_min_mono_V");
        schema.addByteField("battery_min_mono_V_no");
        schema.addByteField("battery_min_mono_V_pos");
        return schema;
    }



    private byte[] ArrayCombine(byte[] a,byte[] b){
        byte[] bytes = new byte[a.length+b.length];
        bytes = Arrays.copyOf(a , a.length + b.length);
        for(int i = 0; i < b.length; i++){
            bytes[a.length + i] = b[i];
        }
        return bytes;
    }

    private BufferedReader getBufferReader() throws FileNotFoundException, UnsupportedEncodingException {
        DataInputStream in = new DataInputStream(new FileInputStream(new File("/home/hadoop/123/16-2016-05-26--2016-05-28.csv-utf8")));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        return  bufferedReader;
    }

    private static int len = 0;

    public static void main(String[] args) throws IOException, ParseException {
        DataCompressor datacompressor = new DataCompressor();
        DataSchema dataSchema = datacompressor.init();
        String stemp;
        int sumByte = 0,sumCom = 0,sumTime = 0;
        byte[] bytess = new byte[0];

        int count = 0, nums = 6000;
            try {
                BufferedReader bufferedReader = datacompressor.getBufferReader();

                stemp = bufferedReader.readLine();
                System.out.println(stemp);
                Compressor compressor = CompressorFactory.compressor(CompressorFactory.Algorithm.GZip);
                //System.out.println(stemp);
                long start, end;
                while (true) {

                    while ((stemp = bufferedReader.readLine()) != null) {
                        if (count++ == nums) break;
                        DataTuple dataTuple = dataSchema.parseTuple(stemp, ",");
                        byte[] bytes = dataSchema.serializeTuple(dataTuple);
                        bytess = datacompressor.ArrayCombine(bytes, bytess);
                    }
                    start = System.currentTimeMillis();
                    byte[] compressed = compressor.compress(bytess);
                    end = System.currentTimeMillis();
                    long time = end - start;
                    System.out.print("[" + bytess.length + ", " + compressed.length + "]" );
                    sumByte += bytess.length;
                    sumCom += compressed.length;
                    sumTime += time;
                    len++;
                    System.out.print ( "    ");
                    if (stemp == null) break;
                    count = 0;
                    bytess = new byte[0];
                }


                System.out.println();
                System.out.println("[" + sumByte/len + ", " + sumCom/len + "]" + ((sumByte/len)/(sumCom/len)) + "/" + sumTime + "/" + sumTime/((sumByte/len)/(sumCom/len)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

}
