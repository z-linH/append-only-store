package indexingTopology.compression.DataCompression;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.compression.CompressorFactory;

import javax.xml.crypto.Data;
import java.io.*;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class Compressor {
    public DataSchema init(){
        DataSchema schema = new DataSchema();
        schema.addDoubleField("data_time");
        schema.addDoubleField("DJClnV");
        schema.addDoubleField("DJClnA");
        schema.addDoubleField("FDJClnA");
        schema.addDoubleField("DJSpeed");
        schema.addDoubleField("DJTemp");
        schema.addDoubleField("DJCTemp");
        schema.addDoubleField("FDJTemp");
        schema.addDoubleField("FDJCTemp");
        schema.addDoubleField("driver_status");
        schema.addDoubleField("gear");
        schema.addDoubleField("neutralg");
        schema.addDoubleField("reverseg");
        schema.addDoubleField("driveg");
        schema.addDoubleField("climbing_mode");
        schema.addDoubleField("power_mode");
        schema.addDoubleField("high_V_switch");
        schema.addDoubleField("capacity_contactor");
        schema.addDoubleField("capacity_charge_switch");
        schema.addDoubleField("battery_contactor");
        schema.addDoubleField("battery_charge_switch");
        schema.addDoubleField("aircon");
        schema.addDoubleField("charge_signal");
        schema.addDoubleField("FDJwork");
        schema.addDoubleField("ebraking");
        schema.addDoubleField("DJworkmode");
        schema.addDoubleField("FDJworkmode");
        schema.addDoubleField("VCU");
        schema.addDoubleField("sys_status");
        schema.addDoubleField("engine_speed");
        schema.addDoubleField("engine_wtemp");
        schema.addDoubleField("engine_load");
        schema.addDoubleField("NSGYW");
        schema.addDoubleField("engine_target_throttle");
        schema.addDoubleField("engine_real_throttle");
        schema.addDoubleField("VCLIFE");
        schema.addDoubleField("tow_pedal");
        schema.addDoubleField("brake_pedal");
        schema.addDoubleField("engine_oil_pressure");
        schema.addDoubleField("engine_air_temp");
        schema.addDoubleField("engine_oil_gas");
        schema.addDoubleField("DCDC_start");
        schema.addDoubleField("DCAC_turn");
        schema.addDoubleField("DCAC_turn_mode");
        schema.addDoubleField("DCAC_air_compressor");
        schema.addDoubleField("a1");
        schema.addDoubleField("a2");
        schema.addDoubleField("a3");
        schema.addDoubleField("a4");
        schema.addDoubleField("a5");
        schema.addDoubleField("a6");
        schema.addDoubleField("a7");
        schema.addDoubleField("a8");
        schema.addDoubleField("version");
        schema.addDoubleField("instant_gas");
        schema.addDoubleField("bi_DCDC_error");
        schema.addDoubleField("DCDC_error");
        schema.addDoubleField("lubro_pump_power_DCAC_error");
        schema.addDoubleField("air_pump_power_DCAC_error");
        schema.addDoubleField("battery_total_V");
        schema.addDoubleField("charge_discharge_A");
        schema.addDoubleField("soc");
        schema.addDoubleField("statusFlag1");
        schema.addDoubleField("statusFlag2");
        schema.addDoubleField("statusFlag3");
        schema.addDoubleField("battery_max_mono_V");
        schema.addDoubleField("battery_max_mono_V_no");
        schema.addDoubleField("battery_max_mono_V_pos");
        schema.addDoubleField("battery_max_temp");
        schema.addDoubleField("battery_max_temp_no");
        schema.addDoubleField("battery_max_temp_pos");
        schema.addDoubleField("battery_min_mono_V");
        schema.addDoubleField("battery_min_mono_V_no");
        schema.addDoubleField("battery_min_mono_V_pos");
        return schema;
    }
    public void Test(){

    }
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public Long DataStringToLong(String str) throws ParseException {
        Date data = sdf.parse(str);
        Long l = data.getTime();
        return l;
    }

    public Integer FloatString1ToInt(String str){
        float fl = Float.parseFloat(str);
        int Scale = 1;
        int roundingMode = 4;
        BigDecimal bd = new BigDecimal(fl);
        bd = bd.setScale(Scale,roundingMode);
        fl = bd.floatValue()*10;
        return (int)fl;
    }

    public Integer FloatString2ToInt(String str){
        float fl = Float.parseFloat(str);
        int Scale = 1;
        int roundingMode = 4;
        BigDecimal bd = new BigDecimal(fl);
        bd = bd.setScale(Scale,roundingMode);
        fl = bd.floatValue()*100;
        return (int)fl;
    }

    public ArrayList<Object> data(String[] str) throws ParseException{
        ArrayList<Object> arrayList = new ArrayList<>();
        arrayList.add(DataStringToLong(str[0]));
        int flag0 = 1,flag1 = 2,flag2 = 5,flag3 = 29,flag4 = 33,flag5 = 34,flag6 = 37,flag7 = 50,flag8 = 51,flag9 = 56,
                flag10 = 57,flag11 = 58,flag12 = 62,flag13 = 68;
        for(int i = 1; i <= 70; i++){
            if(i == flag0 || i == flag1 || i == flag2 || i == flag3 || i == flag4
                    || i == flag5 || i == flag6 || i == flag7 || i == flag8 || i == flag9
                    || i == flag10 || i == flag11 || i == flag12) {
                arrayList.add(FloatString1ToInt(str[i]));
            }else if(i == flag13) {
                arrayList.add(FloatString2ToInt(str[i]));
            }else {
                arrayList.add((int)(Float.parseFloat(str[i])));
            }

        }
        return arrayList;
    }

    private byte[] ArrayCombine(byte[] a,byte[] b){
        byte[] bytes = new byte[a.length+b.length];
        bytes = Arrays.copyOf(a , a.length + b.length);
        for(int i = 0; i < b.length; i++){
            bytes[a.length + i] = b[i];
        }
        return bytes;
    }

    public static void main(String[] args) {
        Compressor compressor = new Compressor();
        DataSchema dataSchema = compressor.init();
        byte[] bytess = new byte[0];
        try {
            DataInputStream in = new DataInputStream(new FileInputStream(new File("/home/hadoop/123/16-2016-05-26--2016-05-28.csv-utf8")));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in,"UTF-8"));
            String stemp;
            stemp = bufferedReader.readLine();
            //System.out.println(stemp);
            while((stemp = bufferedReader.readLine()) != null){
                DataTuple dataTuple = dataSchema.parseTuple(stemp,",");
                byte[] bytes = dataSchema.serializeTuple(dataTuple);
                bytess = compressor.ArrayCombine(bytes, bytess);
            }
            System.out.println(bytess.length);
//            String str[] = stemp.split(",");
//            ArrayList<Object> array = compressor.data(str);
//            for(int i = 0; i <= 70; i++){
//                dataTuple.add(array.get(i));
//            }
//            DataSchema schema = compressor.init();

            /*for (byte i : bytes) {
                System.out.print(i + ",");
            }*/

            indexingTopology.compression.Compressor compressor1 = CompressorFactory.compressor(CompressorFactory.Algorithm.Snappy);
            byte[] compressed = compressor1.compress(bytess);
            System.out.println(compressed.length);
            //System.out.println(bytes[1]);
            /*while((stemp =bufferedReader.readLine()) != null){
                //System.out.println(stemp);
                String str[] = stemp.split(",");


            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
