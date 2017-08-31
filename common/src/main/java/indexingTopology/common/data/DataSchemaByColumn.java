package indexingTopology.common.data;

import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Create by zelin on 17-8-26
 **/
public class DataSchemaByColumn implements Serializable{

    private static final int arrayLen = 6827;

    public static class DataType implements Serializable{
        DataType(Class type, int length) {
            this.type = type;
            this.length = length;
        }

        public Class type;
        public int length;
    }

    private final List<String> fieldNames = new ArrayList<>();
    private final List<DataType> dataTypes = new ArrayList<>();


    public void addDoubleField(String name) {
        final DataType dataType = new DataType(Double.class, Double.BYTES);
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addFloatField(String name) {
        final DataType dataType = new DataType(Float.class, Float.BYTES);
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addByteField(String name) {
        final DataType dataType = new DataType(Byte.class, Byte.BYTES);
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addIntField(String name) {
        final DataType dataType = new DataType(Integer.class, Integer.BYTES);
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addShortField(String name) {
        final DataType dataType = new DataType(Short.class, Short.BYTES);
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addField(DataType dataType, String fieldName) {
        fieldNames.add(fieldName);
        dataTypes.add(dataType);
    }

    public void addVarcharField(String name, int length) {
        final DataType dataType = new DataType(String.class, length);
        fieldNames.add(name);
        dataTypes.add(dataType);
    }

    public void addLongField(String name) {
        final DataType dataType = new DataType(Long.class, Long.BYTES);
        fieldNames.add(name);
        dataTypes.add(dataType);
    }


}
