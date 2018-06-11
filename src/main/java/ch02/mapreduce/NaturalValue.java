package ch02.mapreduce;


import org.apache.hadoop.io.WritableComparable;
import util.DateUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lihe
 * @Title: NaturalValue
 * @Description: 自然值，这里是（timestamp, price）对
 * @date 2018/6/8下午3:51
 */
public class NaturalValue implements WritableComparable<NaturalValue> {
    private long timestamp;
    private double price;

    public NaturalValue(long timestamp, double price) {
        this.timestamp = timestamp;
        this.price = price;
    }

    public NaturalValue() {
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getPrice() {
        return price;
    }

    public static NaturalValue copy(NaturalValue value) {
        return new NaturalValue(value.timestamp, value.price);
    }

    public NaturalValue clone() {
        return new NaturalValue(this.timestamp, this.price);
    }

    public String getDate() {
        return DateUtil.getDateAsString(this.timestamp);
    }

    public static NaturalValue read(DataInput in) throws IOException {
        NaturalValue value = new NaturalValue();
        value.readFields(in);
        return value;
    }

    @Override
    public int compareTo(NaturalValue o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.timestamp);
        dataOutput.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp = dataInput.readLong();
        this.price = dataInput.readDouble();
    }
}
