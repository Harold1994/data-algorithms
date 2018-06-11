package ch02.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author lihe
 * @Title: CompositeKey
 * @Description: 组合键定义,在stockSymbol字段上完成一次分组，将相同类型的所有数据分为一组，然后shufful阶段的二次排序，
 * 使用timeStamp分量对数据点排序，使得他们到达reducer时已经分区且有序
 * @date 2018/6/8上午11:34
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
    private String stockSymbol;
    private long timestamp;

    public CompositeKey() {
    }

    public CompositeKey(String stockSymbol, long timestamp) {
        this.stockSymbol = stockSymbol;
        this.timestamp = timestamp;
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public void setStockSymbol(String stockSymbol) {
        this.stockSymbol = stockSymbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(CompositeKey other) {
        if (this.stockSymbol.compareTo(other.stockSymbol) != 0) {
            return this.stockSymbol.compareTo(other.stockSymbol);
        } else if (this.timestamp != other.timestamp) {
            return this.timestamp < other.timestamp ? -1 : 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.stockSymbol);
        dataOutput.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.stockSymbol = dataInput.readUTF();
        this.timestamp = dataInput.readLong();
    }

    //这里的组合键比较器
    public static class CompositeKeyComparator extends WritableComparator {
        public CompositeKeyComparator() {
            super(CompositeKey.class, true);
        }
        //compare方法可以从每个字节数组b1和b2中读取给定起始位置(s1和s2)以及长度l1和l2的一个整数直接进行比较。
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }
    //注册键比较器
    static {
        WritableComparator.define(CompositeKey.class, new CompositeKeyComparator());
    }
}
