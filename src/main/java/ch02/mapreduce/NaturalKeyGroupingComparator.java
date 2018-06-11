package ch02.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author lihe
 * @Title: NaturalKeyGroupingComparator
 * @Description: 分组比较器，在shuffel阶段根据自然键对组合键分组
 * @date 2018/6/8下午4:32
 */
public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey ck1 = (CompositeKey) a;
        CompositeKey ck2 = (CompositeKey) b;
        return ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
    }
}
