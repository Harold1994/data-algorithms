package ch01.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author lihe
 * @Title:
 * @Description: 分组比较器，控制哪些键分组到一个reduce函数调用
 * @date 2018/6/7上午11:33
 */
public class DateTempratureGroupingComparator  extends WritableComparator {
    public DateTempratureGroupingComparator() {
        super(DateTemperaturePair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateTemperaturePair pair = (DateTemperaturePair) a;
        DateTemperaturePair pair2 = (DateTemperaturePair) b;
        return pair.getYearMonth().compareTo(pair2.getYearMonth());
    }
}
