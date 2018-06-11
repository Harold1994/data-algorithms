package ch02.mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author lihe
 * @Title: NatureKeyPartitioner
 * @Description: 自然键分区器,在shufful之前执行分区
 * @date 2018/6/8下午3:46
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, NaturalValue> {
    @Override
    public int getPartition(CompositeKey compositeKey, NaturalValue naturalValue, int numberOfPartitions) {
        return Math.abs((int) (hash(compositeKey.getStockSymbol()) % numberOfPartitions));
    }

    static long hash(String str) {
        long h = 1125899906842597L; // prime
        int length = str.length();
        for (int i = 0; i < length; i++ ) {
            h = 31*h + str.charAt(i);
        }
        return h;
    }
}
