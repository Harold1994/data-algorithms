package ch01.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {
    @Override
    public int getPartition(DateTemperaturePair dateTemperaturePair, Text text, int i) {
        return Math.abs(dateTemperaturePair.getYearMonth().hashCode() % i);
    }
}
