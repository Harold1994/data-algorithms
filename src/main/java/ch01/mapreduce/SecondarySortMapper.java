package ch01.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author harold
 * @Title:
 * @Description: map()函数完成解析和词法分析，然后将temperature注入到reducer键中
 * @date 2018/6/7下午1:34
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, DateTemperaturePair, Text> {

    private final Text theTemperature = new Text();
    private final DateTemperaturePair pair = new DateTemperaturePair();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] tokens = line.split(",");
        String yearMonth = tokens[0] + tokens[1];
        String day = tokens[2];
        int temperature = Integer.parseInt(tokens[3]);

        pair.setDay(day);
        pair.setTemperature(temperature);
        pair.setYearMonth(yearMonth);
        theTemperature.set(tokens[3]);

        context.write(pair, theTemperature);
    }
}
