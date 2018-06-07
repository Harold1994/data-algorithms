package ch01.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lihe
 * @Title:
 * @Description: 连接值
 * @date 2018/6/7下午1:44
 */
public class SecondarySortReducer  extends Reducer<DateTemperaturePair, Text, Text, Text> {
    @Override
    protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value.toString());
            builder.append(",");
        }
        context.write(key.getYearMonth(), new Text(builder.toString()));
    }
}
