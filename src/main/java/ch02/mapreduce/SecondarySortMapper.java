package ch02.mapreduce;

import com.sun.jersey.api.ParamException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import util.DateUtil;

import java.io.IOException;
import java.util.Date;

/**
 * @author lihe
 * @Title:
 * @Description: mapper
 * @date 2018/6/8下午4:39
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, NaturalValue> {
    private final CompositeKey reduceKey = new CompositeKey();
    private final NaturalValue reduceValue = new NaturalValue();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] tokens = StringUtils.split(value.toString().trim(),',');
        if (tokens.length == 3) {
            Date date = DateUtil.getDate(tokens[1]);
            if (date == null) {
                return;
            }
            long timestamp = date.getTime();
            reduceKey.setStockSymbol(tokens[0]);
            reduceKey.setTimestamp(timestamp);
            reduceValue.setTimestamp(timestamp);
            reduceValue.setPrice(Double.parseDouble(tokens[2]));
            context.write(reduceKey, reduceValue);
        }
        else {
            // ignore the entry or log as error, not enough tokens
        }
    }
}
