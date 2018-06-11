package ch02.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.DateUtil;

import java.io.IOException;

/**
 * @author lihe
 * @Title:
 * @Description: Reducer
 * @date 2018/6/8下午4:52
 */
public class SecondarySortReducer extends Reducer<CompositeKey, NaturalValue, Text, Text> {
    @Override
    protected void reduce(CompositeKey key, Iterable<NaturalValue> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (NaturalValue data : values) {
            builder.append("(");
            String dateAsString = DateUtil.getDateAsString(data.getTimestamp());
            double price = data.getPrice();
            builder.append(dateAsString);
            builder.append(",");
            builder.append(price);
            builder.append(")");
        }
        context.write(new Text(key.getStockSymbol()), new Text(builder.toString()));
    }
}
