package ch03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sun.jvm.hotspot.debugger.MachineDescriptionPPC;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author lihe
 * @Title: TopNMapper
 * @Description: 唯一键Mapper
 * @date 2018/6/11下午2:19
 */
public class TopNMapper extends Mapper<Text, IntWritable, NullWritable, Text> {
    private int N = 10;
    private SortedMap<Integer, String> top = new TreeMap<Integer, String>();

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String keyAsString = key.toString();
        int frequency = value.get();
        String compositValue = keyAsString + "," + frequency;
        top.put(frequency, compositValue);
        if (top.size() > N) {
            top.remove(top.firstKey());
        }
    }
    //每个mapper执行一次,一开始从参数中获取N
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.N = context.getConfiguration().getInt("N", 10);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String str : top.values())
            context.write(NullWritable.get(), new Text(str));
    }
}
