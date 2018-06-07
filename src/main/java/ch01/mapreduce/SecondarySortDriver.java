package ch01.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * @author lihe
 * @Title:
 * @Description: 驱动器
 * @date 2018/6/7下午1:54
 */
public class SecondarySortDriver extends Configured implements Tool {

    private static Logger theLogger = Logger.getLogger(SecondarySortDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("SecondarySort");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(DateTemperaturePair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTempratureGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        theLogger.info("run: status" + status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            theLogger.warn("SecondarySortDriver <input-dir> <output-dir>");
            throw new IllegalArgumentException("SecondarySortDriver <input-dir> <output-dir>");
        }

        int returnStatus = ToolRunner.run(new SecondarySortDriver(), args);
        theLogger.info("returnStatus=" + returnStatus);
        System.exit(returnStatus);
    }
}
