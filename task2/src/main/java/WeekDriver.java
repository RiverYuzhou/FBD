import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeekDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WeekdayTransactionAnalysis <input path> <output path>");
            System.exit(-1);
        }

        // 配置和启动 MapReduce 作业
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weekday Transaction Analysis");

        job.setJarByClass(WeekDriver.class);
        job.setMapperClass(WeekMapper.class);
        job.setReducerClass(WeekReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BalanceWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
