import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class ActiveReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // 使用 TreeMap 存储用户及其活跃天数
    private TreeMap<Integer, Text> activeDaysMap = new TreeMap<>(Collections.reverseOrder());

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int activeDays = 0;

        // 对每个用户的活跃天数进行累加
        for (IntWritable val : values) {
            activeDays += val.get();  // 每次遇到活跃记录，就加1
        }

        // 将用户和活跃天数存入 TreeMap
        activeDaysMap.put(activeDays, new Text(key)); // 使用活跃天数作为键
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 输出排序后的结果
        for (Map.Entry<Integer, Text> entry : activeDaysMap.entrySet()) {
            // 拼接输出格式为 user_id<tab>activeDays
            String outputKey = entry.getValue().toString() + "\t"; // 用户 ID 后加制表符
            context.write(new Text(outputKey), new IntWritable(entry.getKey()));
        }
    }
}
