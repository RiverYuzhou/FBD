import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ActiveMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private Text userId = new Text();
    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.contains("total_purchase_amt") || line.contains("total_redeem_amt")) {
            return; // 跳过含有字段名的行
        }

        String[] fields = line.split(",");  // 假设数据使用逗号分隔
        // 获取 user_id 和 direct_purchase_amt, redeem_amt
        String userIdStr = fields[0];  // user_id
        double directPurchaseAmt = Double.parseDouble(fields[5]);  // direct_purchase_amt
        double totalRedeemAmt = Double.parseDouble(fields[8]);          // total_redeem_amt

        // 用户 ID
        userId.set(userIdStr);

        // 如果 direct_purchase_amt 或 redeem_amt 大于 0，则该用户当天活跃
        if (directPurchaseAmt > 0 || totalRedeemAmt > 0) {
            // 输出用户 ID 和活跃的天数（1天）
            context.write(userId, one);
        }
    }
}
