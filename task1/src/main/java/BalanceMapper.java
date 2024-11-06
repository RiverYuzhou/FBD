import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BalanceMapper extends Mapper<LongWritable, Text, Text, BalanceWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取当前行的文本内容
        String line = value.toString();

        // 如果行是表头（如字段名），则跳过
        if (line.contains("total_purchase_amt") || line.contains("total_redeem_amt")) {
            return; // 跳过含有字段名的行
        }

        String[] fields = line.split(",");  // 数据使用逗号分隔

        // 提取所需字段
        String reportDate = fields[1];  // report_date
        long totalPurchaseAmt = parseLong(fields[4]);  // total_purchase_amt
        long totalRedeemAmt = parseLong(fields[8]);  // total_redeem_amt

        // 将结果写入上下文
        context.write(new Text(reportDate), new BalanceWritable(totalPurchaseAmt, totalRedeemAmt));
    }

    // 辅助方法，用于安全解析长整型值
    private long parseLong(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0L;  // 如果字段为空，返回 0
        }

        try {
            return Long.parseLong(value);  // 尝试转换为 long
        } catch (NumberFormatException e) {
            return 0L;  // 如果转换失败，返回 0
        }
    }
}
