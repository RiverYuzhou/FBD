import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BalanceReducer extends Reducer<Text, BalanceWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<BalanceWritable> values, Context context) throws IOException, InterruptedException {
        long totalIn = 0L;
        long totalOut = 0L;

        // 聚合资金流入和流出
        for (BalanceWritable balance : values) {
            totalIn += balance.getPurchaseAmt();
            totalOut += balance.getRedeemAmt();
        }

        // 输出格式: <日期> TAB <资金流入量>,<资金流出量>
        context.write(key, new Text(totalIn + "," + totalOut));
    }
}
