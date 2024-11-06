import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class WeekReducer extends Reducer<Text, BalanceWritable, Text, Text> {

    private List<DayRecord> dayRecords = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<BalanceWritable> values, Context context) throws IOException, InterruptedException {
        long totalPurchase = 0L;
        long totalRedeem = 0L;
        int count = 0;

        // 汇总所有记录的资金流入和流出量
        for (BalanceWritable val : values) {
            totalPurchase += val.getPurchaseAmt();
            totalRedeem += val.getRedeemAmt();
            count++;
        }

        // 计算平均值
        long avgPurchase = (count > 0) ? totalPurchase / count : 0;
        long avgRedeem = (count > 0) ? totalRedeem / count : 0;

        // 将当前日期和对应的资金流入、流出数据添加到结果列表中
        dayRecords.add(new DayRecord(key.toString(), avgPurchase, avgRedeem));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 按 avgPurchase 降序排序
        Collections.sort(dayRecords, new Comparator<DayRecord>() {
            @Override
            public int compare(DayRecord o1, DayRecord o2) {
                return Long.compare(o2.getAvgPurchase(), o1.getAvgPurchase());
            }
        });

        // 输出排序后的结果
        for (DayRecord record : dayRecords) {
            context.write(new Text(record.getDay()), new Text(record.getAvgPurchase() + "," + record.getAvgRedeem()));
        }
    }

    // 辅助类，用于存储每一天的统计数据
    static class DayRecord {
        private String day;
        private long avgPurchase;
        private long avgRedeem;

        public DayRecord(String day, long avgPurchase, long avgRedeem) {
            this.day = day;
            this.avgPurchase = avgPurchase;
            this.avgRedeem = avgRedeem;
        }

        public String getDay() {
            return day;
        }

        public long getAvgPurchase() {
            return avgPurchase;
        }

        public long getAvgRedeem() {
            return avgRedeem;
        }
    }
}
