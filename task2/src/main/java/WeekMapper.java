import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class WeekMapper extends Mapper<Object, Text, Text, BalanceWritable> {

    private Text weekdayText = new Text();
    private BalanceWritable balanceWritable = new BalanceWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 输入数据格式： time<TAB>purchaseAmt,redeemAmt
        String[] parts = value.toString().split("\t");  // 按制表符分隔
        if (parts.length < 2) {
            return;  // 如果数据不符合预期格式，跳过该行
        }

        String timestamp = parts[0];  // 时间戳
        String[] amounts = parts[1].split(",");  // 按逗号分隔 purchaseAmt 和 redeemAmt

        if (amounts.length < 2) {
            return;  // 如果资金流入和流出数据格式不正确，跳过该行
        }

        try {
            long purchaseAmt = Long.parseLong(amounts[0].trim());  // 解析 purchaseAmt
            long redeemAmt = Long.parseLong(amounts[1].trim());    // 解析 redeemAmt

            // 获取星期几 (假设时间戳是Unix时间戳)
            String weekday = getWeekdayFromTimestamp(timestamp);

            weekdayText.set(weekday);
            balanceWritable.set(purchaseAmt, redeemAmt);

            context.write(weekdayText, balanceWritable);
        } catch (NumberFormatException e) {
            // 如果解析失败，打印日志并跳过此条数据
            System.err.println("Error parsing amounts: " + parts[1]);
        }
    }

    // 从时间戳获取星期几
    private String getWeekdayFromTimestamp(String timestamp) {
        try{
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            java.util.Calendar calendar = java.util.Calendar.getInstance();
            calendar.setTime(sdf.parse(timestamp));
            int dayOfWeek = calendar.get(java.util.Calendar.DAY_OF_WEEK);
            switch (dayOfWeek) {
                case java.util.Calendar.SUNDAY: return "Sunday";
                case java.util.Calendar.MONDAY: return "Monday";
                case java.util.Calendar.TUESDAY: return "Tuesday";
                case java.util.Calendar.WEDNESDAY: return "Wednesday";
                case java.util.Calendar.THURSDAY: return "Thursday";
                case java.util.Calendar.FRIDAY: return "Friday";
                case java.util.Calendar.SATURDAY: return "Saturday";
                default: return "Unknown";
            }
        }catch(ParseException e){
            return "Invalid date format";
        }
    }
}
