import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BalanceWritable implements Writable {
    private long purchaseAmt;  // 修改为 long 类型
    private long redeemAmt;    // 修改为 long 类型

    // 无参构造器
    public BalanceWritable() {
        this.purchaseAmt = 0L;  // 初始化为 0L（long 类型）
        this.redeemAmt = 0L;    // 初始化为 0L（long 类型）
    }

    // 带参构造器
    public BalanceWritable(long purchaseAmt, long redeemAmt) {
        this.purchaseAmt = purchaseAmt;
        this.redeemAmt = redeemAmt;
    }

    // Getter 方法
    public long getPurchaseAmt() {
        return purchaseAmt;
    }

    public long getRedeemAmt() {
        return redeemAmt;
    }

    // 写入数据
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(purchaseAmt);  // 使用 writeLong 方法
        out.writeLong(redeemAmt);    // 使用 writeLong 方法
    }

    // 读取数据
    @Override
    public void readFields(DataInput in) throws IOException {
        purchaseAmt = in.readLong();  // 使用 readLong 方法
        redeemAmt = in.readLong();    // 使用 readLong 方法
    }
}
