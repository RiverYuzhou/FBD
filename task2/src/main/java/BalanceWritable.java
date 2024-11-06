import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BalanceWritable implements Writable {
    private long purchaseAmt;  // 资金流入量
    private long redeemAmt;    // 资金流出量

    // 默认构造函数
    public BalanceWritable() {
        this.purchaseAmt = 0L;
        this.redeemAmt = 0L;
    }

    // 构造函数
    public BalanceWritable(long purchaseAmt, long redeemAmt) {
        this.purchaseAmt = purchaseAmt;
        this.redeemAmt = redeemAmt;
    }

    public long getPurchaseAmt() {
        return purchaseAmt;
    }

    public long getRedeemAmt() {
        return redeemAmt;
    }

    public void set(long purchaseAmt, long redeemAmt) {
        this.purchaseAmt = purchaseAmt;
        this.redeemAmt = redeemAmt;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(purchaseAmt);
        out.writeLong(redeemAmt);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.purchaseAmt = in.readLong();
        this.redeemAmt = in.readLong();
    }

    @Override
    public String toString() {
        return purchaseAmt + "," + redeemAmt;
    }
}
