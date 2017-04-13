import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by shihailong on 2017/4/13.
 */
public class SnowflakeUID {
    private static final long EPOCH;   // 时间起始标记点，作为基准，一般取系统的最近时间

    static {
        //2017-01-01 00:00:00 000
        Calendar calendar = Calendar.getInstance(Locale.CHINA);
        calendar.set(Calendar.YEAR, 2017);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        EPOCH = calendar.getTimeInMillis();
    }

    private static final long WORKER_ID_BITS = 10L;      // 机器标识位数
    private static final long WORKER_ID_MASK = ~(-1L << WORKER_ID_BITS);// 机器ID最大值: 1023
    private static final long SEQUENCE_BITS = 12L;      //毫秒内自增位
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;                             // 12
    private static final long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_SHIFT;// 22
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);                 // 4095,111111111111,12位
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final AtomicLong lastTimeSequence = new AtomicLong(getCurrentTimeSequence());
    private long maxTimeExceedMillis = Long.MIN_VALUE;
    private final long workerId;

    public SnowflakeUID(long workerId) {
        this.workerId = workerId;
        if (workerId < 0 || workerId > WORKER_ID_MASK) {
            throw new RuntimeException("workerId (" + workerId + ") mast between 0 and " + WORKER_ID_MASK);
        }
    }

    public long next() {
        final long currentTimeSequence = lastTimeSequence.incrementAndGet();
        long lastTimestamp = (currentTimeSequence >> SEQUENCE_BITS) + EPOCH;
        long currentTimestamp = System.currentTimeMillis();
        if (currentTimestamp > lastTimestamp) {
            /**
             * 当前的时间序列落后了, 重置一下! 使用CAS确保只重置一次.
             */
            lastTimeSequence.compareAndSet(currentTimeSequence, getCurrentTimeSequence());
            return next();
        }
        if (currentTimestamp < lastTimestamp) {
            /**
             * 当前的时间序列超前了. 理论上说,不会超过1ms. 等待一下再返回.
             */
            if (lastTimestamp - currentTimestamp <= getMaxTimeExceedMillis()) {
                /**
                 * 许可内的溢出
                 */
                while (System.currentTimeMillis() < lastTimestamp) {
                    Thread.yield();
                }

            } else {
                throw new RuntimeException("时间回流, 我都不知道怎么工作了....");
            }
        }
        return (currentTimeSequence >> SEQUENCE_BITS) << TIMESTAMP_LEFT_SHIFT | this.workerId << WORKER_ID_SHIFT | (currentTimeSequence & SEQUENCE_MASK);
    }

    /**
     * 获取最大可能的毫秒时间溢出, 通常为1.
     *
     * @return 最大毫秒溢出
     */
    private long getMaxTimeExceedMillis() {
        if (maxTimeExceedMillis != Long.MIN_VALUE) {
            return maxTimeExceedMillis;
        }
        final int threadCount = THREAD_MX_BEAN.getThreadCount();
        if (threadCount < SEQUENCE_MASK) {
            return 1;
        }
        return (threadCount >> SEQUENCE_BITS) << 1;
    }

    /**
     * 设置最大毫秒溢出时间
     *
     * @param maxTimeExceedMillis 最大毫秒溢出时间
     */
    public void setMaxTimeExceedMillis(long maxTimeExceedMillis) {
        this.maxTimeExceedMillis = maxTimeExceedMillis;
    }

    private long getCurrentTimeSequence() {
        return (System.currentTimeMillis() - EPOCH) << SEQUENCE_BITS;
    }

    public String toString(long uid) {
        long timestamp = (uid >> TIMESTAMP_LEFT_SHIFT) + EPOCH;
        long sequence = uid & SEQUENCE_MASK;
        long workerId = (uid & (WORKER_ID_MASK << SEQUENCE_BITS)) >> SEQUENCE_BITS;
        Calendar calendar = Calendar.getInstance(Locale.CHINA);
        calendar.setTimeInMillis(timestamp);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
        return "TIME=[" + simpleDateFormat.format(calendar.getTime()) + "] ID=[" + workerId + "] SEQ=[" + sequence + "]";
    }
}
