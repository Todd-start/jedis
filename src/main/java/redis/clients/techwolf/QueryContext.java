package redis.clients.techwolf;

/**
 * Created by zhaoyalong on 17-3-28.
 */
public class QueryContext {



    public static final int OP_READ = 1 << 0;

    public static final int OP_WRITE = 1 << 2;

    private int operationType;

    public QueryContext(int operationType) {
        this.operationType = operationType;
    }

    public boolean isRead() {
        return operationType == OP_READ;
    }
}
