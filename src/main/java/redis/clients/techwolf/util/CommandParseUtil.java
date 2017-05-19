package redis.clients.techwolf.util;

/**
 * Created by zhaoyalong on 17-5-19.
 */
public class CommandParseUtil {

    /**
     * [0]nodeId
     * [1]ip:port
     * [2]role
     * [3]masterNodeId
     * [4]ping time
     * [5]ping time
     * [6]epoch
     * [7]conn status
     * @param str
     * @return
     */
    public static String[] paresClusterNodes(String str){
        return str.split("\\s");
    }

    public static void main(String[] args) {
        String[] a = paresClusterNodes("6cc6413be149b43c05ef8e6b923a5449e515aa03 172.16.0.34:7001 slave 9d7e5e6c88d47474f0a4feda1dd74581d4d1e25a 0 1495193594636 14 connected");
        System.out.println(a[0]);
    }
}
