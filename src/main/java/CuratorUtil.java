import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

public class CuratorUtil {
    static private int ZK_CONNECTION_TIMEOUT_MILLIS = 15000;
    static private int ZK_SESSION_TIMEOUT_MILLIS = 60000;
    static private int RETRY_WAIT_MILLIS = 5000;
    static private int MAX_RECONNECT_ATTEMPTS = 3;

    static public CuratorFramework newClient(String zkUrl, int zkPort) {
        CuratorFramework zk = CuratorFrameworkFactory.builder()
                .connectString(zkUrl + ":" + zkPort)
                .sessionTimeoutMs(ZK_SESSION_TIMEOUT_MILLIS)
                .retryPolicy(new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
                .build();
        zk.start();
        return zk;
    }

    static public void mkdir(CuratorFramework zk, String path) throws Exception {
        if(zk.checkExists().forPath(path) == null) {
            try{
                zk.create().creatingParentsIfNeeded().forPath(path);
            }catch (KeeperException.NodeExistsException e){
                //do nothing
            }
        }
    }

    static public void deleteRecursive(CuratorFramework zk, String path) throws Exception {
        if(! zk.getChildren().forPath(path).isEmpty()) {
            for (String child: zk.getChildren().forPath(path)) {
                deleteRecursive(zk, path + "/" + child);
            }
            deleteRecursive(zk, path);
        }else {
//            System.out.println(path);
            zk.delete().forPath(path);
        }
    }

}
