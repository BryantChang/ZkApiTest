import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorTest {

    public static void main(String[] args) {
        String rootPath = "/bryantchang";
        CuratorFramework zk = CuratorUtil.newClient("centos27", 2181);
        try {
            CuratorUtil.mkdir(zk, rootPath + "/" + "test1");
            CuratorUtil.mkdir(zk, rootPath + "/" + "test2");
            zk.create().creatingParentsIfNeeded().forPath(rootPath + "/test1/lover", "I love Yixi".getBytes());
            System.out.println(new String(zk.getData().forPath(rootPath + "/test1/lover")));
            CuratorUtil.deleteRecursive(zk, rootPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
