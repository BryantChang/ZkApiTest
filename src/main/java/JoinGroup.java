import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;

public class JoinGroup extends ConnectionWatcher{

    private static String host = "centos25:2181";

    public void join(String groupName, String memberName) throws KeeperException, InterruptedException {
        String path = "/" + groupName ;
        String createPath = zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Created:" + createPath);
    }

    public static void main(String[] args) {
        JoinGroup instance = new JoinGroup();
        try {
            instance.connect(host);
            instance.join(args[0], args[1]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
