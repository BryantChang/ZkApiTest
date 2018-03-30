import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.*;

public class ZookeeperTest implements Watcher{
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    public static void main(String[] args) throws IOException {
        ZooKeeper zk = new ZooKeeper("centos25:2181", 5000, new ZookeeperTest());
        System.out.println(zk.getState());

        try {
            connectedSemaphore.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("ZooKeeper session Created");
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println("Recieve the Event:" + watchedEvent);
        if(Event.KeeperState.SyncConnected == watchedEvent.getState()){
            connectedSemaphore.countDown();
        }
    }
}
