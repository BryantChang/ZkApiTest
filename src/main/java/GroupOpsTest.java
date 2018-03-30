import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupOpsTest extends ConnectionWatcher{

    private String host = "";

    public GroupOpsTest(String host) {
        this.host = host;
    }

    /**
     * 初始化特定的组，自动创建跟节点(永久节点)
     * @param groupName 组名
     * @return 改组的zk路径
     */
    private String init(String groupName) {
        String groupPath = "/" + groupName;
        try {
            Stat exist = null;
            exist = zk.exists(groupPath, false);
            if(null == exist) {
                groupPath = zk.create(groupPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("init succ!!");
        return groupPath;
    }

    /**
     * 加入特定的组
     * @param groupName 组名
     * @param memberName 成员名称
     */
    public void joinGroup(String groupName, String memberName)  {
        String absPath = this.init(groupName) + "/" + memberName;
        try {
            String createPath = zk.create(absPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("Created:" + createPath);
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    /**
     * 获取某个组中的全部成员
     * @param groupName 组名
     */
    public void listGroup(String groupName) {
        String groupPath = "/" + groupName;
        try {
            System.out.println("=====The members of the group " + groupPath + "=====");
           List<String> children = zk.getChildren(groupPath, false);
           if(children.isEmpty()) {
               System.out.println("No members in group " + groupPath);
               System.exit(1);
           }
            for (String child: children) {
                System.out.println(child);
            }
        }catch(KeeperException.NoNodeException e){
            System.out.println("Group " + groupPath + " not exist");
            System.exit(1);
        }catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 判断某个成员是否存在
     * @param groupName 组名
     * @param memberName 成员名
     */
    public void memberExists(String groupName, String memberName) {

        String nodePath = "";
        if(memberName.equals("")) {
            nodePath = "/" + groupName;
        }else {
            nodePath = "/" + groupName + "/" + memberName;
        }
        try {
            Stat status = zk.exists(nodePath, false);
            if(null != status) {
                System.out.println(nodePath + " exists");
            }else {
                System.out.println(nodePath + " not exists!!");
            }
        } catch (KeeperException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 删除某个成员
     * @param groupName 组名
     * @param memberName 成员名
     */
    public void deleteNode(String groupName, String memberName) {
        String nodePath = "/" + groupName + "/" + memberName;
        try {
            zk.delete(nodePath, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (KeeperException e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("delete " + nodePath + " succ!!");
    }

    /**
     * 删除整个组
     * @param groupName 组名
     */
    public void deleteGroup(String groupName) {
        String groupPath = "/" + groupName;
        try {
            List<String> children = zk.getChildren(groupPath, false);
            for (String child: children){
               this.deleteNode(groupName, child);
            }
            zk.delete(groupPath, -1);
        } catch (KeeperException.NoNodeException e) {
            System.out.println("group " + groupPath + " not exist");
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.out.println("delete group " + groupPath + " succ!!");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String host = "centos25:2181";
        GroupOpsTest instance = new GroupOpsTest(host);
        instance.connect(host);
        String groupName = "bryantchang";
        for (int i = 0; i < 10; i++) {
            String memberName = "member" + (i+1);
            instance.joinGroup(groupName, memberName);
        }
        instance.listGroup(groupName);
        instance.memberExists(groupName, "member2");
        instance.deleteNode(groupName, "member2");
        instance.listGroup(groupName);
        instance.deleteGroup(groupName);
        instance.memberExists(groupName, "");
        try {
            instance.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
