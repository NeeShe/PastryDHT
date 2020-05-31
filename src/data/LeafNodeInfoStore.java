package data;

import node.PastryNode;
import util.NodeAddress;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static util.Util.convertBytesToHex;

public class LeafNodeInfoStore {
    public HashMap<String, NodeInfo> nodeSet;

    public LeafNodeInfoStore(){
        this.nodeSet = new HashMap<>();
    }

    public void setNodeInfo(PastryNode node,String id, Timestamp timestamp, Map<byte[], NodeAddress> leafSet){
        NodeInfo nodeInfo = new NodeInfo(timestamp,leafSet);
        node.leafNodeInfoStore.nodeSet.put(id,nodeInfo);
    }

    public Timestamp getTimestamp(PastryNode node,String id){
        NodeInfo nodeInfo = node.leafNodeInfoStore.nodeSet.get(id);
        return nodeInfo.getTimestamp();
    }

    public Map<byte[], NodeAddress> getLeafSet(PastryNode node, String id){
        node.readWriteLock.writeLock().lock();
        Map<byte[], NodeAddress> leaves=null;
        try{
            NodeInfo nodeInfo = node.leafNodeInfoStore.nodeSet.get(id);
            leaves =nodeInfo.getLeaves();

        }catch (NullPointerException e){
            System.err.println("Null pointer exception in LeafNodeInfoStore");
        }finally {
            node.readWriteLock.writeLock().unlock();
        }
        return leaves;
    }

    public  void removeNode(PastryNode node, String id){
        node.readWriteLock.writeLock().lock();
        try{
            if(node.leafNodeInfoStore.nodeSet.containsKey(id)){
                node.leafNodeInfoStore.nodeSet.remove(id);
            }
        }finally {
            node.readWriteLock.writeLock().unlock();
        }

    }
    public class NodeInfo{
        private Timestamp timestamp;
        private Map<byte[], NodeAddress> leafSet;

        public NodeInfo(Timestamp timestamp, Map<byte[], NodeAddress> leafSet){
            this.timestamp = timestamp;
            this.leafSet=leafSet;
        }

        public Timestamp getTimestamp(){
            return timestamp;
        }

        public Map<byte[], NodeAddress> getLeaves(){
            return leafSet;
        }
    }
}
