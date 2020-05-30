package data;

import javafx.scene.Node;
import util.NodeAddress;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class LeafNodeInfoStore {
    public Map<byte[], NodeInfo> nodeSet;

    public LeafNodeInfoStore(){
        nodeSet = new HashMap<>();
    }

    public void setNodeInfo(byte[] id, Timestamp timestamp, Map<byte[], NodeAddress> leafSet){
        NodeInfo nodeInfo = new NodeInfo(timestamp,leafSet);
        this.nodeSet.put(id,nodeInfo);
    }

    public Timestamp getTimestamp(byte[] id){
        NodeInfo nodeInfo = this.nodeSet.get(id);
        return nodeInfo.getTimestamp();
    }

    public Map<byte[], NodeAddress> getLeafSet(byte[] id){
        NodeInfo nodeInfo = this.nodeSet.get(id);
        return nodeInfo.getLeafSet();
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

        public Map<byte[], NodeAddress> getLeafSet(){
            return leafSet;
        }
    }
}
