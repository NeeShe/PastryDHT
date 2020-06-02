package node;

import message.DataTransferMsg;
import message.NodeFailNotifyMsg;
import message.NodeJoinMessage;
import util.NodeAddress;
import util.Util;

import static util.Util.*;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FailureHandler {
    public static final int LEAF_NODE = 0,
                            ROUTE_NODE = 1;
    public static final boolean DISSEMINATE = true,
                                NON_DISSEMINATE = false;
    public int routing_prefix;
    public NodeAddress resultAddr;



    public FailureHandler(){
        this.routing_prefix = -1;
    }

    public NodeAddress getResultAddr() {
        return this.resultAddr;
    }


    public void setRouting_prefix(int prefixLen) {
        this.routing_prefix = prefixLen;
    }

    public void handleFail(PastryNode node,byte[] failNodeId, int nodeType, boolean disseminate){
        switch (nodeType){
            case LEAF_NODE:
                this.handleLeafFail(node,failNodeId,disseminate);
                break;
            case ROUTE_NODE:
                this.handleRouteFailure(node, failNodeId);
                break;
            default:
                System.err.println("Handle fail types");
        }
    }

    //handle failure that didn't realize and still send out request
    //then do leaf failure handler first and then use route failure handler to get data from another node in routing table
    private void handleRouteFailure(PastryNode node, byte[] failNodeId) {
        System.out.println("----------------------------------");

        //remove if found in neighbourSet
        node.neighborhoodSet.removeNode(node,failNodeId);
        System.out.println("removed from neighbor set");
        //remove from routing table
//        node.routingTable.get(node, this.routing_prefix).remove(failNodeId);
        System.out.println("prefix = " + routing_prefix);
        node.routingTable.removeNode(node, convertBytesToHex(failNodeId), this.routing_prefix);
        System.out.println("removed from routing table");

        //resend the message to another node
        //TODO
        //didn't realize the node is failed
        //need to remove the node from routing table
        //and resend the lookup message -> next node in the same row of routing table as this node
        //                              -> the first node in the previous row
        //                              -> the first node in the next row (need to consider coner cases)
        List<Map<String, NodeAddress>> routing_table = node.routingTable.get();
        if(routing_table.get(routing_prefix).size() > 0) {
            for(String resultIdStr : routing_table.get(routing_prefix).keySet()) {
                resultAddr = routing_table.get(routing_prefix).get(resultIdStr);
                break;
            }
            System.out.println("Find new in prefix");

        } else if((routing_prefix > 0) && (routing_table.get(routing_prefix - 1).size() > 0)) {
            for(String resultIdStr : routing_table.get(routing_prefix - 1).keySet()) {
                resultAddr = routing_table.get(routing_prefix - 1).get(resultIdStr);
                break;
            }
            System.out.println("find new in prefix-1");

        } else if((routing_prefix < node.routingTable.tableSize) && (routing_table.get(routing_prefix + 1).size() > 0)) {
            for(String resultIdStr : routing_table.get(routing_prefix + 1).keySet()) {
                resultAddr = routing_table.get(routing_prefix + 1).get(resultIdStr);
                break;
            }
            System.out.println("find new in prefix+1");

        }
    }

    //handle fails that has been detected
    //while non-disseminate - node received the fail notification from other nodes detect that failure
    //while disseminate     - node detect the failure by itself, need to notify the others
    private void handleLeafFail(PastryNode node, byte[] failNodeId,boolean disseminate) {
        //remove if found in neighbourSet
        node.neighborhoodSet.removeNode(node,failNodeId);

        //remove from routing table
        String failNodeIDStr = convertBytesToHex(failNodeId);
        int i=0;
        for(i=0; i<node.routingTable.tableSize; i++) { //find matching characters to find the row
            if(node.idStr.charAt(i) != failNodeIDStr.charAt(i)) {
//                failNodeIDStr = "" + failNodeIDStr.charAt(i);
                break;
            }
        }
        System.out.println("prefix = " + i);
        node.routingTable.removeNode(node,failNodeIDStr, i);

        //remove from leaf set
        if(node.leafSet.leftSet.containsKey(failNodeId) || node.leafSet.rightSet.containsKey(failNodeId)){
            System.out.println("removing from leaf set id = " + failNodeIDStr);
            //remove from leaf set
            node.leafSet.removeNode(node,failNodeId);

            //obtain its leaf set
            Map<byte[], NodeAddress> failNodeLeafSet = node.leafNodeInfoStore.getLeafSet(node,convertBytesToHex(failNodeId));

            //remove node from leaf info node store
            node.leafNodeInfoStore.removeNode(node,convertBytesToHex(failNodeId));

            //send node fail notify message to its leaves
            if(disseminate){
                System.out.println("Disseminate to leaf nodes");
                for(Map.Entry<byte[],NodeAddress> entry: failNodeLeafSet.entrySet()){
                    if(java.util.Arrays.equals(entry.getKey(),node.nodeID) &&
                            java.util.Arrays.equals(entry.getKey(), failNodeId)){
                        this.failNotify(failNodeId,entry.getValue());
                    }
                }
                System.out.println("Send node failed msg to discovery node");
                //notify discovery node about node failed
                NodeFailNotifyMsg nodeFailNotifyMsg = new NodeFailNotifyMsg(failNodeId);
                try {
                    Socket nodeSocket = new Socket(node.discoveryNodeAddress, node.discoveryNodePort);
                    ObjectOutputStream notifyStream = new ObjectOutputStream(nodeSocket.getOutputStream());
                    notifyStream.writeObject(nodeFailNotifyMsg);
                    nodeSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            updateStateTables(node, failNodeId, failNodeLeafSet);
        }
    }

    private void failNotify(byte[] failNodeId,NodeAddress nodeAddress) {
        NodeFailNotifyMsg nodeFailNotifyMsg = new NodeFailNotifyMsg(failNodeId);
        try {
            Socket nodeSocket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());
            ObjectOutputStream notifyStream = new ObjectOutputStream(nodeSocket.getOutputStream());
            notifyStream.writeObject(nodeFailNotifyMsg);
            nodeSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateStateTables(PastryNode node, byte[] failNodeId, Map<byte[], NodeAddress> failNodeLeafSet) {
        //update my state tables
        //do not add to neighbour set
        //add to leaf set
        String newNodeId = "";
        NodeAddress newNodeAddress = null;
        for(Map.Entry<byte[],NodeAddress> entry : failNodeLeafSet.entrySet()){
            //TODO byte[] comparison not always consistent
            if(java.util.Arrays.equals(entry.getKey(),node.nodeID) &&
                    java.util.Arrays.equals(entry.getKey(), failNodeId)){
                boolean leafChanged = node.leafSet.addNewNode(node,entry.getKey(), entry.getValue());
                if(leafChanged){
                    newNodeId = convertBytesToHex(entry.getKey());
                    newNodeAddress=entry.getValue();
                    //TODO Check if update should be outside leafChanged condition
                    //update routing table only based on each entry in leaf set (implies there is path)
                    String nodeIDStr = Util.convertBytesToHex(entry.getKey());
                    int j;
                    for(j=0; j<4; j++) { //find matching characters to find the row
                        if(node.idStr.charAt(j) != nodeIDStr.charAt(j)) {
                            nodeIDStr = "" + nodeIDStr.charAt(j);
                            break;
                        }
                    }
                    node.routingTable.addNewNode(node, nodeIDStr, entry.getValue(), j);
                }
            }
        }

        //handle data the failed node had
        int idInInt = Integer.parseInt(convertBytesToHex(node.nodeID),16);
        int failIdInInt = Integer.parseInt(convertBytesToHex(failNodeId),16);
        if(idInInt < failIdInInt){
            //I was the left node of failed node.
            //move all replicated data to owned
            node.readWriteLock.writeLock().lock();
            node.dataStore.ownedData.putAll(node.dataStore.replicatedData);
            node.dataStore.replicatedData.clear();
            node.readWriteLock.writeLock().unlock();
        }else{
            //do nothing until you find a new left node
        }

        //if new node is added
        if(!newNodeId.equals("")){
            int newNodeIdInInt = Integer.parseInt(newNodeId,16);

            //if the new node is left
            if(newNodeIdInInt < idInInt){
                //transfer all data I own as replica
                DataTransferMsg dataTransferMsg = new DataTransferMsg(node.nodeID,node.address,new HashMap<>(),node.dataStore.ownedData);
                try {
                    Socket nodeSocket = new Socket(newNodeAddress.getInetAddress(), newNodeAddress.getPort());
                    ObjectOutputStream dataTransferStream = new ObjectOutputStream(nodeSocket.getOutputStream());
                    dataTransferStream.writeObject(dataTransferMsg);
                    nodeSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else{
                //do nothing because, even that newNode gets my id and it sends data to store as my replica
            }
        }else{
            //new node not added but I still need to transfer my owned data to my left node if it exists
            try{
                if(node.leafSet.leftSet.size() > 0){
                    byte[] replicaNode = node.leafSet.leftSet.firstKey();
                    NodeAddress replicaNodeAddress = node.leafSet.leftSet.get(replicaNode);

                    System.out.println("Transferring a copy to left leaf node");
                    DataTransferMsg dataTransferMsg = new DataTransferMsg(node.nodeID,node.address,new HashMap<>(),node.dataStore.ownedData);
                    Socket nodeSocket = new Socket(replicaNodeAddress.getInetAddress(), replicaNodeAddress.getPort());
                    ObjectOutputStream dataTransferStream = new ObjectOutputStream(nodeSocket.getOutputStream());
                    dataTransferStream.writeObject(dataTransferMsg);
                    nodeSocket.close();
                }
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
}
