package node;

import message.DataTransferMsg;
import message.NodeFailNotifyMsg;
import util.NodeAddress;
import util.Util;

import static util.Util.*;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class FailureHandler {
    public static final int LEAF_NODE = 0,
                            ROUTE_NODE = 1,
                            NEIGHBOR_NODE = 2;
    public static final boolean DISSEMINATE = true,
    NON_DISSEMINATE = false;



    FailureHandler(){
    }

    public void handleFail(PastryNode node,byte[] nodeId, int nodeType, boolean disseminate){
        switch (nodeType){
            case LEAF_NODE: this.handleLeafFail(node,nodeId,disseminate);
                            break;
            default:System.err.println("Handle fail types");
        }
    }

    private void handleLeafFail(PastryNode node,byte[] failNodeId,boolean disseminate) {
        //remove if found in neighbourSet
        node.neighborhoodSet.removeNode(node,failNodeId);

        //remove from routing table
        String failNodeIDStr = convertBytesToHex(failNodeId);
        int i=0;
        for(i=0; i<4; i++) { //find matching characters to find the row
            if(node.idStr.charAt(i) != failNodeIDStr.charAt(i)) {
                failNodeIDStr = "" + failNodeIDStr.charAt(i);
                break;
            }
        }
        node.routingTable.removeNode(node,failNodeIDStr, i);

        //remove from leaf set
        if(node.leafSet.leftSet.containsKey(failNodeId) || node.leafSet.rightSet.containsKey(failNodeId)){

            //remove from leaf set
            node.leafSet.removeNode(node,failNodeId);

            //obtain its leaf set
            Map<byte[], NodeAddress> failNodeLeafSet =node.leafNodeInfoStore.getLeafSet(node,convertBytesToHex(failNodeId));

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

            //update my state tables
            //do not add to neighbour set
            //add to leaf set
            String newNodeId="";
            NodeAddress newNodeAddress=null;
            for(Map.Entry<byte[],NodeAddress> entry: failNodeLeafSet.entrySet()){
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
}
