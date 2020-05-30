package node;

import data.LeafNodeInfoStore;
import message.*;
import util.NodeAddress;
import util.Util;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import data.LeafNodeInfoStore.NodeInfo;
import static util.Util.*;

public class PastryNodeWorker extends Thread{
    protected Socket socket;
    protected PastryNode node;
    protected Message replyMsg;

    public PastryNodeWorker(Socket socket, PastryNode node) {
        this.socket  = socket;
        this.node = node;
        this.replyMsg = null;
    }

    @Override
    public void run() {
        try {
            //read request message
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            Message requestMsg = (Message) in.readObject();

            Message replyMsg = null;
            switch(requestMsg.getMsgType()) {
                case Message.NODE_JOIN_MSG:
                    this.nodeJoinHandler(requestMsg);
                    break;
                case Message.ROUTING_INFO_MSG:
                    this.routingInfoHandler(requestMsg);
                    break;
                case Message.LOOKUP_NODE_MSG:
                    this.lookupNodeHandler(requestMsg);
                    break;
                case Message.READ_DATA_MSG:
                    this.readDataHandler(requestMsg);
                    break;
                case Message.WRITE_DATA_MSG:
                    this.writeDataHandler(requestMsg);
                    break;
                case Message.DATA_TRANSFER_MSG:
                    this.dataTransferHandler(requestMsg);
                    break;
                case Message.REQUEST_DATA_MSG:
                    this.dataRequestHandler(requestMsg);
                    break;
                case Message.KEEP_ALIVE_MSG:
                    this.keepAliveMsgHandler(requestMsg);
                    break;
                default:
                    System.err.println("Unrecognized request message type '" + requestMsg.getMsgType() + "'");
                    break;
            }

        } catch(Exception e) {
            e.printStackTrace();
            System.err.println(e.getMessage());
        } finally {
            if(replyMsg != null) {
                try {
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(replyMsg);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private void nodeJoinHandler(Message requestMsg) {
        try{
            NodeJoinMessage nodeJoinMsg = (NodeJoinMessage) requestMsg;
            if(nodeJoinMsg.getNodeAddress().getInetAddress() == null) {
                nodeJoinMsg.getNodeAddress().setInetAddress(socket.getInetAddress());
            }
            System.out.println("Received node join request from '" + nodeJoinMsg.toString() + "'.");
            int p = nodeJoinMsg.getPrefixLength();
            System.out.println("Search for an exact match in the Routing table");
            //search for an exact match in the routing table
            NodeAddress nodeAddress = this.node.routingTable.searchExact(this.node, nodeJoinMsg.getID(), p);
            if(nodeAddress != null) {
                //update longest prefix match if we have indeed found a match
                nodeJoinMsg.setLongestPrefixMatch((nodeJoinMsg.getPrefixLength() + 1));
                System.out.println("Current nodeJoinMsg : " + nodeJoinMsg.toString());
            }

            //search for closest node in routing table
            if(nodeAddress == null) {
                System.out.println("Search for closest node in routing table");
                nodeAddress = this.node.routingTable.searchClosest(this.node, nodeJoinMsg.getID(), p);
            }


            //find closest node in leaf set
            if(nodeAddress == null || nodeJoinMsg.hopContains(nodeAddress)) {
                System.out.println("Find closest node in leaf set");
                nodeAddress = this.node.leafSet.searchClosest(this.node, nodeJoinMsg.getID());
            }

            System.out.println("Send routing information to joining node");
            //send routing information to joining node
            Socket joinNodeSocket = null;
            joinNodeSocket = new Socket(nodeJoinMsg.getNodeAddress().getInetAddress(), nodeJoinMsg.getNodeAddress().getPort());
            ObjectOutputStream joinNodeOut = new ObjectOutputStream(joinNodeSocket.getOutputStream());
            joinNodeOut.writeObject(
                        new RoutingInfoMsg(this.node.leafSet.get(this.node), this.node.neighborhoodSet.get(this.node),p, this.node.routingTable.get(this.node, p),
                                nodeAddress.getInetAddress() == null
                                //last routing info message received by the joining node
                                //notice that broadcastMsg will be true for only one node which is closest
                        )
                );
                joinNodeSocket.close();
                //neetha: add it to neighborset if the node contacted this node first
                if(nodeJoinMsg.getHopsSize() == 1){
                    this.node.neighborhoodSet.addNewNode(node,nodeJoinMsg.getID(),nodeJoinMsg.getNodeAddress());
                }

                //neetha: after fixing searchClosest in leafset and routing table, unexpected entry inside this if condition is resolved
                //without that fix NodeC was forwarding message to NodeC
                //if we found a closer node forward the node join message
                if(nodeAddress.getInetAddress() != null) {
                    System.out.println("Forwarding node join message with id '" + convertBytesToHex(nodeJoinMsg.getID()) + "' to '" + nodeAddress + "'");
                    nodeJoinMsg.addHop(nodeAddress);
                    Socket nodeSocket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());
                    ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
                    nodeOut.writeObject(nodeJoinMsg);
                    nodeSocket.close();
                }
        }catch(Exception e){
            System.out.println("DEBUG: Exception area 1");
            e.printStackTrace();
        }
    }
    //method to handle incoming routing information message
    private void routingInfoHandler(Message requestMsg) {
        RoutingInfoMsg routingInfoMsg = (RoutingInfoMsg) requestMsg;
        System.out.println("Received routing info message with " + routingInfoMsg.getLeafSet().size() + " routes.");
        boolean changed = false;

        //loop through leaf set if any
        for(Map.Entry<byte[],NodeAddress> entry : routingInfoMsg.getLeafSet().entrySet()) {
            //update leaf set
            if(entry.getValue().getInetAddress() == null) {
                changed = this.node.leafSet.addNewNode(node, entry.getKey(), new NodeAddress(entry.getValue().getNodeName(), socket.getInetAddress(), entry.getValue().getPort())) || changed;
            } else {
                changed = this.node.leafSet.addNewNode(node,entry.getKey(), entry.getValue()) || changed;
            }

            //update routing table based on each entry in leaf set
            if(!java.util.Arrays.equals(entry.getKey(), node.nodeID)) { //if not this id
                String nodeIDStr = Util.convertBytesToHex(entry.getKey());
                int i=0;
                for(i=0; i<4; i++) { //find matching characters to find the row
                    if(node.idStr.charAt(i) != nodeIDStr.charAt(i)) {
                        nodeIDStr = "" + nodeIDStr.charAt(i);
                        break;
                    }
                }

                if(entry.getValue().getInetAddress() == null) {
                    changed = this.node.routingTable.addNewNode(node,nodeIDStr, new NodeAddress(entry.getValue().getNodeName(), socket.getInetAddress(), entry.getValue().getPort()), i) || changed;
                } else {
                    changed = this.node.routingTable.addNewNode(node,nodeIDStr, entry.getValue(), i) || changed;
                }
            }
        }

        //update routing table based on routing info table
        for(Map.Entry<String,NodeAddress> entry : routingInfoMsg.getRoutingTable().entrySet()) {
            changed = this.node.routingTable.addNewNode(node,entry.getKey(), entry.getValue(), routingInfoMsg.getPrefixLength()) || changed;
        }

        //update neighbourhoodSet based on neighbourset info
        for(Map.Entry<byte[],NodeAddress> entry: routingInfoMsg.getNeighborSet().entrySet()){
            changed = this.node.neighborhoodSet.addNewNode(node,entry.getKey(),entry.getValue()) || changed;
        }
        //print out leaf set and routing table. (if at least one entry in leaf set or routing table changed)
        //neetha: not printing here as the print does not work in multiple thread. CHeck locks
        //yinna: this can print correctly
        //neetha: no. order is not maintained.. all the threads write. so same lines get printed multiple times
//        if(changed) {
//            node.leafSet.print(node);
//            node.routingTable.print(node);
//        }

        node.readWriteLock.readLock().lock();
        try {
            //if this is a message from the closest node send routing information to every node in leaf set
            if(routingInfoMsg.getBroadcastMsg()) {
                List<NodeAddress> nodeBlacklist = new LinkedList<>();

                //send to left leaf set
                for(Map.Entry<byte[],NodeAddress> entry : node.leafSet.leftSet.entrySet()) {
                    if(nodeBlacklist.contains(entry.getValue())) {
                        continue;
                    } else {
                        nodeBlacklist.add(entry.getValue());
                    }

                    //find longest prefix match
                    String nodeIDStr = Util.convertBytesToHex(entry.getKey());
                    int i=0;
                    for(i=0; i<4; i++) {
                        if(node.idStr.charAt(i) != nodeIDStr.charAt(i)) {
                            break;
                        }
                    }

                    //send routing update
                    Socket nodeSocket = new Socket(entry.getValue().getInetAddress(), entry.getValue().getPort());
                    ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
                    nodeOut.writeObject(new RoutingInfoMsg(this.node.leafSet.get(this.node), this.node.neighborhoodSet.get(this.node),i, this.node.routingTable.get(this.node,i), false));
                    nodeSocket.close();
                }

                //send to right leaf set
                for(Map.Entry<byte[],NodeAddress> entry : node.leafSet.rightSet.entrySet()) {
                    if(nodeBlacklist.contains(entry.getValue())) {
                        continue;
                    } else {
                        nodeBlacklist.add(entry.getValue());
                    }

                    //find longest prefix match
                    String nodeIDStr = Util.convertBytesToHex(entry.getKey());
                    int i=0;
                    for(i=0; i<4; i++) {
                        if(node.idStr.charAt(i) != nodeIDStr.charAt(i)) {
                            break;
                        }
                    }

                    //send routing update
                    Socket nodeSocket = new Socket(entry.getValue().getInetAddress(), entry.getValue().getPort());
                    ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
                    nodeOut.writeObject(new RoutingInfoMsg(this.node.leafSet.get(this.node),this.node.neighborhoodSet.get(this.node), i, this.node.routingTable.get(this.node,i), false));
                    nodeSocket.close();
                }

                //send to routing table nodes
                for(int i=0; i< node.routingTable.tableSize; i++) {
                    Map<String,NodeAddress> map = this.node.routingTable.get(this.node,i);
                    for(Map.Entry<String,NodeAddress> entry : map.entrySet()) {
                        if(nodeBlacklist.contains(entry.getValue())) {
                            continue;
                        } else {
                            nodeBlacklist.add(entry.getValue());
                        }

                        Socket nodeSocket = new Socket(entry.getValue().getInetAddress(), entry.getValue().getPort());
                        ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
                        nodeOut.writeObject(new RoutingInfoMsg(this.node.leafSet.get(this.node), this.node.neighborhoodSet.get(this.node),i, map, false));
                        nodeSocket.close();
                    }
                }
                //send to neighbouring nodes
                for(Map.Entry<byte[],NodeAddress> entry : node.neighborhoodSet.neighborSet.entrySet()) {
                    if(nodeBlacklist.contains(entry.getValue())) {
                        continue;
                    } else {
                        nodeBlacklist.add(entry.getValue());
                    }

                    //find longest prefix match for routing info
                    String nodeIDStr = Util.convertBytesToHex(entry.getKey());
                    int i=0;
                    for(i=0; i<4; i++) {
                        if(node.idStr.charAt(i) != nodeIDStr.charAt(i)) {
                            break;
                        }
                    }

                    //send routing update
                    Socket nodeSocket = new Socket(entry.getValue().getInetAddress(), entry.getValue().getPort());
                    ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
                    nodeOut.writeObject(new RoutingInfoMsg(this.node.leafSet.get(this.node),this.node.neighborhoodSet.get(this.node), i, this.node.routingTable.get(this.node,i), false));
                    nodeSocket.close();
                }

                //request for data transfer from leaf nodes
                RequestDataMessage requestDataMessage = new RequestDataMessage(this.node.nodeID,this.node.address);
                for(Map.Entry<byte[],NodeAddress> entry : node.leafSet.leftSet.entrySet()) {
                    //send routing update
                    Socket nodeSocket = new Socket(entry.getValue().getInetAddress(), entry.getValue().getPort());
                    ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
                    nodeOut.writeObject(requestDataMessage);
                    nodeSocket.close();
                }
                for(Map.Entry<byte[],NodeAddress> entry : node.leafSet.rightSet.entrySet()) {
                    //send routing update
                    Socket nodeSocket = new Socket(entry.getValue().getInetAddress(), entry.getValue().getPort());
                    ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
                    nodeOut.writeObject(requestDataMessage);
                    nodeSocket.close();
                }
            }
            }catch (Exception e) {
                System.out.println(e.getMessage());
            } finally {
            node.readWriteLock.readLock().unlock();
        }
    }

    private void lookupNodeHandler(Message requestMsg) throws IOException {
        LookupNodeMsg lookupNodeMsg = (LookupNodeMsg)requestMsg;
        if(lookupNodeMsg.getNodeAddress().getInetAddress() == null) {
            lookupNodeMsg.getNodeAddress().setInetAddress(socket.getInetAddress());
        }

        System.out.println("Received look up node message '" + lookupNodeMsg.toString() + "'");
        NodeAddress forwardAddr = null;

        //check the request type
        //if read
        boolean foundData= false;
        if(lookupNodeMsg.getRequestType() == 0){
            //search if you have data id in store
            String data = node.dataStore.searchDataID(node,convertBytesToHex(lookupNodeMsg.getID()));
            if (!data.equals("")){
                foundData = true;
                //current node can answer to read request.
                System.out.println("Current node is the closest node. Send response to '" + lookupNodeMsg.getNodeAddress() + "'");

                Socket socket = new Socket(lookupNodeMsg.getNodeAddress().getInetAddress(), lookupNodeMsg.getNodeAddress().getPort());
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(new NearByNodeInfoMsg(this.node.nodeID, new NodeAddress(this.node.name, null, this.node.port)));
                socket.close();
            }
        }
        if(!foundData){
            //check if data existed in leaf set
            int searchId = Integer.parseInt(convertBytesToHex(lookupNodeMsg.getID()),16);
            byte[] lsMinIdInByte = null;
            byte[] lsMaxIdInByte = null;
            int lsMinId = Integer.MIN_VALUE;
            int lsMaxId = Integer.MAX_VALUE;
            if((this.node.leafSet.leftSet.size() > 0 && this.node.leafSet.rightSet.size() > 0)) {
                lsMinIdInByte = this.node.leafSet.leftSet.firstKey();
                lsMaxIdInByte = this.node.leafSet.rightSet.firstKey();
                lsMinId = Integer.parseInt(convertBytesToHex(lsMinIdInByte),16);
                lsMaxId = Integer.parseInt(convertBytesToHex(lsMaxIdInByte),16);
            }

            //neetha: now lsMinId is always less than lsMaxId (no negative numbers)
            //min < search < max || (max < min < search || search < max < min)
            if ((this.node.leafSet.leftSet.size() > 0 && this.node.leafSet.rightSet.size() > 0) &&
                    (lsMinId <= searchId && searchId <= lsMaxId)) {    //min = 2, id = 4, max = 6
                forwardAddr = this.node.leafSet.searchClosest(this.node, lookupNodeMsg.getID());
            } else {
                forwardAddr = this.node.routingTable.searchExact(this.node, lookupNodeMsg.getID(), lookupNodeMsg.getPrefixLength());
                if (forwardAddr != null) {
                    lookupNodeMsg.setLongestPrefixMatch(lookupNodeMsg.getPrefixLength() + 1);
                } else {
                    forwardAddr = this.node.routingTable.searchClosest(this.node, lookupNodeMsg.getID(), lookupNodeMsg.getPrefixLength());
                    if (forwardAddr == null) {
                        forwardAddr = this.node.leafSet.searchClosest(this.node, lookupNodeMsg.getID());
                    }
                }
            }
            if(forwardAddr.getInetAddress() == null) {
                //The current node is the closest where the data should reside in
                System.out.println("Current node is the closest node. Send response to '" + lookupNodeMsg.getNodeAddress() + "'");

                Socket socket = new Socket(lookupNodeMsg.getNodeAddress().getInetAddress(), lookupNodeMsg.getNodeAddress().getPort());
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(new NearByNodeInfoMsg(this.node.nodeID, new NodeAddress(this.node.name, null, this.node.port)));
                socket.close();
            } else {
                //Keep forwarding the request to closer node
                System.out.println("Forwarding lookup node message for id = " + convertBytesToHex(lookupNodeMsg.getID()) + " to closer node '" + forwardAddr + "'");
                Socket socket = new Socket(forwardAddr.getInetAddress(), forwardAddr.getPort());
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(lookupNodeMsg);
                socket.close();
            }
        }
    }

    private void readDataHandler(Message requestMsg) {
        ReadDataMsg readDataMsg = (ReadDataMsg)requestMsg;
        String readId = convertBytesToHex(readDataMsg.getID());

        System.out.println("Reading data with id = " + readId);
        //check if we have data with this id
        String dataInString = node.dataStore.searchDataID(node,readId);
        if(!dataInString.equals("")){
            System.out.println("read id found in datalist");
            byte[] data = node.dataStore.getByteData(node,readId);
            replyMsg = new WriteDataMsg(readDataMsg.getID(), data);
        }else{
            System.out.println("Data with id = " + readId + " is not found on this node");
            replyMsg = new ErrorMessage("The current node does not contain data with id = " + readId);
        }
    }

    private void writeDataHandler(Message requestMsg) {
        try{
            WriteDataMsg writeDataMsg = (WriteDataMsg)requestMsg;
            String writeId = convertBytesToHex(writeDataMsg.getID());

            System.out.println("Writing in data with id = " + writeId);
            //write data to the current node
            this.node.dataStore.ownedData.put(writeId, writeDataMsg.getData());
            //send a copy to the first left leaf node to maintain a copy
            if(this.node.leafSet.leftSet.size() > 0){
                byte[] replicaNode = this.node.leafSet.leftSet.firstKey();
                NodeAddress replicaNodeAddress = this.node.leafSet.leftSet.get(replicaNode);
                HashMap<String,byte[]> replicaToSend = new HashMap<>();
                replicaToSend.put(writeId,writeDataMsg.getData());
                System.out.println("Transferring a copy to left leaf node");
                DataTransferMsg dataTransferMsg = new DataTransferMsg(this.node.nodeID,this.node.address,new HashMap<>(),replicaToSend);
                Socket nodeSocket = new Socket(replicaNodeAddress.getInetAddress(), replicaNodeAddress.getPort());
                ObjectOutputStream dataTransferStream = new ObjectOutputStream(nodeSocket.getOutputStream());
                dataTransferStream.writeObject(dataTransferMsg);
                nodeSocket.close();
            }
        }catch(Exception e){
              e.printStackTrace();
        }

    }

    private void dataRequestHandler(Message requestMsg) {
        try{
            RequestDataMessage requestDataMsg = (RequestDataMessage) requestMsg;
            if(requestDataMsg.getNodeAddress().getInetAddress() == null) {
                requestDataMsg.getNodeAddress().setInetAddress(socket.getInetAddress());
            }
            System.out.println("Received data transfer request from node:"+convertBytesToHex(requestDataMsg.getID()));
            int requesterIdInInt = Integer.parseInt(convertBytesToHex(requestDataMsg.getID()),16);
            int idInInt = Integer.parseInt(convertBytesToHex(this.node.nodeID),16);
            //search if requester is in left leaf set
            HashMap<String,byte[]> dataToSend = new HashMap<>();
            HashMap<String,byte[]> replicaToSend = new HashMap<>();
            HashMap<String,byte[]> dataToMove = new HashMap<>();
            if(this.node.leafSet.leftSet.containsKey(requestDataMsg.getID())){
                //transfer the data of ids < node id
                for(String dataId : node.dataStore.ownedData.keySet()) {
                    int dataIdInInt = Integer.parseInt(dataId,16);
                    if(dataIdInInt < idInInt ){
                        //now this data should belong to joining node
                        dataToSend.put(dataId,node.dataStore.ownedData.get(dataId));
                        //remove from this node
                        node.dataStore.ownedData.remove(dataId);
                    }
                }
            }
            //search if the requester is in right set
            if(this.node.leafSet.rightSet.containsKey(requestDataMsg.getID())){
                for(String dataId : node.dataStore.ownedData.keySet()) {
                    int dataIdInInt = Integer.parseInt(dataId,16);
                    if(dataIdInInt > idInInt && dataIdInInt >= requesterIdInInt){
                        //now this data should belong to joining node
                        dataToSend.put(dataId,node.dataStore.ownedData.get(dataId));
                        //dont remove but move it to replicated data store
                        dataToMove.put(dataId,node.dataStore.ownedData.get(dataId));
                        node.dataStore.ownedData.remove(dataId);
                    }
                }
                for(String dataId : node.dataStore.replicatedData.keySet()) {
                    int dataIdInInt = Integer.parseInt(dataId,16);
                    if(dataIdInInt > idInInt && dataIdInInt >= requesterIdInInt){
                        //now this data should belong to joining node
                        replicaToSend.put(dataId,node.dataStore.replicatedData.get(dataId));
                        //remove from replicated data
                        node.dataStore.replicatedData.remove(dataId);
                    }
                }
                //now move data
                node.dataStore.replicatedData.putAll(dataToMove);
            }
            if(dataToSend.size() != 0 || replicaToSend.size()!= 0){
                System.out.println("Transferring applicable data to joining node");
                DataTransferMsg dataTransferMsg = new DataTransferMsg(this.node.nodeID,this.node.address,dataToSend,replicaToSend);
                Socket nodeSocket = new Socket(requestDataMsg.getNodeAddress().getInetAddress(), requestDataMsg.getNodeAddress().getPort());
                ObjectOutputStream dataTransferStream = new ObjectOutputStream(nodeSocket.getOutputStream());
                dataTransferStream.writeObject(dataTransferMsg);
                nodeSocket.close();
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private void dataTransferHandler(Message requestMsg) {
        try{
            DataTransferMsg dataTransferMsg = (DataTransferMsg) requestMsg;
            System.out.println("Received data transfer message from node:"+convertBytesToHex(dataTransferMsg.getID()));
            //add without ignoring any
            this.node.dataStore.ownedData.putAll(dataTransferMsg.getData());
            this.node.dataStore.replicatedData.putAll(dataTransferMsg.getReplicatedData());

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private void keepAliveMsgHandler(Message requestMsg) {
        try{
            KeepAliveMsg keepAliveMsg = (KeepAliveMsg) requestMsg;
            System.out.println("Received keep alive message from node:"+convertBytesToHex(keepAliveMsg.getID()));
            this.node.leafNodeInfoStore.setNodeInfo(keepAliveMsg.getID(),keepAliveMsg.getTimestamp(),keepAliveMsg.getLeafSet());
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}