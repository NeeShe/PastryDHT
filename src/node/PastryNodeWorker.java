package node;

import message.Message;
import message.NodeJoinMessage;
import message.RoutingInfoMsg;
import util.NodeAddress;
import util.Util;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static util.Util.convertBytesToHex;

public class PastryNodeWorker extends Thread{
    protected Socket socket;
    protected PastryNode node;

    public PastryNodeWorker(Socket socket, PastryNode node) {
        this.socket  = socket;
        this.node = node;
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
                default:
                    System.err.println("Unrecognized request message type '" + requestMsg.getMsgType() + "'");
                    break;
            }

            //write reply message
            if(replyMsg != null) {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(replyMsg);
            }
        } catch(Exception e) {
            e.printStackTrace();
            System.err.println(e.getMessage());
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
//        if(changed) {
//            node.leafSet.print(node);
//            node.routingTable.print(node);
//        }

        node.readWriteLock.readLock().lock();
        try {
            //if this is a message from the closest node send routing information to every node in leaf set
            if(routingInfoMsg.getBroadcastMsg()) {
                List<NodeAddress> nodeBlacklist = new LinkedList();

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
                //send to right leaf set
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
            }
//TODO
//            transfer data to other nodes if needed
//            for(String dataID : dataStore) {
//                short dataIDValue = convertBytesToShort(HexConverter.convertHexToBytes(dataID));
//                int minDistance = Math.min(lessThanDistance(idValue, dataIDValue), greaterThanDistance(idValue, dataIDValue));
//                NodeAddress forwardNodeAddress = null;
//
//                //check less than leaf set
//                for(Entry<byte[],NodeAddress> entry : lessThanLS.entrySet()) {
//                    short nodeIDValue = convertBytesToShort(entry.getKey());
//                    int distance = Math.min(lessThanDistance(dataIDValue, nodeIDValue), greaterThanDistance(dataIDValue, nodeIDValue));
//                    if(distance < minDistance) {
//                        minDistance = distance;
//                        forwardNodeAddress = entry.getValue();
//                    }
//                }
//
//                //check greater than leaf set
//                for(Entry<byte[],NodeAddress> entry : greaterThanLS.entrySet()) {
//                    short nodeIDValue = convertBytesToShort(entry.getKey());
//                    int distance = Math.min(lessThanDistance(dataIDValue, nodeIDValue), greaterThanDistance(dataIDValue, nodeIDValue));
//                    if(distance < minDistance) {
//                        minDistance = distance;
//                        forwardNodeAddress = entry.getValue();
//                    }
//                }
//
//                if(forwardNodeAddress != null) {
//                    //read file
//                    File file = new File(getFilename(storageDir,dataID));
//                    byte[] data = new byte[(int)file.length()];
//                    FileInputStream fileIn = new FileInputStream(file);
//                    if(fileIn.read(data) != data.length) {
//                        throw new Exception("Unknown error reading file.");
//                    }
//
//                    fileIn.close();
//
//                    //send write data message to node
//                    LOGGER.info("Forwarding data with id '" + dataID + "' to node " + forwardNodeAddress + ".");
//                    Socket forwardSocket = new Socket(forwardNodeAddress.getInetAddress(), forwardNodeAddress.getPort());
//                    ObjectOutputStream forwardOut = new ObjectOutputStream(forwardSocket.getOutputStream());
//                    forwardOut.writeObject(new WriteDataMsg(HexConverter.convertHexToBytes(dataID), data));
//
//                    forwardSocket.close();
//                    file.delete();
            }catch (Exception e) {
                System.out.println(e.getMessage());
            } finally {
            node.readWriteLock.readLock().unlock();
        }
    }
}