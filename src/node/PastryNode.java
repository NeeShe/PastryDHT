package node;

import data.DataStore;
import data.LeafNodeInfoStore;
import message.*;
import routing.LeafSet;
import routing.NeighborhoodSet;
import routing.RoutingTable;
import message.NearByNodeInfoMsg;
import util.NodeAddress;
import util.Util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class PastryNode extends Thread{
    public static final int ID_BYTES = 2;
    public static final int leafSize = 1;
    public static final int NEIGHBOR_SIZE = 2;
    public int port;
    public int discoveryNodePort;
    public InetAddress discoveryNodeAddress;
    public byte[] nodeID;
    public String idStr;
    public String name;
    public NodeAddress address;
    public DataStore dataStore;
    public LeafNodeInfoStore leafNodeInfoStore;
    public LeafSet leafSet;
    public NeighborhoodSet neighborhoodSet;
    public RoutingTable routingTable;
    public ReadWriteLock readWriteLock;
    public static long keepAlivePeriod = 60000; // 1 minute
    public FailureHandler failureHandler;
    public PastryNode(byte[] id, String name, int port, int discoveryNodePort) throws UnknownHostException {

        this.name=name;
        this.port = port;
        this.discoveryNodePort = discoveryNodePort;
        this.nodeID = id;
        this.idStr = Util.convertBytesToHex(this.nodeID);
        this.address = new NodeAddress(this.nodeID, this.name, InetAddress.getLocalHost(), this.port);
        this.dataStore = new DataStore();
        this.leafNodeInfoStore = new LeafNodeInfoStore();
        this.leafSet = new LeafSet(this.nodeID, this.leafSize);
        this.neighborhoodSet = new NeighborhoodSet(this.nodeID,this.NEIGHBOR_SIZE);
        this.routingTable = new RoutingTable();
        this.readWriteLock = new ReentrantReadWriteLock();
        this.failureHandler = new FailureHandler();
        System.out.println("Initialized  "+name+": with id:"+idStr);
    }


    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);

            //register the nodeID into the discovery node list
            boolean success = false;
            while(!success) {
                System.out.println("Registering ID '" + Util.convertBytesToHex(nodeID) + "' to Discovery Node at '" + discoveryNodePort + "'");
                this.discoveryNodeAddress = InetAddress.getLocalHost();
                Socket discoveryNodeSocket = new Socket(discoveryNodeAddress, discoveryNodePort);
                RegisterMessage registerNodeMsg = new RegisterMessage(name, nodeID, serverSocket.getInetAddress(), port);
                ObjectOutputStream out = new ObjectOutputStream(discoveryNodeSocket.getOutputStream());
                out.writeObject(registerNodeMsg);

                ObjectInputStream in = new ObjectInputStream(discoveryNodeSocket.getInputStream());
                Message replyMsg = (Message) in.readObject();
                discoveryNodeSocket.close();

                //perform action on reply message
                switch(replyMsg.getMsgType()) {
                    case Message.NEARBY_NODE_INFO_MSG:
                        //got the info of a random nearby node
                        NearByNodeInfoMsg nodeInfoMsg = (NearByNodeInfoMsg) replyMsg;
                        System.out.println("Sending Node join message to '" + nodeInfoMsg.getNodeAddress().getInetAddress() + ":" + nodeInfoMsg.getNodeAddress().getPort() + "'");
                        //send node join message to the random node we just got
                        this.neighborhoodSet.addNewNode(this,nodeInfoMsg.getID(),nodeInfoMsg.getNodeAddress());
                        this.sendJoinRequest(nodeInfoMsg);
                        success = true;
                        break;
                    case Message.SUCCESS_MSG:
                            //if you're the first node registering a success messasge is sent back
                            System.out.println("First Node in the overlay network");
                            success = true;
                            break;
                    case Message.ERROR_MSG:
                        System.err.println(((ErrorMessage)replyMsg).getMsg());
                        nodeID = Util.generateRandomID(ID_BYTES);
                        continue;
                    default:
                        System.err.println("Received an unexpected message type '" + replyMsg.getMsgType() + "'.");
                        return;
                }
            }
            //now start listening to incoming requests
            while(true) {
                System.out.println("Waiting for requests in the main thread");
                Socket socket = serverSocket.accept();
                System.out.println("Received connection from '" + socket.getInetAddress() + ":" + socket.getPort() + "'.");
               new Thread(new PastryNodeWorker(socket, this)).start();
            }
        } catch(Exception e) {
            e.printStackTrace();
            System.err.println(e.getMessage());
        }
    }

    private void sendJoinRequest(NearByNodeInfoMsg nodeInfoMsg) {
        //the nodeInfoMsg contains info of the nearby random node
        //the nodeJoinMsg contains info of the current node
        NodeJoinMessage nodeJoinMsg = new NodeJoinMessage(nodeID, 0, new NodeAddress(nodeID, name, null, port));
        nodeJoinMsg.addHop(nodeInfoMsg.getNodeAddress());   //hop is now "currentNode -> randomNode" but will be received by randomNode
        Socket nodeSocket = null;
        try {
            nodeSocket = new Socket(nodeInfoMsg.getNodeAddress().getInetAddress(), nodeInfoMsg.getNodeAddress().getPort());
            ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
            nodeOut.writeObject(nodeJoinMsg);
        } catch (ConnectException e) {
            System.out.println("Cannot connect to node with port " + nodeInfoMsg.getNodeAddress().getNodeId());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        try {
            String name = args[0];
            int port = Integer.parseInt(args[1]);
            int discoveryNodePort = Integer.parseInt(args[2]);
            byte[] id= (args.length == 4) ? Util.convertHexToBytes(args[3]) : Util.generateRandomID(ID_BYTES);
            PastryNode node = new PastryNode(id, name, port, discoveryNodePort);
            new Thread(node).start();

            KeepAliveThread keepAliveThread = new KeepAliveThread(node,keepAlivePeriod);
            new Thread(keepAliveThread).start();

            FailureDetectorThread failureDetectorThread = new FailureDetectorThread(node,keepAlivePeriod);
            new Thread(failureDetectorThread).start();

            Scanner scn = new Scanner(System.in);
            while (true) {
                String op = scn.nextLine();
                if(op.equalsIgnoreCase("print")){
                    System.out.println("------------NODE:"+node.idStr+"-----------------");
                    node.neighborhoodSet.print(node);
                    node.leafSet.print(node);
                    node.routingTable.print(node);
                    System.out.println();
                    node.dataStore.printData(node);
                }else if(op.equalsIgnoreCase("leave")){
                    leave(node);
                }
            }

        } catch(Exception e) {
            System.err.println(e.getMessage());
            System.out.println("Usage: node.PastryNode port discoveryNodePort [id]");
        }
    }



    //the current node is leaving, notify all the other nodes in its list
    private static void leave(PastryNode node) throws IOException {
        System.out.println("the current node is leaving, notify all the other nodes in its list");
        //find out all the nodes in its list

        System.out.println("send leave request to nodes in routing table");
        //send leave request to nodes in routing table
        for(int prefixLen = 0; prefixLen < node.routingTable.tableSize; prefixLen++) {
            Map<String, NodeAddress> routingTableRow = node.routingTable.get(node, prefixLen);
            System.out.println("routing table prefix="+prefixLen+ ": " + routingTableRow);
            for(String pre : routingTableRow.keySet()) {
                byte[] id = routingTableRow.get(pre).getNodeId();
                sendLeaveRequest(node, id, routingTableRow.get(pre), prefixLen);
            }
        }

        System.out.println("notify nodes in leaf set");
        //notify nodes in leaf set
        for(byte[] id : node.leafSet.leftSet.keySet()) {
            sendLeaveRequest(node, id, node.leafSet.leftSet.get(id), -1);
        }
        for(byte[] id : node.leafSet.rightSet.keySet()) {
            sendLeaveRequest(node, id, node.leafSet.rightSet.get(id), -1);
        }

        System.out.println("notify nodes in neighborhood set");
        //notify nodes in neighborhood set
        for(byte[] id : node.neighborhoodSet.get(node).keySet()) {
            sendLeaveRequest(node, id, node.neighborhoodSet.get(node).get(id), -1);
        }

        if(node.leafSet.leftSet.size() > 0){
            System.out.println("transfer all data to left node in its left set");
            //transfer all data to left node in its left set
            NodeLeaveDataTransferMsg nodeLeaveDataTransferMsg = new NodeLeaveDataTransferMsg(node.nodeID, node.dataStore);
            byte[] destId = node.leafSet.leftSet.firstKey();
            NodeAddress destAddr = node.leafSet.leftSet.get(destId);

            System.out.println("Sending data store to '" + destAddr.getInetAddress() + ":" + destAddr.getPort() + "'");
            DataTransferMsg dataTransferMsg = new DataTransferMsg(destId, destAddr, nodeLeaveDataTransferMsg.getOwnedData(), nodeLeaveDataTransferMsg.getReplicatedData());
            Socket nodeSocket = null;
            try{
                nodeSocket = new Socket(destAddr.getInetAddress(), destAddr.getPort());

                //send out the message
                ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
                nodeOut.writeObject(dataTransferMsg);
            } catch (ConnectException e) {
                //TODO
                //data transfer. if failed to connect, needs to transfer data to another node
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if(node.leafSet.rightSet.size() > 0) {
            System.out.println("left leafset is empty and transfer all data to right node in its left set");
            //transfer all data to left node in its left set
            NodeLeaveDataTransferMsg nodeLeaveDataTransferMsg = new NodeLeaveDataTransferMsg(node.nodeID, node.dataStore);
            byte[] destId = node.leafSet.rightSet.firstKey();
            NodeAddress destAddr = node.leafSet.rightSet.get(destId);

            System.out.println("Sending data store to '" + destAddr.getInetAddress() + ":" + destAddr.getPort() + "'");
            DataTransferRightMsg dataTransferRightMsg = new DataTransferRightMsg(destId, destAddr, nodeLeaveDataTransferMsg.getOwnedData());
            Socket nodeSocket = null;
            try{
                nodeSocket = new Socket(destAddr.getInetAddress(), destAddr.getPort());

                //send out the message
                ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
                nodeOut.writeObject(dataTransferRightMsg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // FIXME
        ///////For test
        else {
            System.out.println("both leaf set are empty.");
        }
        /////

        System.out.println("Send leave msg to discovery node");
        //notify discovery node about node leaving
        NodeLeaveMsg nodeLeaveMsg = new NodeLeaveMsg(node.nodeID, 0, node.address);
        Socket discoveryNodeSocket = new Socket(node.discoveryNodeAddress, node.discoveryNodePort);
        ObjectOutputStream out = new ObjectOutputStream(discoveryNodeSocket.getOutputStream());
        System.out.println("send msg type = " + nodeLeaveMsg.getMsgType());
        out.writeObject(nodeLeaveMsg);
    }

    //send out the notification to a node
    private static void sendLeaveRequest(PastryNode node, byte[] destId, NodeAddress destAddress, int prefixLen) {

        System.out.println("sending out the notification to a node");
        //get the node info of the the destination node that needs to be notified
//        LeaveNodeInfoMsg leaveNodeInfoMsg = new LeaveNodeInfoMsg(destId, prefixLen, destAddress);
        System.out.println("Sending node leave message to '" + destAddress.getInetAddress() + ":" + destAddress.getPort() + "'");

        NodeLeaveMsg nodeLeaveMsg = new NodeLeaveMsg(node.nodeID, prefixLen, node.address);

        Socket nodeSocket = null;
        try{
            //connect with the node needs to be notified
            nodeSocket = new Socket(destAddress.getInetAddress(), destAddress.getPort());

            //send out the message
            ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
            nodeOut.writeObject(nodeLeaveMsg);
        } catch (ConnectException e) {
            //Here is to notify all the nodes in routing table and neighborhood set
            //so we just need to remove the failed node
//            node.failureHandler.handleFail(node, destId, node.failureHandler.LEAF_NODE, node.failureHandler.NON_DISSEMINATE);
            System.out.println("Sorry, the node with id = " + destId + " cannot be reached.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

