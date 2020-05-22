package node;

import model.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;


public class PastryNode extends Thread{
    public static final int ID_BYTES = 2;
    protected int port;
    protected int discoveryNodePort;
    protected InetAddress discoveryNodeAddress;
    protected byte[] nodeID;
    protected short idValue;
    protected String idStr;
    protected String name;

    public PastryNode(String name, int port, int discoveryNodePort, byte[] id) {
        this.name=name;
        this.port = port;
        this.discoveryNodePort = discoveryNodePort;
        this.nodeID = id;
        idValue = Util.convertBytesToShort(this.nodeID);
        idStr = Util.convertBytesToHex(id);

        System.out.println("Hi Im "+name);
        System.out.println("My id is "+idStr);
        //initialize tree map structures
//        lessThanLS = new TreeMap(
//                new Comparator<byte[]>() {
//                    @Override
//                    public int compare(byte[] b1, byte[] b2) {
//                        int s1 = lessThanDistance(convertBytesToShort(b1), idValue),
//                                s2 = lessThanDistance(convertBytesToShort(b2), idValue);
//
//                        if (s1 < s2) {
//                            return 1;
//                        } else if (s1 > s2) {
//                            return -1;
//                        } else {
//                            return 0;
//                        }
//                    }
//                }
//        );

//        greaterThanLS = new TreeMap(
//                new Comparator<byte[]>() {
//                    @Override
//                    public int compare(byte[] b1, byte[] b2) {
//                        int s1 = greaterThanDistance(convertBytesToShort(b1), idValue),
//                                s2 = greaterThanDistance(convertBytesToShort(b2), idValue);
//
//                        if (s1 > s2) {
//                            return 1;
//                        } else if (s1 < s2) {
//                            return -1;
//                        } else {
//                            return 0;
//                        }
//                    }
//                }
//        );
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);

            //register your id with the discovery node
            boolean success = false;
            while(!success) {
                System.out.println("Registering ID '" + Util.convertBytesToHex(nodeID) + "' to '" + discoveryNodePort + "'");
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
                        NearByNodeInfo nodeInfoMsg = (NearByNodeInfo) replyMsg;
                        System.out.println("Sending node join message to '" + nodeInfoMsg.getNodeAddress().getInetAddress() + ":" + nodeInfoMsg.getNodeAddress().getPort() + "'");
                        //send node join message
                        this.sendJoinRequest(nodeInfoMsg);
                        success = true;
                        break;
                    case Message.SUCCESS_MSG:
                            //if you're the first node registering a success messasge is sent back
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
                System.out.println("Waiting for requests");
                Socket socket = serverSocket.accept();
                System.out.println("Received connection from '" + socket.getInetAddress() + ":" + socket.getPort() + "'.");
               new Thread(new PastryNodeWorker(socket)).start();
            }
        } catch(Exception e) {
            e.printStackTrace();
            System.err.println(e.getMessage());
        }
    }

    private void sendJoinRequest(NearByNodeInfo nodeInfoMsg) {
        NodeJoinMessage nodeJoinMsg = new NodeJoinMessage(nodeID, 0, new NodeAddress(name, null, port));
        nodeJoinMsg.addHop(nodeInfoMsg.getNodeAddress());
        Socket nodeSocket = null;
        try {
            nodeSocket = new Socket(nodeInfoMsg.getNodeAddress().getInetAddress(), nodeInfoMsg.getNodeAddress().getPort());
            ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
            nodeOut.writeObject(nodeJoinMsg);
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
            new Thread(new PastryNode(name,port,discoveryNodePort, id)).start();
        } catch(Exception e) {
            System.err.println(e.getMessage());
            System.out.println("Usage: node.PastryNode port discoveryNodePort [id]");
        }
    }

    protected class PastryNodeWorker extends Thread{
        protected Socket socket;

        public PastryNodeWorker(Socket socket) {
            this.socket  = socket;
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
                System.out.println("Received node join message '" + nodeJoinMsg.toString() + "'.");
                int p = nodeJoinMsg.getPrefixLength();
//TODO
//                //search for an exact match in the routing table
//                NodeAddress nodeAddress = searchRoutingTableExact(nodeJoinMsg.getID(), p);
//                if(nodeAddress != null) {
//                    //update longest prefix match if we have indeed found a match
//                    nodeJoinMsg.setLongestPrefixMatch((nodeJoinMsg.getPrefixLength() + 1));
//                }
//
//                //search for closest node in routing table
//                if(nodeAddress == null) {
//                    nodeAddress = searchRoutingTableClosest(nodeJoinMsg.getID(), p);
//                }
//
//                //find closest node in leaf set
//                if(nodeAddress == null || nodeJoinMsg.hopContains(nodeAddress)) {
//                    nodeAddress = searchLeafSetClosest(nodeJoinMsg.getID());
//                }
//
//                //send routing information to joining node
//                Socket joinNodeSocket = null;
//                joinNodeSocket = new Socket(nodeJoinMsg.getNodeAddress().getInetAddress(), nodeJoinMsg.getNodeAddress().getPort());
//                ObjectOutputStream joinNodeOut = new ObjectOutputStream(joinNodeSocket.getOutputStream());
//                joinNodeOut.writeObject(
//                        new RoutingInfoMsg(getRelevantLeafSet(), p, getRelevantRoutingTable(p),
//                                nodeAddress.getInetAddress() == null
//                                //last routing info message received by the joining node
//                        )
//                );
//                joinNodeSocket.close();
//                //if we found a closer node forward the node join message
//                if(nodeAddress.getInetAddress() != null) {
//                    System.out.println("Forwarding node join message with id '" + Util.convertBytesToHex(nodeJoinMsg.getID()) + "' to '" + nodeAddress + "'");
//                    nodeJoinMsg.addHop(nodeAddress);
//                    Socket nodeSocket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());
//                    ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
//                    nodeOut.writeObject(nodeJoinMsg);
//                    nodeSocket.close();
//                }
//
            }catch(Exception e){
                System.err.println(e.getMessage());
            }
        }
    }

}