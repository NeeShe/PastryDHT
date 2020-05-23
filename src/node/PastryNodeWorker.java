package node;

import message.Message;
import message.NodeJoinMessage;
import message.RoutingInfoMsg;
import util.NodeAddress;
import util.Util;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

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
                System.out.println("search for an exact match in the routing table");
////                //search for an exact match in the routing table
//            NodeAddress nodeAddress = null;
                NodeAddress nodeAddress = this.node.routingTable.searchExact(this.node, nodeJoinMsg.getID(), p);
                System.out.println("OUSITE IF - Got the exact match of nodeAddress as ");
                if(nodeAddress != null) {
                    //update longest prefix match if we have indeed found a match
                    System.out.println("Got the exact match of nodeAddress as " + nodeAddress);
                    nodeJoinMsg.setLongestPrefixMatch((nodeJoinMsg.getPrefixLength() + 1));
                    System.out.println("CUrrent nodeJoinMsg : " + nodeJoinMsg.toString());
                }


                System.out.println("search for closest node in routing table");
//                //search for closest node in routing table
//                if(nodeAddress == null) {
//                    nodeAddress = this.node.routingTable.searchClosest(this.node, nodeJoinMsg.getID(), p);
//                }
//
//                System.out.println("find closest node in leaf set");
////                //find closest node in leaf set
//                if(nodeAddress == null || nodeJoinMsg.hopContains(nodeAddress)) {
//                    nodeAddress = this.node.leafSet.searchClosest(this.node, nodeJoinMsg.getID());
//                }
                System.out.println("Finally got the nodeAddress = " + nodeAddress);
//
//                //send routing information to joining node
                Socket joinNodeSocket = null;
                joinNodeSocket = new Socket(nodeJoinMsg.getNodeAddress().getInetAddress(), nodeJoinMsg.getNodeAddress().getPort());
                ObjectOutputStream joinNodeOut = new ObjectOutputStream(joinNodeSocket.getOutputStream());
                joinNodeOut.writeObject(
                        new RoutingInfoMsg(this.node.leafSet.get(this.node), p, this.node.routingTable.get(this.node, p),
                                nodeAddress.getInetAddress() == null
                                //last routing info message received by the joining node
                        )
                );
                joinNodeSocket.close();
                //if we found a closer node forward the node join message
                if(nodeAddress.getInetAddress() != null) {
                    System.out.println("Forwarding node join message with id '" + convertBytesToHex(nodeJoinMsg.getID()) + "' to '" + nodeAddress + "'");
                    nodeJoinMsg.addHop(nodeAddress);
                    Socket nodeSocket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());
                    ObjectOutputStream nodeOut = new ObjectOutputStream(nodeSocket.getOutputStream());
                    nodeOut.writeObject(nodeJoinMsg);
                    nodeSocket.close();
                }
//
        }catch(Exception e){
            System.err.println(e.getMessage());
        }
    }
}