package node;

import message.Message;
import message.NodeJoinMessage;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class PastryNodeWorker extends Thread{
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
