package node;

import data.DataStore;
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
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static util.Util.*;


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
    public LeafSet leafSet;
    public NeighborhoodSet neighborhoodSet;
    public RoutingTable routingTable;
    public ReadWriteLock readWriteLock;

    public PastryNode(byte[] id, String name, int port, int discoveryNodePort) {

        this.name=name;
        this.port = port;
        this.discoveryNodePort = discoveryNodePort;
        this.nodeID = id;
        this.idStr = Util.convertBytesToHex(this.nodeID);
        this.address = new NodeAddress(this.name, null, this.port);
        this.dataStore = new DataStore();
        this.leafSet = new LeafSet(this.nodeID, this.leafSize);
        this.neighborhoodSet = new NeighborhoodSet(this.nodeID,this.NEIGHBOR_SIZE);
        this.routingTable = new RoutingTable();
        this.readWriteLock = new ReentrantReadWriteLock();

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
                        NearByNodeInfoMsg nodeInfoMsg = (NearByNodeInfoMsg) replyMsg;
                        System.out.println("Sending Node join message to '" + nodeInfoMsg.getNodeAddress().getInetAddress() + ":" + nodeInfoMsg.getNodeAddress().getPort() + "'");
                        //send node join message
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
            PastryNode node = new PastryNode(id, name, port, discoveryNodePort);
            new Thread(node).start();

            Scanner scn = new Scanner(System.in);
            while (true) {
                String op = scn.nextLine();
                if(op.equalsIgnoreCase("print")){
                    System.out.println("------------NODE:"+node.idStr+"-----------------");
                    node.neighborhoodSet.print(node);
                    node.leafSet.print(node);
                    node.routingTable.print(node);
                    node.dataStore.printData(node);
                }else if(op.equalsIgnoreCase("getId")){
                    short idInShort = convertBytesToShort(node.nodeID);
                    System.out.println("ID In Short"+idInShort);
                }
            }

        } catch(Exception e) {
            System.err.println(e.getMessage());
            System.out.println("Usage: node.PastryNode port discoveryNodePort [id]");
        }
    }



}

