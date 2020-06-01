package node;

import message.*;
import message.NearByNodeInfoMsg;
import util.NodeAddress;
import util.Util;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static util.Util.convertBytesToHex;

//There will be one discovery node in the system
//Discovery node maintains information about the list of peers in the system.
//Every time a peer join or leave the system, it notifies this Discovery Node
//Registration Information:  - its 16 bits identifier
//                           - the {host:port} information
//                           - a nickname for this node
//Discovery Node is introduced to simplify the process of discoverying the
// first peer that will be the entry point into the system
//In the case of an ID space collision, the discovery node notifies the peer about the
// collision, at which point the peer will regenerate a new identifier and repeat the process

//Respnsibility     - Return one random node from the set of registered nodes
//                  - Detect collisions
public class DiscoveryNode extends Thread {
    private Random random;
    protected int port;
    protected Map<byte[], NodeAddress> nodes;   //A list of all the peer nodes in the system
    protected ReadWriteLock readWriteLock;

    public DiscoveryNode(int port) {
        random = new Random();  //Returning a random live peer's network information when a peer joins the overlay
        this.port = port;
        nodes = new HashMap<>();
        readWriteLock = new ReentrantReadWriteLock();
    }

    public static void main(String[] args) {
        try {
            int port = Integer.parseInt(args[0]);
            System.out.println("Starting Discovery Node");
            new Thread(new DiscoveryNode(port)).start();
        } catch (Exception e) {
            System.out.println("Usage: node.DiscoveryNode port");
            System.err.println(e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("Discovery node started successfully on port " + port);

            //accept connections
            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("Received connection from '" + socket.getInetAddress() + ":" + socket.getPort() + "'.");
                //System.out.println("Starting a worker thread");
                new Thread(new DiscoveryNodeWorker(socket)).start();
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    protected void addNode(byte[] id, NodeAddress nodeAddress) throws Exception {
        //check to see if id is already registered
        readWriteLock.readLock().lock();
        try {
            //check to see if id already exists in cluster
            for (byte[] array : nodes.keySet()) {
                if (Arrays.equals(array, id)) {
                    throw new Exception("ID '" + convertBytesToHex(id) + "' already exists in cluster");
                }
            }
        } finally {
            readWriteLock.readLock().unlock();
        }

        //else means the id is usable
        //add node to cluster using id and node address
        readWriteLock.writeLock().lock();
        try {
            nodes.put(id, nodeAddress);
            System.out.println("Added ID '" + convertBytesToHex(id) + "' for node at '" + nodeAddress + "'.");
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    protected void removeNode(byte[] leaveId, NodeAddress leaveAddr) throws Exception {
        readWriteLock.writeLock().lock();
        try {
            //check to see if id already exists in cluster
            boolean existed = false;
            for (byte[] id : nodes.keySet()) {
                if (Arrays.equals(id, leaveId)) {
                    nodes.remove(id);

                    //notify the left node that it has left successfully
                    Socket nodeSocket = new Socket(leaveAddr.getInetAddress(), leaveAddr.getPort());
                    NodeRemovedMsg nodeRemovedMsg = new NodeRemovedMsg();
                    ObjectOutputStream out = new ObjectOutputStream(nodeSocket.getOutputStream());
                    out.writeObject(nodeRemovedMsg);

                    existed = true;
                    break;
                }
            }
            if(!existed) {
                throw new Exception("ID '" + convertBytesToHex(leaveId) + "' does not exist in the cluster.");
            }
        } finally {
            readWriteLock.writeLock().unlock();
            printActiveNodes();
        }
    }

    protected void removeFailedNode(byte[] failId) throws Exception {
        readWriteLock.writeLock().lock();
        try {
            //check to see if id already exists in cluster
            for (byte[] id : nodes.keySet()) {
                if (Arrays.equals(id, failId)) {
                    nodes.remove(id);
                }
            }
        } finally {
            readWriteLock.writeLock().unlock();
            printActiveNodes();
        }
    }

    public byte[] getRandomNode() throws Exception {
        readWriteLock.readLock().lock();
        try {
            if (nodes.size() != 0) {
                //generate random number and iterate through peers while decrementing count
                int count = random.nextInt() % nodes.size();
                if (count < 0) {
                    count *= -1;
                }

                //find "count"th element in nodes
                for (byte[] id : nodes.keySet()) {
                    if (count-- == 0) {
                        return id;
                    }
                }
            }

            return null;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    protected void printActiveNodes() {
        readWriteLock.readLock().lock();
        try {
            StringBuilder str = new StringBuilder("----ACTIVE NODES----");
            for(Map.Entry<byte[],NodeAddress> entry : nodes.entrySet()) {
                str.append("\n" + convertBytesToHex(entry.getKey()) + " : " + entry.getValue());
            }
            str.append("\n--------------------");
            System.out.println(str.toString());
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private class DiscoveryNodeWorker extends Thread {
        protected Socket socket;

        public DiscoveryNodeWorker(Socket socket) {
            this.socket  = socket;
        }

        @Override
        public void run() {
            try {
                //read request message
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                Message requestMsg = (Message) in.readObject();

                Message replyMsg = null;

 //               System.out.println("received message type = " + requestMsg.getMsgType());
                switch(requestMsg.getMsgType()) {
                    case Message.REGISTER_NODE_MSG:
                        RegisterMessage registerNodeMsg = (RegisterMessage) requestMsg;

                        try {
                            byte[] id = getRandomNode(); //getting random node before so we don't have to worry about blacklisting the node we're adding
                            if(id == null) {
                                replyMsg = new SuccessMessage();
                            } else {
                                readWriteLock.readLock().lock();
                                try {
                                    //return the info of a random nearby node
                                    replyMsg = new NearByNodeInfoMsg(id, nodes.get(id));
                                } finally {
                                    readWriteLock.readLock().unlock();
                                }
                            }

                            addNode(registerNodeMsg.getID(), new NodeAddress(registerNodeMsg.getID(), registerNodeMsg.getNodeName(), socket.getInetAddress(), registerNodeMsg.getPort()));
                        } catch(Exception e) {
                            replyMsg = new ErrorMessage(e.getMessage());
                        }

                        //print out nodes
                        printActiveNodes();

                        break;
                    case Message.NODE_LEAVE_MSG:

                        NodeLeaveMsg nodeLeaveMsg = (NodeLeaveMsg)requestMsg;
                        byte[] leaveId = nodeLeaveMsg.getID();
                        NodeAddress leaveAddr = nodeLeaveMsg.getNodeAddress();
                        removeNode(leaveId, leaveAddr);
                        break;
                    case Message.REQUEST_RANDOM_NODE_MSG:
                        try {
                            byte[] id = getRandomNode();
                            if(id == null) {
                                replyMsg = new ErrorMessage("There aren't any nodes registered in the cluster yet.");
                            } else {
                                readWriteLock.readLock().lock();
                                try {
                                    replyMsg = new NearByNodeInfoMsg(id, nodes.get(id));
                                } finally {
                                    readWriteLock.readLock().unlock();
                                }
                            }
                        } catch(Exception e) {
                            replyMsg = new ErrorMessage(e.getMessage());
                        }
                        break;
                    case Message.NODE_FAIL_NOTIFY_MSG:
                        NodeFailNotifyMsg nodeFailNotifyMsg =(NodeFailNotifyMsg)requestMsg;
                        byte[] failId = nodeFailNotifyMsg.getID();
                        removeFailedNode(failId);
                        break;
                    default:
                        System.out.println(requestMsg.toString());
                        System.err.println("Unrecognized request message type '" + requestMsg.getMsgType() + "'");
                        break;
                }

                //write reply message
                if(replyMsg != null) {
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(replyMsg);
                }
            } catch(Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }
}