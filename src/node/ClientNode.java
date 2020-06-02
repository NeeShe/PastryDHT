package node;

import message.*;
import util.NodeAddress;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

import static util.Util.*;

public class ClientNode {

    public static byte[] id;
    public static String opType;
    public static String dataContent;
    public static byte[] data;
    static final int ID_BYTES = 2;


    public static void main(String args[]) throws Exception {
            int port = Integer.parseInt(args[0]);
            int discoveryNodePort = Integer.parseInt(args[1]);
            InetAddress discoveryNodeAddr = InetAddress.getLocalHost();
            int requestType;
            while (true) {
                System.out.println("------Options-------");
                System.out.println("W) Write data");
                System.out.println("R) Retrieve data");
                System.out.println("Q) Quit");
                System.out.print("Input:");

                Scanner scn = new Scanner(System.in);
                opType = scn.nextLine();
                if (opType.equalsIgnoreCase("Q")) {
                    break;
                } else if (opType.equalsIgnoreCase("W")
                        || opType.equalsIgnoreCase("R")) {
                    //store in new data
                    requestType=0;
                    System.out.print("Data Id (or leave blank to auto-generate for Write operations): ");
                    String idStr = scn.nextLine();
                    while((id != null && idStr.length() != 0 && idStr.length() < 4) || !isHexNumber(idStr)){
                        System.out.print("id format incorrect, please enter again : ");
                        idStr = scn.nextLine();
                    }
                    if(opType.equalsIgnoreCase("W")) {
                        System.out.print("New Data Content: ");
                        String content = scn.nextLine();
                        data = content.getBytes();
                        requestType=1; //data request type 1 => write
                    }
                    if (idStr == null || idStr.length() <= 0) {
                        //auto generate a new id randomly
                        id = generateRandomID(ID_BYTES);
                    } else {
                        id = convertHexToBytes(idStr);
                    }
                } else {
                    System.out.println("Sorry, your input is invalid. Please enter again.");
                    continue;
                }

                //assign node address to the new data node by getting a random node
                NodeAddress nodeAddr = getRandomNode(discoveryNodeAddr, discoveryNodePort);

                Socket testSocket = null;
                try{
                    testSocket = new Socket(nodeAddr.getInetAddress(), nodeAddr.getPort());
                    TestConnectMsg testConnectMsg = new TestConnectMsg();
                    ObjectOutputStream out = new ObjectOutputStream(testSocket.getOutputStream());
                    out.writeObject(testConnectMsg);
                    testSocket.close();
                } catch (ConnectException e) {

                    nodeAddr = getRandomNode(discoveryNodeAddr, discoveryNodePort);
                    System.out.println("delete this------ original random node is not reachable, assigned another one");
                }

                //look for the closest node in network
                NodeAddress closestNodeAddr = lookUpNode(nodeAddr, port,requestType);

                if (opType.equalsIgnoreCase("W")) {
                    System.out.println("Writing data with id " + convertBytesToHex(id) );//+ " to node " + closestNodeAddr);
                    //write data to the node found
                    WriteDataMsg writeDataMsg = new WriteDataMsg(id, data);
                    Socket socket = new Socket(closestNodeAddr.getInetAddress(), closestNodeAddr.getPort());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(writeDataMsg);

                    socket.close();
                } else if (opType.equalsIgnoreCase("R")) {

                    //retrieve data from closest node found
                    ReadDataMsg readDataMsg = new ReadDataMsg(id);
                    Socket socket = new Socket(closestNodeAddr.getInetAddress(), closestNodeAddr.getPort());
                    ObjectOutputStream socketOutput = new ObjectOutputStream(socket.getOutputStream());
                    socketOutput.writeObject(readDataMsg);

                    //parse reply message
                    Message replyMsg = (Message) new ObjectInputStream(socket.getInputStream()).readObject();
                    socket.close();

                    if (replyMsg.getMsgType() == Message.ERROR_MSG) {
                        System.out.println("error");
//                        throw new Exception(((ErrorMessage) replyMsg).getMsg());
                        System.out.println(((ErrorMessage)replyMsg).getMsg());
                        System.exit(1);
                    } else if(replyMsg.getMsgType() == Message.NO_DATA_MSG) {
                        System.out.println(((NoDataMsg)replyMsg).getMsg());
                    } else if (replyMsg.getMsgType() == Message.WRITE_DATA_MSG) {
                        WriteDataMsg writeDataMsg = (WriteDataMsg) replyMsg;

                        //write with data retrieved
                        data = writeDataMsg.getData();
                        System.out.println("Data found : " + printByteAsString(data));
                    } else {
                        throw new Exception("Received an unexpected message type '" + replyMsg.getMsgType() + "'.");
                    }


                }
            }

    }

    private static boolean isHexNumber(String str){
        boolean flag = true;
        for(int i=0;i<str.length();i++){
            char cc = str.charAt(i);
            if(cc!='0'&& cc!='1'&& cc!='2' && cc!='3'&& cc!='4' && cc!='5'&& cc!='6'&& cc!='7'
                    &&cc!='8'&&cc!='9'&&cc!='A'&&cc!='B'&&cc!='C'&& cc!='D'&& cc!='E'&&cc!='F'
                    &&cc!='a'&&cc!='b'&&cc!='c'&&cc!='c'&&cc!='d'&&cc!='e'&&cc!='f'){
                flag = false;
                break;
            }
        }
        return flag;
    }

    public byte[] getData() {
        return this.data;
    }

    private static NodeAddress getRandomNode(InetAddress discoveryNodeAddr, int discoveryNodePort) throws Exception{
        RequestRandomNodeMsg requestRandomNodeMsg = new RequestRandomNodeMsg();
        Socket discoveryNodeSocket = new Socket(discoveryNodeAddr, discoveryNodePort);
        ObjectOutputStream out = new ObjectOutputStream(discoveryNodeSocket.getOutputStream());
        out.writeObject(requestRandomNodeMsg);

        ObjectInputStream in = new ObjectInputStream(discoveryNodeSocket.getInputStream());
        Message replyMsg = (Message) in.readObject();
        discoveryNodeSocket.close();

        if(replyMsg.getMsgType() == Message.ERROR_MSG) {
            System.out.println(((ErrorMessage)replyMsg).getMsg());
            System.exit(1);
        } else if(replyMsg.getMsgType() != Message.NEARBY_NODE_INFO_MSG) {
            throw new Exception("Received an unexpected message type '" + replyMsg.getMsgType() + "'.");
        }
        return ((NearByNodeInfoMsg)replyMsg).getNodeAddress();
    }

    private static NodeAddress lookUpNode(NodeAddress nodeAddress, int port, int requestType) throws Exception {
        //start a new server socket to receive the connection
        ServerSocket serverSocket = new ServerSocket(port);

        //send store data message to the random node
        System.out.println("Forwarding lookup node message with id " + convertBytesToHex(id) + " to node " + nodeAddress);
        LookupNodeMsg lookupNodeMsg = new LookupNodeMsg(id, new NodeAddress(id,"ClientNode", null, port), 0,requestType);
        lookupNodeMsg.addHop(nodeAddress);

        try {
            Socket newDataSocket = new Socket(nodeAddress.getInetAddress(), nodeAddress.getPort());

            ObjectOutputStream seedOut = new ObjectOutputStream(newDataSocket.getOutputStream());
            seedOut.writeObject(lookupNodeMsg);

            newDataSocket.close();
        } catch (ConnectException e) {
            System.out.println("Cannot connect to node with id " + nodeAddress.getNodeId());

        }


        //System.out.println("Wait to receive connection from the node where the data should reside");
        //Receive connection from the node where the data should reside
        Socket nodeSocket = serverSocket.accept();
        Message replyMsg = (Message) new ObjectInputStream(nodeSocket.getInputStream()).readObject();

        //ensure we get a node info message
        if(replyMsg.getMsgType() == Message.ERROR_MSG) {
            System.out.println(((ErrorMessage)replyMsg).getMsg());
            System.exit(1);
        } else if(replyMsg.getMsgType() != Message.NEARBY_NODE_INFO_MSG) {
            throw new Exception("Received an unexpected message type '" + replyMsg.getMsgType() + "'.");
        }

        //parse reply message
        NearByNodeInfoMsg nearByNodeInfoMsg = (NearByNodeInfoMsg) replyMsg;
        if(nearByNodeInfoMsg.getNodeAddress().getInetAddress() == null) {
            nearByNodeInfoMsg.getNodeAddress().setInetAddress(nodeSocket.getInetAddress());
        }

        serverSocket.close();
        nodeSocket.close();
        return nearByNodeInfoMsg.getNodeAddress();
    }
}
