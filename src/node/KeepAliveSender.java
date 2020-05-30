package node;


import message.KeepAliveMsg;
import util.NodeAddress;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.sql.Timestamp;


import static util.Util.convertBytesToHex;

public class KeepAliveSender extends Thread{
    protected PastryNode node;
    protected long timeIntervalInMilli;

    public KeepAliveSender(PastryNode node, long timeIntervalInMilli) {
        this.node = node;
        this.timeIntervalInMilli = timeIntervalInMilli;
    }
    @Override
    public void run(){
        //TODO change while condition to isAlive
        try{
            while(true){
                if (node.leafSet.leftSet.size() != 0) {
                    byte[] leftNodeID = this.node.leafSet.leftSet.firstKey();
                    NodeAddress leftNodeAddr = this.node.leafSet.leftSet.get(leftNodeID);
                    System.out.println("Sending keep-alive message to " + convertBytesToHex(leftNodeID));
                    KeepAliveMsg keepAliveMsg = new KeepAliveMsg(node.nodeID, node.address, node.leafSet.get(this.node), new Timestamp(System.currentTimeMillis()));
                    try {
                        Socket nodeSocket = new Socket(leftNodeAddr.getInetAddress(), leftNodeAddr.getPort());
                        ObjectOutputStream keepAliveStream = new ObjectOutputStream(nodeSocket.getOutputStream());
                        keepAliveStream.writeObject(keepAliveMsg);
                        nodeSocket.close();
                    } catch (ConnectException connEx) {
                        System.err.println("Connection exception in left leaf");
                        FailureHandler handler = new FailureHandler(this.node);
                        handler.handleFail(leftNodeID,FailureHandler.LEAF_NODE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (node.leafSet.rightSet.size() != 0) {
                    byte[] rightNodeID = this.node.leafSet.rightSet.firstKey();
                    NodeAddress rightNodeAddr = this.node.leafSet.rightSet.get(rightNodeID);
                    System.out.println("Sending keep-alive message to " + convertBytesToHex(rightNodeID));
                    KeepAliveMsg rightKeepAlive = new KeepAliveMsg(node.nodeID, node.address, node.leafSet.get(this.node), new Timestamp(System.currentTimeMillis()));
                    try{
                        Socket nodeSocket = new Socket(rightNodeAddr.getInetAddress(), rightNodeAddr.getPort());
                        ObjectOutputStream rightStream = new ObjectOutputStream(nodeSocket.getOutputStream());
                        rightStream.writeObject(rightKeepAlive);
                        nodeSocket.close();
                    }catch(ConnectException connEx){
                        System.err.println("Connection Exception");
                        FailureHandler handler = new FailureHandler(this.node);
                        handler.handleFail(rightNodeID,FailureHandler.LEAF_NODE);
                    }catch (IOException ex){
                        ex.printStackTrace();
                    }
                }
                Thread.sleep(timeIntervalInMilli);
            }
        }catch (InterruptedException e) {
            System.err.println("Sleep Interrupted");
            e.printStackTrace();
        }
    }
}
