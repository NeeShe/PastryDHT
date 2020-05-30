package node;

import data.LeafNodeInfoStore.NodeInfo;

import java.sql.Timestamp;

import static util.Util.convertBytesToHex;

public class FailureDetector extends Thread{
    protected PastryNode node;
    protected long failCheckTimeInMilli;
    public FailureDetector(PastryNode node, long timeIntervalInMilli) {
        this.node = node;
        this.failCheckTimeInMilli = timeIntervalInMilli;
    }
    @Override
    public void run() {
        try {
            //TODO change while condition to isAlive
            while (true) {
                for(byte[] node: this.node.leafNodeInfoStore.nodeSet.keySet()){
                    Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
                    Timestamp nodeTimestamp = this.node.leafNodeInfoStore.getTimestamp(node);
                    // if the last keep alive received is older than 2*T
                    if(currentTimestamp.getTime() - nodeTimestamp.getTime() > (2*failCheckTimeInMilli) ){
                        System.err.println("Node Failed: "+node);
                    }else{
                        System.err.println("Node alive: "+node);
                    }

                }
                Thread.sleep(failCheckTimeInMilli);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
