package node;

import java.sql.Timestamp;
import static util.Util.convertHexToBytes;

public class FailureDetectorThread extends Thread{
    protected PastryNode node;
    protected long failCheckTimeInMilli;

    public FailureDetectorThread(PastryNode node, long timeIntervalInMilli) {
        this.node = node;
        this.failCheckTimeInMilli = timeIntervalInMilli;
    }
    @Override
    public void run() {
        while (true) {
            try{
                for(String leafNodeId: this.node.leafNodeInfoStore.nodeSet.keySet()){
                    Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
                    Timestamp nodeTimestamp = this.node.leafNodeInfoStore.getTimestamp(node,leafNodeId);
                    // if the last keep alive received is older than 2*T
                    if(currentTimestamp.getTime() - nodeTimestamp.getTime() > (2*failCheckTimeInMilli) ){
                        System.err.println("Node Failed: "+ leafNodeId);
                        this.node.failureHandler.handleFail(node,convertHexToBytes(leafNodeId), FailureHandler.LEAF_NODE,FailureHandler.DISSEMINATE);
                    }
                }
                Thread.sleep(failCheckTimeInMilli);
            }catch (Exception e) {
                //System.err.println("DEBUG"+e.getMessage());
                //e.printStackTrace();
                //rerun even if exception
            }
        }
    }

}
