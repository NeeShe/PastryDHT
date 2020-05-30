package node;

import util.NodeAddress;
import util.Util;

import java.io.Serializable;
import java.util.Map;

public class FailureHandler implements Serializable {
    public static final int LEAF_NODE = 0,
                            ROUTE_NODE = 1,
                            NEIGHBOR_NODE = 2;
    public PastryNode node;

    FailureHandler(PastryNode node){
        this.node = node;
    }

    public void handleFail(byte[] nodeId, int nodeType){
        switch (nodeType){
            case LEAF_NODE: this.handleLeafFail(nodeId);
                            break;
            default:System.err.println("Handle fail types");
        }
    }

    private void handleLeafFail(byte[] nodeId) {
        //remove if found in neighbourSet
        if(this.node.neighborhoodSet.neighborSet.containsKey(nodeId)){
            this.node.neighborhoodSet.neighborSet.remove(nodeId);
        }
        //remove from routing table

        String nodeIDStr = Util.convertBytesToHex(nodeId);
        int i=0;
        for(i=0; i<4; i++) { //find matching characters to find the row
            if(node.idStr.charAt(i) != nodeIDStr.charAt(i)) {
                nodeIDStr = "" + nodeIDStr.charAt(i);
                break;
            }
        }
        this.node.routingTable.removeFailNode(node,nodeIDStr, i);
        //remove from
        if(this.node.leafSet.leftSet.containsKey(nodeId)){
            //remove from leftSet
            this.node.leafSet.leftSet.remove(nodeId);

        }
    }
}
