package routing;

import node.PastryNode;
import util.NodeAddress;

import javax.xml.soap.Node;
import java.util.HashMap;
import java.util.Map;

//Routing table maintains information about several peers in the system
//All peer identifiers are in hexadecimal format
//and the routing table classify identifiers based on their hexadecimal prefix
//The table has as many rows as the digits of a hexadecimal identifier have
//Hence, based on our 16 bits id, there are 16/4 = 4 rows
public class RoutingTable {
    private Map<String, NodeAddress>[] routingTable;

    public RoutingTable() {
        this.routingTable = new Map[4];
        for(Map<String, NodeAddress> map : routingTable) {
            map = new HashMap<>();
        }
    }

    public Map<String, NodeAddress> get(PastryNode node, int prefixLen) {
        node.readWriteLock.readLock().lock();
        try{
            return routingTable[prefixLen];
        } finally {
            node.readWriteLock.readLock().unlock();
        }
    }

    public boolean addNewNode(PastryNode node, String newIdStr, NodeAddress newAddress, int prefixLen) {
        node.readWriteLock.writeLock().lock();
        try {
            if (node.idStr.substring(prefixLen, prefixLen + 1).equals(newIdStr)) {
                return false;
            }
            if (!routingTable[prefixLen].containsKey((newIdStr))) {
                routingTable[prefixLen].put(newIdStr, newAddress);
                return true;
            }
        } finally {
            node.readWriteLock.writeLock().unlock();
        }
        return false;
    }
}
