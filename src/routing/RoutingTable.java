package routing;

import node.PastryNode;
import util.NodeAddress;

import javax.xml.soap.Node;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static util.Util.*;

//Routing table maintains information about several peers in the system
//All peer identifiers are in hexadecimal format
//and the routing table classify identifiers based on their hexadecimal prefix
//The table has as many rows as the digits of a hexadecimal identifier have
//Hence, based on our 16 bits id, there are 16/4 = 4 rows
public class RoutingTable {
    private List<Map<String, NodeAddress>> routingTable;    //each element of the table list is a row of (id, addr) pairs
    private final int tableSize = 4;

    public RoutingTable() {
        this.routingTable = new ArrayList<>(tableSize);
        for(Map<String, NodeAddress> map : routingTable) {
            map = new HashMap<>();
        }
    }

    public Map<String, NodeAddress> get(PastryNode node, int prefixLen) {
        node.readWriteLock.readLock().lock();
        try{
            return routingTable.get(prefixLen);
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
            if (!routingTable.get(prefixLen).containsKey((newIdStr))) {
                routingTable.get(prefixLen).put(newIdStr, newAddress);
                return true;
            }
        } finally {
            node.readWriteLock.writeLock().unlock();
        }
        return false;
    }

    public void print(PastryNode node) {
        node.readWriteLock.readLock().lock();
        try{
            StringBuilder stringBuilder = new StringBuilder("------------------------Routing Table----------------------");
            int i = 0;
            for(Map<String, NodeAddress> map : routingTable) {
                stringBuilder.append("\n prefix length = " + i);
                for(String id : map.keySet()) {
                    stringBuilder.append("\n\t" + id + " : " + map.get(id));
                }
                i++;
            }
            stringBuilder.append("/n--------------------------------");
            System.out.println(stringBuilder.toString());
        } finally {
            node.readWriteLock.readLock().unlock();
        }
    }

//    public NodeAddress searchExact(PastryNode node, byte[] id, int rowIndex) {
//        node.readWriteLock.readLock().lock();
//        try{
//            String idStr = convertBytesToHex(id).substring(rowIndex, rowIndex + 1);
//            return routingTable.get(rowIndex).get(idStr);
//        } finally {
//            node.readWriteLock.readLock().unlock();
//        }
//    }

    public NodeAddress searchClosest(PastryNode node, byte[] searchId, int prefixLen) {
        node.readWriteLock.readLock().lock();
        try {
            String searchIdStr = convertBytesToHex(searchId);
            short closest = (short)Integer.parseInt(node.idStr.substring(prefixLen, prefixLen+1));
            int closestDist = getHexDistance(node.idStr.substring(prefixLen, prefixLen+1), searchIdStr.substring(prefixLen, prefixLen+1));
            NodeAddress closestAddr = node.address;
            String idStr = convertBytesToHex(searchId);
            for(String id : routingTable.get(prefixLen).keySet()) {
                short cur = (short)Integer.parseInt(id);
                int dist = getHexDistance(id.substring(prefixLen, prefixLen+1), searchIdStr.substring(prefixLen, prefixLen+1));
                if(dist < closestDist ||(dist == closestDist && cur < closest)) {
                    closest = cur;
                    closestDist = dist;
                    closestAddr = routingTable.get(prefixLen).get(id);
                }
            }
            return closestAddr;
        } finally {
            node.readWriteLock.readLock().unlock();
        }
    }
}
