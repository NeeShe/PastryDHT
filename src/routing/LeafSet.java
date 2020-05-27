package routing;

import node.PastryNode;
import util.NodeAddress;
import util.Util;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

import static util.Util.convertBytesToHex;
import static util.Util.convertBytesToShort;

public class LeafSet {
    private int leafSize;
    public SortedMap<byte[], NodeAddress> leftSet;
    public SortedMap<byte[], NodeAddress> rightSet;

    public LeafSet(byte[] id, int leafSize) {
        this.leafSize = leafSize;
       // short idInShort = convertBytesToShort(id);
        String idInHex= convertBytesToHex(id);
        leftSet = new TreeMap<>(
                new Comparator<byte[]>() {
                    @Override
                    public int compare(byte[] id1, byte[] id2) {
                        int dist1 = getDistance(convertBytesToHex(id1), idInHex);
                        int dist2 = getDistance(convertBytesToHex(id2), idInHex);

                        if(dist1 < dist2) {
                            return 1;
                        } else if(dist1 > dist2) {
                            return -1;
                        } else {
                            return 0;
                        }
                    }
                }
        );

        rightSet = new TreeMap<>(
                new Comparator<byte[]>() {
                    @Override
                    public int compare(byte[] id1, byte[] id2) {
                        int dist1 = getDistance(idInHex, convertBytesToHex(id1));
                        int dist2 = getDistance(idInHex, convertBytesToHex(id2));

                        if(dist1 > dist2) {
                            return 1;
                        } else if(dist1 < dist2) {
                            return -1;
                        } else {
                            return 0;
                        }
                    }
                }
        );
    }
/* Do not remove until end

 */
//    private int getDistance(short id1, short id2) {
//        int dist = 0;
//        if(id1 < id2) {
//            dist = Math.abs(id2 - id1);
//        } else if(id1 > id2) {
//            dist = Math.abs(Short.MAX_VALUE - id1) + Math.abs(id2 - Short.MIN_VALUE);
//        }
//        return dist;
//    }

    private int getDistance(String hex1, String hex2) {
        int dist = 0;
        int id1 = Integer.parseInt(hex1, 16);
        int id2 = Integer.parseInt(hex2, 16);
        if(id1 < id2) {
            dist = Math.abs(id2 - id1);
        } else if(id1 > id2) {
            dist = Math.abs(Short.MAX_VALUE - id1) + Math.abs(id2 - Short.MIN_VALUE);
        }
        return dist;
    }

    public Map<byte[], NodeAddress> get(PastryNode node) {
        node.readWriteLock.readLock().lock();
        try {
            Map<byte[], NodeAddress> leafSet = new HashMap<>();
            leafSet.putAll(leftSet);
            leafSet.putAll(rightSet);
            leafSet.put(node.nodeID, new NodeAddress(node.name, null, node.port));
            return leafSet;
        } finally {
            node.readWriteLock.readLock().unlock();
        }
    }

    public boolean addNewNode(PastryNode node, byte[] newId, NodeAddress newAddress) {
        System.out.println("Updating the leaf set of node ");
        node.readWriteLock.writeLock().lock();
        try{
            //If the new node's id is equal to the current node's id
            if(Arrays.equals(node.nodeID, newId)) {
                return false;
            }

            String idInHex = convertBytesToHex(node.nodeID);
            String newIdInHex = convertBytesToHex(newId);

            int idInInt = Integer.parseInt(idInHex,16);
            int newIdInInt = Integer.parseInt(newIdInHex,16);

            //If the new node's id is equal to one of the node's id in LEFT leaf set
            for(byte[] id : this.leftSet.keySet()) {
                if(Arrays.equals(id, newId)) {
                    return false;
                }
            }
            //neetha: added additional condition
            if(leftSet.size() < this.leafSize ) {
                if(newIdInInt < idInInt){leftSet.put(newId, newAddress);}
            } else if(getDistance(newIdInHex, idInHex) < getDistance(convertBytesToHex(leftSet.firstKey()), idInHex)) {
                leftSet.remove(leftSet.firstKey());
                leftSet.put(newId, newAddress);
            }


            //If the new node's id is equal to one of the node's id in RIGHT leaf set
            for(byte[] id : this.rightSet.keySet()) {
                if (Arrays.equals(id, newId)) {
                    return false;
                }
            }
            //neetha: added additional condition
            if(rightSet.size() < this.leafSize) {
                if(newIdInInt > idInInt){rightSet.put(newId, newAddress);}
            } else if(getDistance(idInHex, newIdInHex) < getDistance(idInHex, convertBytesToHex(rightSet.firstKey()))) {
                rightSet.remove(rightSet.firstKey());
                rightSet.put(newId, newAddress);
            }
        } catch(Exception e){
            System.out.println("DEBUG: exception area2");
            e.printStackTrace();
        }finally {
            node.readWriteLock.writeLock().unlock();
        }
        return true;
    }

    public void print(PastryNode node) {
        node.readWriteLock.readLock().lock();
        try{
            System.out.println("---------------------------LeafSet-------------------------");
            System.out.println("LEFT SET");
            for(byte[] id : node.leafSet.leftSet.keySet()) {
                NodeAddress addr = node.leafSet.leftSet.get(id);
                System.out.println(convertBytesToHex(id) + " : " + addr);
            }
            //stringBuilder.append(convertBytesToHex(node.nodeID) + " : " + convertBytesToShort(node.nodeID) + " - " + node.port);
            System.out.println("RIGHT SET");
            for(byte[] id : node.leafSet.rightSet.keySet()) {
                NodeAddress addr = node.leafSet.rightSet.get(id);
                System.out.println(convertBytesToHex(id) + " : " + addr);
            }
        } finally {
            node.readWriteLock.readLock().unlock();
        }
    }

    public NodeAddress searchClosest(PastryNode node, byte[] searchId) {
        node.readWriteLock.readLock().lock();
        try{
            String idInHex = convertBytesToHex(node.nodeID);
            String searchIdInHex = convertBytesToHex(searchId);
            int idInInt = Integer.parseInt(idInHex,16);
            int searchIdInInt = Integer.parseInt(searchIdInHex,16);
            NodeAddress closestAddr = null;
            int closestDist = Math.min(getDistance(searchIdInHex, idInHex),getDistance(idInHex,searchIdInHex));
            int closest =searchIdInInt;

            for(byte[] id : leftSet.keySet()) {
                String curInHex = convertBytesToHex(id);
                int curInInt = Integer.parseInt(curInHex,16);
                int dist = Math.min(getDistance(searchIdInHex,curInHex),getDistance(curInHex,searchIdInHex));
                if(dist < closestDist || (dist == closestDist && closest < curInInt)) {
                    closest = curInInt;
                    closestDist = dist;
                    closestAddr = leftSet.get(id);
                    System.out.println("Found in left set");
                }
            }

            for(byte[] id : rightSet.keySet()) {
                String curInHex = convertBytesToHex(id);
                int curInInt = Integer.parseInt(curInHex,16);
                int dist = Math.min(getDistance(searchIdInHex, curInHex), getDistance(curInHex, searchIdInHex));
                if(dist < closestDist || (dist == closestDist && closest > curInInt)) {
                    closest = curInInt;
                    closestDist = dist;
                    closestAddr = rightSet.get(id);
                    System.out.println("Found in right set");
                }
            }
            if(closestAddr == null){
                System.out.println("Not found");
                return new NodeAddress(node.name,null,node.port);
            }
            return closestAddr;
        } finally {
            node.readWriteLock.readLock().unlock();
        }
    }
}
