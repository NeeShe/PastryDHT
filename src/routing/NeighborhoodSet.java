package routing;

import node.PastryNode;
import util.NodeAddress;

import java.util.*;

import static util.Util.convertBytesToHex;
import static util.Util.convertBytesToShort;


public class NeighborhoodSet {
    private int neighborSize;
    public SortedMap<byte[], NodeAddress> neighborSet;

    public NeighborhoodSet(byte[] id, int neighborSize) {
        this.neighborSize = neighborSize;
        short idInShort = convertBytesToShort(id);
        neighborSet = new TreeMap(
                new Comparator<byte[]>() {
                    @Override
                    public int compare(byte[] id1, byte[] id2) {
                        int dist1 = getDistance(convertBytesToShort(id1), idInShort);
                        int dist2 = getDistance(convertBytesToShort(id2), idInShort);

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
    }

    private int getDistance(short id1, short id2) {
        int dist = 0;
        if(id1 < id2) {
            dist = Math.abs(id2 - id1);
        } else if(id1 > id2) {
            dist = Math.abs(Short.MAX_VALUE - id1) + Math.abs(id2 - Short.MIN_VALUE);
        }
        return dist;
    }

    public boolean addNewNode(PastryNode node, byte[] newId, NodeAddress newAddress) {
        System.out.println("Updating the Neighborhood set of node ");
        node.readWriteLock.writeLock().lock();
        try{
            //If the new node's id is equal to the current node's id
            if(Arrays.equals(node.nodeID, newId)) {
                return false;
            }

            short idInShort = convertBytesToShort(node.nodeID);
            short newIdInShort = convertBytesToShort(newId);

            //If the new node's id is equal to one of the node's id in neighborhood set
            if(neighborSet.containsKey(newId)){
                return false;
            }

            //else add if there is room
            //or check if the new node is numerically farthest from the node. If yes add it.
            // This is different compared to leaf set
            //numerically closest have high probability getting added in leaf set and routing table
            //if we add those nodes which are not numerically closest but geographacilly closest, we can have good coverage
            if(neighborSet.size() < this.neighborSize ) {
                neighborSet.put(newId,newAddress);
            } else if(getDistance(newIdInShort, idInShort) > getDistance(convertBytesToShort(neighborSet.firstKey()), idInShort)) {
                neighborSet.remove(neighborSet.firstKey());
                neighborSet.put(newId, newAddress);
            }
        } catch(Exception e){
            System.out.println("DEBUG: exception area in Neighborhood Set");
            e.printStackTrace();
        }finally {
            node.readWriteLock.writeLock().unlock();
        }
        return true;
    }
    public Map<byte[], NodeAddress> get(PastryNode node) {
        node.readWriteLock.readLock().lock();
        try {
            Map<byte[], NodeAddress> nSet = new HashMap<>();
            nSet.putAll(neighborSet);
            return nSet;
        } finally {
            node.readWriteLock.readLock().unlock();
        }
    }

    public void print(PastryNode node) {
        node.readWriteLock.readLock().lock();
        try{
            System.out.println("------------------------NeighborhoodSet-------------------------");
            for(byte[] id : node.neighborhoodSet.neighborSet.keySet()) {
                NodeAddress addr = node.neighborhoodSet.neighborSet.get(id);
                System.out.println(convertBytesToHex(id) + " : " + addr);
            }
        } finally {
            node.readWriteLock.readLock().unlock();
        }
    }
}
