package routing;

import node.PastryNode;
import util.NodeAddress;

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
        short idInShort = convertBytesToShort(id);

        leftSet = new TreeMap(
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

        rightSet = new TreeMap<>(
                new Comparator<byte[]>() {
                    @Override
                    public int compare(byte[] id1, byte[] id2) {
                        int dist1 = getDistance(idInShort, convertBytesToShort(id1));
                        int dist2 = getDistance(idInShort, convertBytesToShort(id2));

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

    public Map<byte[], NodeAddress> get(PastryNode node) {
        System.out.println("Getting the leaf set of node " + node.nodeID);
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
        System.out.println("Updating the leaf set of node " + node.nodeID);
        node.readWriteLock.writeLock().lock();
        try{
            //If the new node's id is equal to the current node's id
            if(Arrays.equals(node.nodeID, newId)) {
                return false;
            }

            short idInShort = convertBytesToShort(node.nodeID);
            short newIdInShort = convertBytesToShort(newId);

            //If the new node's id is equal to one of the node's id in LEFT leaf set
            for(byte[] id : this.leftSet.keySet()) {
                if(Arrays.equals(id, newId)) {
                    return false;
                }
            }
            if(leftSet.size() < this.leafSize) {
                leftSet.put(newId, newAddress);
            } else if(getDistance(newIdInShort, idInShort) < getDistance(convertBytesToShort(leftSet.firstKey()), idInShort)) {
                leftSet.remove(leftSet.firstKey());
                leftSet.put(newId, newAddress);
            }


            //If the new node's id is equal to one of the node's id in RIGHT leaf set
            for(byte[] id : this.rightSet.keySet()) {
                if (Arrays.equals(id, newId)) {
                    return false;
                }
            }
            if(rightSet.size() < this.leafSize) {
                rightSet.put(newId, newAddress);
            } else if(getDistance(idInShort, newIdInShort) < getDistance(idInShort, convertBytesToShort(rightSet.firstKey()))) {
                rightSet.remove(rightSet.firstKey());
                rightSet.put(newId, newAddress);
            }
        } finally {
            node.readWriteLock.writeLock().unlock();
        }
        return true;
    }

    public void print(PastryNode node) {
        node.readWriteLock.readLock().lock();
        try{
            StringBuilder stringBuilder = new StringBuilder("---------------------------LeafSet-------------------------");
            for(byte[] id : node.leafSet.leftSet.keySet()) {
                NodeAddress addr = node.leafSet.leftSet.get(id);
                stringBuilder.append(convertBytesToHex(id) + " : " + convertBytesToShort(id) + " - " + addr);
            }
            stringBuilder.append(convertBytesToHex(node.nodeID) + " : " + convertBytesToShort(node.nodeID) + " - " + node.port);
            for(byte[] id : node.leafSet.rightSet.keySet()) {
                NodeAddress addr = node.leafSet.rightSet.get(id);
                stringBuilder.append(convertBytesToHex(id) + " : " + convertBytesToShort(id) + " - " + addr);
            }
            stringBuilder.append("/n--------------------------------");
            System.out.println(stringBuilder.toString());
        } finally {
            node.readWriteLock.readLock().unlock();
        }
    }

    public NodeAddress searchClosest(PastryNode node, byte[] searchId) {
        node.readWriteLock.readLock().lock();
        try{
            short idInShort = convertBytesToShort(node.nodeID);
            short searchIdInShort = convertBytesToShort(searchId);
            short closest = searchIdInShort;
            NodeAddress closestAddr = node.address;
            int closestDist = Math.min(getDistance(searchIdInShort, idInShort), getDistance(idInShort, searchIdInShort));

            for(byte[] id : leftSet.keySet()) {
                short cur = convertBytesToShort(id);
                int dist = Math.min(getDistance(searchIdInShort, cur), getDistance(cur, searchIdInShort));
                if(dist < closestDist || (dist == closestDist && closest < cur)) {
                    closest = cur;
                    closestDist = dist;
                    closestAddr = leftSet.get(id);
                }
            }

            for(byte[] id : rightSet.keySet()) {
                short cur = convertBytesToShort(id);
                int dist = Math.min(getDistance(searchIdInShort, cur), getDistance(cur, searchIdInShort));
                if(dist < closestDist || (dist == closestDist && closest < cur)) {
                    closest = cur;
                    closestDist = dist;
                    closestAddr = rightSet.get(id);
                }
            }

            return closestAddr;
        } finally {
            node.readWriteLock.readLock().unlock();
        }
    }
}
