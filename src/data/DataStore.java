package data;

import node.PastryNode;

import java.util.HashMap;
import java.util.Map;

import static util.Util.printByteAsString;

public class DataStore {
    public Map<String, byte[]> ownedData;
    public Map<String, byte[]> replicatedData;

    public DataStore(){
        this.ownedData = new HashMap<>();
        this.replicatedData = new HashMap<>();
    }

    public String searchDataID(PastryNode node, String searchId){
        node.readWriteLock.readLock().lock();
        try {
            if (node.dataStore.ownedData.containsKey(searchId)) {
                return printByteAsString(node.dataStore.ownedData.get(searchId));
            } else if (node.dataStore.replicatedData.containsKey(searchId)) {
                return printByteAsString(node.dataStore.replicatedData.get(searchId));
            }
        } finally {
            node.readWriteLock.readLock().unlock();
        }
        return "";
    }

    public byte[] getByteData(PastryNode node, String dataId){
        node.readWriteLock.readLock().lock();
        try {
            if (node.dataStore.ownedData.containsKey(dataId)) {
                return node.dataStore.ownedData.get(dataId);
            } else if (node.dataStore.replicatedData.containsKey(dataId)) {
                return node.dataStore.replicatedData.get(dataId);
            }
        } finally {
            node.readWriteLock.readLock().unlock();
        }
        return null;
    }
    public void printData(PastryNode node) {
        System.out.println("---------------Data Stored----------------");
        System.out.println("---------------Owned Data----------------");
        for(String dataId : node.dataStore.ownedData.keySet()) {
            System.out.println("id = " + dataId + "  :  data = " + printByteAsString(node.dataStore.ownedData.get(dataId)));
        }
        System.out.println("---------------Replicated Data----------------");
        for(String dataId : node.dataStore.replicatedData.keySet()) {
            System.out.println("id = " + dataId + "  :  data = " + printByteAsString(node.dataStore.replicatedData.get(dataId)));
        }
    }
}
