package util;

import node.PastryNode;

import java.util.Scanner;

import static util.Util.convertBytesToShort;

public class InputListner extends Thread{
    PastryNode node;
    public InputListner(){

    }
    public InputListner(PastryNode node){
        this.node = node;
    }

    @Override
    public void run() {
        //super.run();
        Scanner scn = new Scanner(System.in);
        while(true){
            String word = scn.nextLine();
            if(word.equalsIgnoreCase("print")){
                node.leafSet.print(node);
                node.routingTable.print(node);
            }else if(word.equalsIgnoreCase("test")){
                short idInShort = convertBytesToShort(node.nodeID);
                System.out.println("ID IN Short"+idInShort);
            }
        }
    }
}
