package myCode;

import java.util.HashMap;
import java.util.Map;

public class MainTest {

    public static void main(String[] args) {
        BPlusTree<Integer, String> myTree = new BPlusTree<Integer, String>(8);
        
        int max = 1000000;
        long start = System.currentTimeMillis();
        for(int i = 0; i < max; i++) {
            myTree.set(i, String.valueOf(i));
        }
        System.out.println("time cost with BPlusTree: " + (System.currentTimeMillis() - start));
        System.out.println("Data has been inserted into tree");
        
        System.out.println("height: " + myTree.height());
        
        start = System.currentTimeMillis();
        Map<Integer, String> hashMap = new HashMap<Integer, String>();
        for (int i = 0; i < max; i++) {
            hashMap.put(i, String.valueOf(i));
        }
        System.out.println("time cost with HashMap: " + (System.currentTimeMillis() - start));
        
        for (int i = 0; i < max; i++) {
            if (!String.valueOf(i).equals(myTree.get(i))) {
                System.err.println("error for: " + i);
            }
        }
        
        System.out.println("Success");
    }
}
