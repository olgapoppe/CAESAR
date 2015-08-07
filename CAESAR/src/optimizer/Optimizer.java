package optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import operator.*;

public class Optimizer {
	
	static void omit (List<Operator> query_plan, int k) {
		
		for (Operator operator : query_plan) {
			// if (operator.omittable()) {
				 
			 //}
		}
		
	}
	
	static void permute (List<Operator> arr, int k) {
        for(int i = k; i < arr.size(); i++){
            Collections.swap(arr, i, k);
            permute(arr, k+1);
            Collections.swap(arr, k, i);
        }
        if (k == arr.size()-1){
            System.out.println(Arrays.toString(arr.toArray()));
        }
    }
    public static void main(String[] args) {
    	
    	ArrayList<Operator> query_plan = new ArrayList<Operator>();
    	
    	
    	
    	//Optimizer.permute(Arrays.asList(1,2,3), 0);
    }
}
