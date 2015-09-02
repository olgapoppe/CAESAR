package window;

import java.util.ArrayList;

public class WindowDistribution {
	
	public static void main (String args[]) {
		
		int lambda = 10;
		int expensive_window_number = 3;
		
		int lastSec = 180;
		int window_length = 70;	
		
		/* int count = 0;
		 * while (count < 100) {
			System.out.println(expensive_window_number + " poisson numbers with lambda " + lambda + " are: " + getPoissonNumbers(lastSec,window_length,lambda,expensive_window_number));
			count++;
		}*/
		
		System.out.println(expensive_window_number + " uniform numbers are: " + getUniformNumbers(lastSec,window_length,expensive_window_number));
	}
	
	public static ArrayList<Integer> getPoissonNumbers (int lastSec, int window_length, int expensive_window_number, double lambda) {
		
		ArrayList<Integer> results = new ArrayList<Integer> ();
		int total_window_number = lastSec/window_length;
			
		while (results.size() < expensive_window_number) {
			
			int result = getPoisson(lambda);
			if (!results.contains(result) && result <= total_window_number) results.add(result);	
		}
		return results;
	}
	
	public static int getPoisson (double lambda) {
		
		double L = Math.exp(-lambda);
		double p = 1.0;
		int k = 0;

		do {
			k++;
		    p *= Math.random();
		} while (p > L);

		return k - 1;
	}
	
	public static ArrayList<Integer> getUniformNumbers (int lastSec, int window_length, int expensive_window_number) {
		  
		ArrayList<Integer> results = new ArrayList<Integer>();
		
		/*** Get total window number ***/
		int total_window_number = lastSec/window_length;
		// Not enough windows
		if (total_window_number < expensive_window_number) {
			System.err.println(	"Total window number is: " + total_window_number + 
								" Expensive window number: " + expensive_window_number + "\n");			
		} else {
			// Exact number of windows
			if (total_window_number == expensive_window_number) {
				int result = 0;		
				while (result < expensive_window_number) {
					results.add(result);
					result++;
				}
			} else {			
				int cheap_window_number_inbetween = total_window_number/(expensive_window_number+1) - 1;				
				if (cheap_window_number_inbetween == 0) {					
					if (expensive_window_number*2-1 > total_window_number) {
						// Without cheap windows in-between
						int count = 0;
						int result = 0;		
						while (count < expensive_window_number) {
							results.add(result);
							result++;
							count++;
						}
					} else {
						// With 1 cheap window in-between
						int count = 0;
						int result = 0;		
						while (count < expensive_window_number) {			
							results.add(result);
							result += 2;
							count++;
						}
					}
				} else {
					// With cheap windows in-between
					int current_window = cheap_window_number_inbetween;
					
					int count = 0;		
					while (count < expensive_window_number) {	
						int result = current_window + 1;
						results.add(result);
						current_window = result + cheap_window_number_inbetween;
						count++;
					}
				}
			}
		}			
		return results;
	}
}
