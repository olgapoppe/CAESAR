package window;

import java.util.ArrayList;

public class WindowDistribution {
	
	public static void main (String args[]) {
		
		/*** Input parameters ***/	
		int lastSec = 10784;
		int window_length = 300;	
		int expensive_window_number = 2;
		
		int window_distribution = 1; // 0 for uniform, 1 for Poisson
		int window_center = 10784;
		
		if (window_distribution == 0) {			
		
			/*** Uniform window distribution ***/
			ArrayList<TimeInterval> timeIntervals = getTimeIntervalsForUniformDistribution(lastSec, window_length, expensive_window_number);
			int count = 1;
			for (TimeInterval i : timeIntervals) {
				String s = i.toString();
				if (count % 5 == 0) s+="\n";
				System.out.print(s);
				count++;
			}
		} else { 
		
			/*** Poisson window distribution ***/
			int lambda = window_center/window_length + 1;
			System.out.println("Central window is: " + lambda);
			int count_1 = 0;
			while (count_1 < 5) {
				System.out.println(	expensive_window_number + " poisson numbers with lambda " + lambda + " are: " + 
									getTimeIntervalsForPoissonDistribution(lastSec, window_length, expensive_window_number, lambda));
				count_1++;
			}
		}
	}
	
	public static ArrayList<TimeInterval> getTimeIntervalsForPoissonDistribution (double lastSec, int window_length, int expensive_window_number, int lambda) {
		
		/*** Get expensive windows ***/
		ArrayList<Integer> expensive_windows = getPoissonNumbers(lastSec, window_length, expensive_window_number, lambda);
		
		/*** Get expensive time intervals ***/
		ArrayList<TimeInterval> results = new ArrayList<TimeInterval>();
		for (Integer expensive_window : expensive_windows) {
			
			double start = new Double(expensive_window * window_length + 1).intValue();
			double end = new Double((expensive_window + 1) * window_length).intValue();
			
			TimeInterval i = new TimeInterval(start,end);
			results.add(i);			
		}		
		return results;
	}
	
	public static ArrayList<Integer> getPoissonNumbers (double lastSec, int window_length, int expensive_window_number, double lambda) {
		
		ArrayList<Integer> results = new ArrayList<Integer> ();
		int total_window_number = new Double(lastSec).intValue()/window_length - 1;
			
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
	
	public static ArrayList<TimeInterval> getTimeIntervalsForUniformDistribution (double lastSec, int window_length, int expensive_window_number) {
		 	
		/*** Get cheap window length ***/
		double expensive_time = expensive_window_number * window_length;
		double cheap_time = lastSec - expensive_time;
		double cheap_window_length_double = cheap_time/(expensive_window_number + 1); 
		int cheap_window_length = new Double(cheap_window_length_double).intValue();
		if (cheap_window_length_double - cheap_window_length > 0.5) cheap_window_length++;
		
		/*** Get expensive time intervals ***/
		ArrayList<TimeInterval> results = new ArrayList<TimeInterval>();
		double window_bound = cheap_window_length;
		while (results.size() < expensive_window_number) {
			
			double start = new Double(window_bound).intValue();
			double end = new Double(window_bound + window_length - 1).intValue();
			
			if (start < 0 || end > lastSec) {
				System.err.println("Data bounds are exceeded by the time interval [" + start + "," + end + "]!");
				break;
			} else {
				TimeInterval i = new TimeInterval(start,end);
				results.add(i);
			}		
			window_bound = end + 1 + cheap_window_length;
		}			
		return results;
	}
}
