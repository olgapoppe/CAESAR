package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import event.*;

/**
 * Input file generator parses the input file and copies certain tuples to the output file.  
 * @author Olga Poppe
 */
public class InputFileGenerator {
	
	public static void main (String[] args) {
	
		//int lastSec = 10784;
		
		//Scanner input1 = null;
		//Scanner input2 = null;
		Scanner input = null;
		try {		
			/*** Input file ***/
			//File input_file_1 = new File("../../Dropbox/LR/InAndOutput/2xways/input-0.dat");
			//File input_file_2 = new File("../../Dropbox/LR/InAndOutput/2xways/input-1.dat");
			//File input_file = new File("../../Dropbox/LR/InAndOutput/2xways/input7.dat");
			//File input_file = new File("../../Dropbox/LR/InAndOutput/2xways/cardatapoints.out0");
			//input1 = new Scanner(input_file_1);  			
			//input2 = new Scanner(input_file_2);
					
			/*** Output file ***/
            File output_file = new File("../../input.dat");
            input = new Scanner(output_file);
            
           // BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            
            // Integer.parseInt(args[0]);
            // 357853 events are within the first for 1500 seconds 
            
            /*** Call method ***/            
            //changeXway(input,output,1);
            //zip2files(input1,input2,output,lastSec);
            countTuples(input);
            
            /*** Close the files ***/
       		//input1.close();
       		//input2.close();
       		input.close();
       		//output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	public static void zip2files (Scanner input1, Scanner input2, BufferedWriter output, int lastSec) {
		
		String eventString1 = input1.nextLine();
		String eventString2 = input2.nextLine();
		PositionReport event1 = PositionReport.parse(eventString1);
		PositionReport event2 = PositionReport.parse(eventString2);
		double curr_sec = 0;
		
		try {
			
			while (curr_sec <= lastSec) {							
				
				while (event1 != null && event1.sec == curr_sec) {
						
						// Write event1
						output.write(eventString1 + "\n");
						
						// Reset event1
						if (input1.hasNextLine()) {
							eventString1 = input1.nextLine();
							event1 = PositionReport.parse(eventString1);
						} else {
							event1 = null;
						}
				} 		
				while (event2 != null && event2.sec == curr_sec) {
					
					// Write event2
					output.write(eventString2 + "\n");
					
					// Reset event2
					if (input2.hasNextLine()) {
						eventString2 = input2.nextLine();
						event2 = PositionReport.parse(eventString2);
					} else {
						event2 = null;
					}
				} 	
				curr_sec++;
			}
		} catch (IOException e) { System.err.println(e); }		
	}
	
	/**
	 * Copy position reports from input to output and change their xway to the given value 
	 * @param input
	 * @param output
	 * @param new xway
	 */
	public static void changeXway (Scanner input, BufferedWriter output, int newXway) {
		
		String eventString = "";		
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				PositionReport event = PositionReport.parse(eventString);
				if (event.type == 0) output.write(event.toString(newXway) + "\n");            	            	            	         	
			}   
		} catch (IOException e) { System.err.println(e); }
	}
	
	/**
	 * Copy position reports that have the given direction from input to output
	 * @param input
	 * @param output
	 * @param dir
	 */
	public static void selectTuples (Scanner input, BufferedWriter output, int dir) {
		
		String eventString = "";		
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				PositionReport event = PositionReport.parse(eventString);
				if (event.type == 0 && event.dir == 0) output.write(eventString + "\n");            	            	            	         	
			}   
		} catch (IOException e) { System.err.println(e); }	
	}
	
	/**
	 * Copy the given number of tuples from input to output
	 * @param input
	 * @param output
	 * @param tuple number
	 */
	public static void copyTuples (Scanner input, BufferedWriter output, int tupleNumber) {
		
		String eventString = "";
		int count = 0; 
		try {
			while (input.hasNextLine() && count < tupleNumber) {         	
        		
				count++;
				eventString = input.nextLine();
				output.write(eventString + "\n");            	            	            	         	
			}   
		} catch (IOException e) { System.err.println(e); }				
	}
	
	/**
	 * Count the number of tuples in the input and print it and the last tuple
	 * @param input 
	 */
	public static void countTuples (Scanner input) {
		
		String eventString = "";	
		int count = 0; 
		while (input.hasNextLine()) {         	
        			
			count++;
			eventString = input.nextLine();
		} 
		System.out.println("Count: " + count + " Last event: " + eventString);		
	}
}
