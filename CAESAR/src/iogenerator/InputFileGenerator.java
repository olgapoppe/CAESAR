package iogenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import event.*;

/***
 * Input file generator parses the input file and copies certain tuples to the output file.  
 * @author Olga Poppe
 */
public class InputFileGenerator {
	
	/***
	 * Generate input files
	 * @param args: action: 1 for clean file, 
	 * 						2 for merge files, 
	 * 						3 for select tuples with direction, 
	 * 						4 for copy n tuples, 
	 * 						5 for copy tuples from second
	 * 				path : src/input/ or ../../input/ or ../../../Dropbox/LR/InAndOutput/10xways/
	 * 				if clean file: input file, output file, xway
	 * 				if merge files: first input file, second input file, output file
	 * 				if select tuples: input file, output file, direction
	 * 				if copy n tuples: input file, output file, n
	 * 				if copy tuples from second: input file, output file, second from, second to
	 */
	public static void main (String[] args) {
		
		/*** Validate the input parameter ***/
		if (args.length < 5) {
			System.out.println("At least 5 input parameters are expected.");
			return;
		}	
		
		/*** Instantiate local variables ***/
		int action = Integer.parseInt(args[0]);
		String path = args[1];
				
		/*** Clean file ***/		
		if (action == 1) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			int xway = Integer.parseInt(args[4]);
			System.out.println("The file " + inputfile + " is cleaned, the xway is set to " + xway + ", the result is saved in the file" + outputfile);
			cleanFile(inputfile,outputfile,xway);			
		} 
		/*** Merge file ***/
		if (action == 2) {
			
			String inputfile1 = path + args[2];
			String inputfile2 = path + args[3];
			String outputfile = path + args[4];
			System.out.println("The files " + inputfile1 + " and " + inputfile2 + " are merged into the file" + outputfile);
			mergeFiles(inputfile1,inputfile2,outputfile);
		}	
		/*** Select tuples with given direction ***/
		if (action == 3) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			int dir = Integer.parseInt(args[4]);
			System.out.println("All events with direction " + dir + " from the file " + inputfile + " are copied to the file" + outputfile);
			getTuples(1,inputfile,outputfile,dir,0);
		}	
		/*** Copy n tuples ***/
		if (action == 4) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			int n = Integer.parseInt(args[4]);
			System.out.println(n + " events from the file " + inputfile + " are copied to the file" + outputfile);
			getTuples(2,inputfile,outputfile,n,0);
		}	
		/*** Copy tuples from second ***/
		if (action == 5) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			int from = Integer.parseInt(args[4]);
			int to = Integer.parseInt(args[5]);
			System.out.println("Events from second " + from + " to second " + to + " from the file " + inputfile + " are copied to the file" + outputfile);
			getTuples(3,inputfile,outputfile,from,to);
		}	
		System.out.println("Done!");
	}
	
	/****************************************************************************
	 * Select correct position reports and change xway
	 * @param inputfilename
	 * @param outputfilename
	 * @param xway
	 */
	public static void cleanFile (String inputfilename, String outputfilename, int xway) {
		
		Scanner input = null;
		try {		
			/*** Input file ***/
			File input_file = new File(inputfilename);
			input = new Scanner(input_file);  			
								
			/*** Output file ***/
            File output_file = new File(outputfilename);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));            
              
            /*** Call method ***/            
            changeXway(input,output,xway);
            
            /*** Close the files ***/
       		input.close();
       		output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	/***
	 * Copy position reports from input to output and change their xway to the given value 
	 * @param input
	 * @param output
	 * @param new xway
	 */
	public static void changeXway (Scanner input, BufferedWriter output, int newXway) {
		
		String eventString = "";
		int count = 0; 
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				PositionReport event = PositionReport.parse(eventString);
				
				if (event.correctPositionReport()) {
					
					count++;
					output.write(event.toStringChangeXway(newXway) + "\n");            	            	            	         	
			}}			
		} catch (IOException e) { System.err.println(e); }
		System.out.println("Count: " + count + " Last event: " + eventString);
	}
	
	/****************************************************************************
	 * Merge 2 files
	 * @param filename1
	 * @param filename2
	 */
	public static void mergeFiles (String inputfilename1, String inputfilename2, String outputfilename) {
		
		int lastSec = 10784;
		Scanner input1 = null;
		Scanner input2 = null;
		try {		
			/*** Input file ***/
			File input_file_1 = new File(inputfilename1);
			File input_file_2 = new File(inputfilename2);
			input1 = new Scanner(input_file_1);  			
			input2 = new Scanner(input_file_2);
					
			/*** Output file ***/
            File output_file = new File(outputfilename);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
            
            /*** Call method ***/            
            merge(input1,input2,output,lastSec);
                       
            /*** Close the files ***/
       		input1.close();
       		input2.close();
       		output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	/***
	 * Merges 2 sorted files input1 and input2 into one sorted file output. The files are sorted by time stamps. 
	 * @param input1
	 * @param input2
	 * @param output
	 * @param lastSec last second in both input files
	 */
	public static void merge (Scanner input1, Scanner input2, BufferedWriter output, int lastSec) {
		
		String eventString1 = input1.nextLine();
		String eventString2 = input2.nextLine();
		PositionReport event1 = PositionReport.parse(eventString1);
		PositionReport event2 = PositionReport.parse(eventString2);
		double curr_sec = 0;
		int count = 0; 
		
		try {
			
			while (curr_sec <= lastSec) {							
				
				while (event1 != null && event1.sec == curr_sec) {
					
					count++;
						
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
					
					count++;
					
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
		System.out.println("Count: " + count + " Last event: " + eventString2);
	}
	
	/****************************************************************************
	 * Copy all tuples with given direction or first n tuples
	 * @param choice: 1 for select tuples with given direction, 2 for copy first n tuples
	 * @param inputfilename
	 * @param outputfilename
	 * @param dir or n
	 */
	public static void getTuples (int choice, String inputfilename, String outputfilename, int n, int to) {
		
		Scanner input = null;
		try {		
			/*** Input file ***/
			File input_file = new File(inputfilename);
	        input = new Scanner(input_file);     
	        
	        /*** Output file ***/
            File output_file = new File(outputfilename);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
	            
	        /*** Call method ***/    
            if (choice==1) {
            	selectTuples(input,output,n);
            } else {
            if (choice==2) {
            	copyNTuples(input,output,n);
            } else {
            	copyTuplesFromSec(input,output,n,to);
            }}
	        /*** Close the files ***/       		
	       	input.close();       		       		
	       	output.close();
	        
		} catch (IOException e) { System.err.println(e); }		  
	}	
	
	/**
	 * Select position reports that have the given direction from input to output
	 * @param input
	 * @param output
	 * @param dir
	 */
	public static void selectTuples (Scanner input, BufferedWriter output, int dir) {
		
		String eventString = "";
		int count = 0; 
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				PositionReport event = PositionReport.parse(eventString);
				
				if (event.correctPositionReport() && event.dir == dir) {
					
					count++;
					output.write(eventString + "\n");            	            	            	         	
				}
			}   
		} catch (IOException e) { System.err.println(e); }	
		System.out.println("Count: " + count + " Last event: " + eventString);
	}
	
	/***
	 * Copy the given number of tuples from input to output
	 * @param input
	 * @param output
	 * @param tuple number
	 */
	public static void copyNTuples (Scanner input, BufferedWriter output, int tupleNumber) {
		
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
	 * Select position reports that have the time stamp greater or equals to second from input to output
	 * @param input
	 * @param output
	 * @param sec
	 */
	public static void copyTuplesFromSec (Scanner input, BufferedWriter output, int from, int to) {
		
		String eventString = "";
		int count = 0; 
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				PositionReport event = PositionReport.parse(eventString);
				
				if (event.correctPositionReport() && event.sec >= from && event.sec <= to) {
					
					count++;
					output.write(event.toStringChangeSec(from) + "\n");            	            	            	         	
				}
			}   
		} catch (IOException e) { System.err.println(e); }	
		System.out.println("Count: " + count + " Last event: " + eventString);
	}
}
