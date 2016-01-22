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
	 * 						3 for select events with direction, 
	 * 						4 for copy n events, 
	 * 						5 for copy events from second to second
	 * 						6 for map from other event type
	 * 						7 for count events with a given attribute value 
	 * 						8 for special input file
	 * 	path : src/input/ or ../../input/ or ../../../Dropbox/LR/InAndOutput/10xways/
	 * 	if clean file: input file, output file, xway
	 * 	if merge files: first input file, second input file, output file, last second
	 * 	if select events: input file, output file, direction
	 * 	if copy n events: input file, output file, n
	 * 	if copy events from second to second: input file, output file, second from, second to
	 * 	if map from other event type: input file, output file, person identifier
	 * 	if count events: input file, attribute value
	 *  if special input file: input file and output file			
	 */
	public static void main (String[] args) {
		
		/*** Validate the input parameter ***/
		if (args.length < 4) {
			System.out.println("At least 4 input parameters are expected.");
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
			oneInputOneOutput(0,inputfile,outputfile,xway,0);			
		} 
		/*** Merge file ***/
		if (action == 2) {
			
			String inputfile1 = path + args[2];
			String inputfile2 = path + args[3];
			String outputfile = path + args[4];
			int lastSec = Integer.parseInt(args[5]);
			System.out.println("The files " + inputfile1 + " and " + inputfile2 + " are merged into the file" + outputfile);
			twoInputsOneOutput(inputfile1,inputfile2,outputfile,lastSec);
		}	
		/*** Select events with given direction ***/
		if (action == 3) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			int dir = Integer.parseInt(args[4]);
			System.out.println("All events with direction " + dir + " from the file " + inputfile + " are copied to the file" + outputfile);
			oneInputOneOutput(1,inputfile,outputfile,dir,0);
		}	
		/*** Copy n events ***/
		if (action == 4) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			int n = Integer.parseInt(args[4]);
			System.out.println(n + " events from the file " + inputfile + " are copied to the file" + outputfile);
			oneInputOneOutput(2,inputfile,outputfile,n,0);
		}	
		/*** Copy events from second to second ***/
		if (action == 5) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			int from = Integer.parseInt(args[4]);
			int to = Integer.parseInt(args[5]);
			System.out.println("Events from second " + from + " to second " + to + " from the file " + inputfile + " are copied to the file" + outputfile);
			oneInputOneOutput(3,inputfile,outputfile,from,to);
		}	
		/*** Map to other event type ***/
		if (action == 6) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			int id = Integer.parseInt(args[4]);
			System.out.println("Events with id " + id + " from the file " + inputfile + " are mapped to the file" + outputfile);
			oneInputOneOutput(4,inputfile,outputfile,id,0);
		}
		/*** Count events with attribute value ***/
		if (action == 7) {
			
			String inputfile = path + args[2];
			int value = Integer.parseInt(args[3]);
			System.out.println("Count the number of events with value " + value + " in the file " + inputfile);
			oneInput(inputfile,value);
		}
		/*** Create special input file ***/
		if (action == 8) {
			
			String inputfile = path + args[2];
			String outputfile = path + args[3];
			System.out.println("Create special input file " + outputfile + " from the file " + inputfile);
			oneInputOneOutput(5,inputfile,outputfile,0,0);
		}
		System.out.println("Done!");
	}
	
	/****************************************************************************
	 * Open input and output files, call respective method depending on the value of choice variable and close both files.
	 * @param choice: 	0 for select correct position reports and change xway
	 * 					1 for select events with given direction
	 * 					2 for copy first n events
	 * 					3 for copy all events from second to second
	 * 					4 for change event type
	 * @param inputfilename
	 * @param outputfilename
	 * @param n and to are input parameters to the respective method
	 */
	public static void oneInputOneOutput (int choice, String inputfilename, String outputfilename, int n, int to) {
		
		Scanner input = null;
		try {		
			/*** Input file ***/
			File input_file = new File(inputfilename);
	        input = new Scanner(input_file);     
	        
	        /*** Output file ***/
            File output_file = new File(outputfilename);
            BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
	            
	        /*** Call the method ***/ 
            if (choice == 0) {
            	changeXway(input,output,n);
            } else {
            if (choice == 1) {
            	selectEvents(input,output,n);
            } else {
            if (choice == 2) {
            	copyNEvents(input,output,n);
            } else {
            if (choice == 3) {
            	copyEventsFromSecToSec(input,output,n,to);
            } else {
            if (choice == 4) {
            	changeEventType(input,output,n);
            } else {
            	createSpecialInputFile(input,output);
            }}}}}
	        /*** Close the files ***/       		
	       	input.close();       		       		
	       	output.close();
	        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	/****************************************************************************
	 * Open 2 input files and 1 output file, call the method and close all files.
	 * @param first input file
	 * @param second input file
	 * @param output file
	 * @param last second
	 */
	public static void twoInputsOneOutput (String inputfilename1, String inputfilename2, String outputfilename, int lastSec) {
		
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
            
            /*** Call the method ***/            
            merge(input1,input2,output,lastSec);
                       
            /*** Close the files ***/
       		input1.close();
       		input2.close();
       		output.close();        		
        
		} catch (IOException e) { System.err.println(e); }		  
	}
	
	/****************************************************************************
	 * Open 1 input file, call the method and close the file.
	 * @param input file
	 * @param event attribute value
	 */
	public static void oneInput (String inputfilename, int value) {
		
		Scanner input = null;
		try {		
			/*** Input file ***/
			File input_file = new File(inputfilename);
			input = new Scanner(input_file);			
					
			/*** Call the method ***/            
            countEvents(input,value);
                       
            /*** Close the files ***/
       		input.close();
       		
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
	
	/**
	 * Select position reports that have the given direction from input to output
	 * @param input
	 * @param output
	 * @param dir
	 */
	public static void selectEvents (Scanner input, BufferedWriter output, int dir) {
		
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
	 * Copy the given number of events from input to output
	 * @param input
	 * @param output
	 * @param tuple number
	 */
	public static void copyNEvents (Scanner input, BufferedWriter output, int tupleNumber) {
		
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
	public static void copyEventsFromSecToSec (Scanner input, BufferedWriter output, int from, int to) {
		
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
	
	/***
	 * Copy events from input to output and change their type  
	 * @param input
	 * @param output
	 * @param identifier
	 */
	public static void changeEventType (Scanner input, BufferedWriter output, int id) {
		
		String eventString = "";
		int count = 0; 
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				PositionReport event = ActivityReport.parse(eventString,id);
				
				if (event.correctPositionReport()) {
					
					count++;
					output.write(event.toStringChangeXway(id) + "\n");            	            	            	         	
			}}			
		} catch (IOException e) { System.err.println(e); }
		System.out.println("Count: " + count + " Last event: " + eventString);
	}
	
	/***
	 * Count the number of events with a given attribute value in the input file   
	 * @param input
	 * @param value
	 */
	public static void countEvents (Scanner input, int value) {
		
		String eventString = "";
		int count = 0; 
		int total_count = 0;
		
		while (input.hasNextLine()) {         	
        			
			eventString = input.nextLine();
			PositionReport event = PositionReport.parse(eventString);				
			if (event.pos == value) count++;
			total_count++;
		}		
		System.out.println("Total count: " + total_count + " Count: " + count + " Percentage: " + ((count*100)/total_count + "%"));
	}
	
	/***
	 * Copy position reports from input to output and change their xway to the given value 
	 * @param input
	 * @param output
	 */
	public static void createSpecialInputFile (Scanner input, BufferedWriter output) {
		
		String eventString = "";
		int count = 0; 
		try {
			while (input.hasNextLine()) {         	
        			
				eventString = input.nextLine();
				PositionReport event = PositionReport.parse(eventString);
				
				if (event.correctPositionReport()) {
					
					count++;
					output.write(event.getJsonRepresentaion(count) + "\n");            	            	            	         	
			}}			
		} catch (IOException e) { System.err.println(e); }
		System.out.println("Count: " + count + " Last event: " + eventString);
	}	
}
