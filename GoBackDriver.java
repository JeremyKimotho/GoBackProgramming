
/**
 * A simple driver for GoBackFtp class
 * 
 * CPSC 441
 * Assignment 5
 * 
 * @author 	Majid Ghaderi
 * @version 2022
 *
 */

import java.io.*;
import java.util.*;
import java.util.logging.*;


public class GoBackDriver {
	
	private static final Logger logger = Logger.getLogger("GoBackFtp"); // global logger
	
	/**
	 * main
	 */
	public static void main(String[] args) {
		// input file name is required
		if (args.length == 0) {
			System.out.println("incorrect usage, input file name is required");
			System.out.println("try again");
			System.exit(0);
		}
			
		// parse command line args
		HashMap<String, String> params = parseCommandLine(args);

		//
		// set the parameters
		// may throw illegal argumenrt exception
		// if inputs do not have proper format/type/value
		//
		String fileName = params.getOrDefault("-i", args[0]); // name of the file to be sent to the server
		String serverName = params.getOrDefault("-s", "localhost"); // server name
		int serverPort = Integer.parseInt( params.getOrDefault("-p", "2025") ); // server port number
		int windowSize = Integer.parseInt( params.getOrDefault("-w", "10") ); // window size in segments
		int timeoutInterval = Integer.parseInt( params.getOrDefault("-t", "50") ); // duraiton of retransmission tim-out interval in milli-seconds
		Level logLevel = Level.parse( params.getOrDefault("-v", "all").toUpperCase() ); // log levels: all, info, off

		
		// set log level
		setLogLevel(logLevel);
		
		// send the file
		GoBackFtp ftp = new GoBackFtp(windowSize, timeoutInterval);
		System.out.printf("sending file \'%s\' to the server...\n", fileName);
		ftp.send(serverName, serverPort, fileName);
		System.out.println("send completed.");
	}

	
	// parse command line arguments
	private static HashMap<String, String> parseCommandLine(String[] args) {
		HashMap<String, String> params = new HashMap<String, String>();

		int i = 0;
		while ((i + 1) < args.length) {
			params.put(args[i], args[i+1]);
			i += 2;
		}
		
		return params;
	}
	
	// set the global log level and format
	private static void setLogLevel(Level level) {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s %n");
		
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(level);
		logger.addHandler(handler);		
		logger.setLevel(level);
		logger.setUseParentHandlers(false);
	}
	
}
