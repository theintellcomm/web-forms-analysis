package edu.poly.formsanalysis;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;

/**
 * @author bhaktavatsalam
 *
 */
public class WebFormsAnalysisMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length!=1) {
			System.err.println("Path to URLs file not provided.");
		}
		
		File fileWithURLs = new File(args[0]);
		if(!fileWithURLs.exists()) {
			System.err.println(args[0] + " does not exist.");
		} else if(!fileWithURLs.isFile()) {
			System.err.println(args[0] + " is not a file.");
		} else if(!fileWithURLs.canRead()) {
			System.err.println(args[0] + " cannot be read.");
		}

		FileInputStream is = null;
		try {
			is = new FileInputStream(fileWithURLs);
		} catch(FileNotFoundException e) {
			System.err.println(args[0] + " does not exist.");
		}
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		
		while(true) {
			String str = null;
			try {
				str = br.readLine();
			} catch(IOException e) {
				System.err.println("Could not read from BufferedReader.");
			}
			
			if(str==null) {
				break;
			}
			
			// Since str is url encoded string, let's decode it.
			try {
				str = URLDecoder.decode(str, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				System.err.println("Unsupported encoding when decoding: " + str);
			} catch (IllegalArgumentException e) {
				System.err.println("Unsupported encoding when decoding: " + str);
			}
			
			URL url = null;
			try {
				url = new URL(str);
			} catch(MalformedURLException e) {
				System.err.println(str + " not a valid URL.");
			}
			
			URLConnection con = null;
			if(url!=null) {
				try {
					con = url.openConnection(); 
				} catch(IOException e) {
					System.err.println(url.getFile() + " could not be reached");
				}
			}
			
			if(con!=null) {
				try {
					BufferedReader rd = new BufferedReader(new InputStreamReader(con.getInputStream()));
					StringBuffer sb = new StringBuffer();
					String line;
					while ((line = rd.readLine()) != null)
					{
						sb.append(line);
					}
					rd.close();
					System.out.println(sb);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
			
			
			
			
		}
		
		if(is!=null) {
			try {
				is.close();
			} catch(IOException e) {
				System.err.println("Could not close input stream");
			}
		}
		if(isr!=null) {
			try {
				isr.close();
			} catch(IOException e) {
				System.err.println("Could not close input stream reader");
			}
		}
		if(br!=null) {
			try {
				br.close();
			} catch(IOException e) {
				System.err.println("Could not close buffer reader");
			}
		}
	}

}
