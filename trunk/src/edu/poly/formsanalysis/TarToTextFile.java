/*
 * Bhaktavatsalam Nallanthighal
 * April 14 2012
 * 
 * TarToTextFile.java - Utility to convert tar files to plain text files
 * with key as filename and value as file contents.
 * Based on http://stuartsierra.com/2008/04/24/a-million-little-files.
 */

package edu.poly.formsanalysis;

import org.apache.tools.bzip2.CBZip2InputStream;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.zip.GZIPInputStream;

/** 
 * Utility to convert tar files into plain text Files.  The tar
 * files may be compressed with GZip or BZip2.  Each key (a
 * Text) in the text File is the name of the file in the tar
 * archive, and its value is the contents of the
 * file.
 *
 * @author Bhaktavatsalam Nallanthighal
 */
public class TarToTextFile {

    public static void execute(File inputFile) throws Exception {
        TarInputStream input = null;
        FileWriter output = null;
        try {
            input = openInputFile(inputFile);
            output = new FileWriter((inputFile.getParent() + "/" + inputFile.getName().substring(0, inputFile.getName().lastIndexOf(".")) + "_kvp.txt"));
            TarEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                if (entry.isDirectory()) { continue; }
                
                String filename = entry.getName();
                byte[] data = TarToTextFile.getBytes(input, entry.getSize());
                
                String dataStr = new String(data);
                dataStr = dataStr.replaceAll("\n", " ").replaceAll("\r", " ");
                filename = filename.substring(filename.indexOf("/domains_100k/") + "/domains_100k/".length());
                String domain = filename.substring(0, filename.indexOf("/")).toUpperCase();
                String url = filename.substring(filename.lastIndexOf("/")+1);
                try {
                	url = URLDecoder.decode(url, "UTF-8");
                } catch(Exception e) {
                	
                }
                filename = domain + "::" + url;
                
                if(url.startsWith("http://")) {
                	output.append(filename + "\t" + dataStr + "\n");
                	//System.out.println(filename);
                } else {
                	System.err.println(filename);
                }
            }
        } finally {
            if (input != null) {
            	input.close();
            }
            if (output != null) {
            	output.close();
            }
        }
    }

    private static TarInputStream openInputFile(File inputFile) throws Exception {
        InputStream fileStream = new FileInputStream(inputFile);
        String name = inputFile.getName();
        InputStream theStream = null;
        if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) {
            theStream = new GZIPInputStream(fileStream);
        } else if (name.endsWith(".tar.bz2") || name.endsWith(".tbz2")) {
            /* Skip the "BZ" header added by bzip2. */
            fileStream.skip(2);
            theStream = new CBZip2InputStream(fileStream);
        } else {
            /* Assume uncompressed tar file. */
            theStream = fileStream;
        }
        return new TarInputStream(theStream);
    }

    private static byte[] getBytes(TarInputStream input, long size) throws Exception {
        if (size > Integer.MAX_VALUE) {
            throw new Exception("A file in the tar archive is too large.");
        }
        int length = (int)size;
        byte[] bytes = new byte[length];

        int offset = 0;
        int numRead = 0;

        while (offset < bytes.length &&
               (numRead = input.read(bytes, offset, bytes.length - offset)) >= 0) {
            offset += numRead;
        }

        if (offset < bytes.length) {
            throw new IOException("A file in the tar archive could not be completely read.");
        }

        return bytes;
    }

    public static void main(String[] args) throws Exception {
    	TarToTextFile.execute(new File(args[0]));
    }
}
