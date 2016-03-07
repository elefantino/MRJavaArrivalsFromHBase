//Copyright (c) 2015 E. Rose, University of Tampere

//This project is a HBase variant of Arrivals class from "busarrivaldistr" project. It gets data 
//of the previous day from HBase table Busdata, and compresses this input into 
//arrivals_hb_[YYYY-MM-HH].csv file.

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {
	
	public static void main(String[] args) {
		
		String config_file = "hb.conf.server";
		HashMap<String, String> config = new HashMap<String, String>();
        config.put("hbase_table", "");		
        config.put("hdfs_output_dir", "");
        config.put("local_arrivals_dir", "");
        config.put("hdfs_arrivals_dir", "" );
        config.put("date_files", "" );
        
	    try {
            BufferedReader in = new BufferedReader(new FileReader(config_file));
            String str;
            while ((str = in.readLine()) != null) {
                if( str.startsWith(";") || str.isEmpty() )
                    continue;
                else {	   
                    int idx = str.indexOf( "=" );
                    String key = str.substring( 0, idx );
                    int idx2 = str.lastIndexOf( "=" );
                    String value = str.substring( idx2+1, str.length() );
                    config.put( key.trim(), value.trim());  
                }
            }
            in.close();
        } 
        catch (IOException e) {
            System.out.println(e);
            return;
        }
		
	    //if date is not set in config file, we process the previuos day 
	    String datefiles = config.get("date_files");
	    if (datefiles.equals("")) {
	    	Calendar cal = Calendar.getInstance();
	    	cal.setTime(new Date()); //set current time
	    	cal.add(Calendar.DATE, -1); //refer to the previous day
	    	datefiles = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
	    	config.put("date_files", datefiles);
	    }
	    
        //map reduce job: main processing is here
	    
	    //calculating arrival and departure times for each route and bus stop
	    ArrivalsHbase arrivals = new ArrivalsHbase();
	    try {
	    	arrivals.run(config);
	    	try {
	    		FileSystem hdfs = FileSystem.get(new Configuration());
	    		Path localFilePath = new Path(config.get("local_arrivals_dir") + "/arrivals_hb_" + datefiles + ".csv");
	    		//hdfs.copyToLocalFile(false, new Path(config.get("hdfs_output_dir")+"/part-r-00000"), localFilePath, true);
	    		hdfs.copyToLocalFile(false, new Path(config.get("hdfs_output_dir")+"/part-r-00000"), localFilePath);
	    		//hdfs.copyFromLocalFile(localFilePath, new Path(config.get("hdfs_arrivals_dir") + "/"));
	    		System.out.println("Output is saved to " + config.get("local_arrivals_dir"));
	    	} catch (IOException e) {
	    		System.out.println(e);
	    	}
	    } 
	    catch (Exception e) {
	    	System.out.println(e);
	    }
	   
	  	return;

	}
		
}
