//Copyright (c) 2015 E. Rose, University of Tampere

//This program is an HBase version of Arrival class from busarrival project. 
//It gets the data of the previous day from HBase table Busdata, and compresses
//the data into Arrivals_[YYYY-MM-HH].csv file.

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;


public class ArrivalsHbase {
	
	static class TableMap extends TableMapper<Text, TextArrayWritable> {
		
		protected void map(ImmutableBytesWritable rowkey, Result value, Context context) 
				throws IOException, InterruptedException {

        	HashMap<String, String> vc = new HashMap<String, String>();
			vc.put("timeAPI", "");
			vc.put("lineRef", "");
			vc.put("journeyPatternRef", "");
			vc.put("directionRef", "");
			vc.put("originShortName", "");
			vc.put("destinationShortName", "");
			vc.put("originAimedDepartureTime", "");
			vc.put("dateFrameRef", "");
			vc.put("time", "");
			vc.put("latitude", "");
			vc.put("longitude", "");
			vc.put("delay", "");
	        
			KeyValue[] keyValue= value.raw();
			for(int i=0; i < keyValue.length; i++) {
				vc.put(Bytes.toString(keyValue[i].getQualifier()),Bytes.toString(keyValue[i].getValue()));
			}
			
			//The key is a string 
			//"line,journeyPatternRef,direction,origin,destination,originDepartureTime,dateFrameRef"
			String myKey = vc.get("lineRef") + "," + vc.get("journeyPatternRef") + "," + 
					vc.get("directionRef") + "," + vc.get("originShortName") + "," +
					vc.get("destinationShortName") + "," + vc.get("originAimedDepartureTime") + "," + 
					vc.get("dateFrameRef");
			
        	if (!(vc.get("timeAPI").equals("") || (vc.get("time").equals("")))) {
        	
        		int arrivaltime = -1;
        	  	int epoch = 0;
        	  	
        	  	//The value is a string "arrivaltime,latitude,longitude,delay" where arrivaltime is
				//a number of seconds in arrival time value (equal to the difference between epoch time
				//of this day at 00:00:00 and timeAPI). We don't need to consider time zone here since
				//both time stamp values do not consider time zones
				try {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					Long time0 = sdf.parse(vc.get("time").substring(0, 10)+" 00:00:00").getTime()/1000;
					epoch = Long.valueOf(time0).intValue();
					arrivaltime = Integer.parseInt(vc.get("timeAPI")) - epoch;
				} 
				catch (ParseException e) {}
        		catch (NumberFormatException e) {}
			
				//write the result
				String[] myValue = new String[] {
						Integer.toString(arrivaltime), vc.get("latitude"), vc.get("longitude"), vc.get("delay")
					};
				context.write(new Text(myKey), new TextArrayWritable(myValue));
			}
        	
        }
    }
	
	
	public static class Reduce extends Reducer<Text,TextArrayWritable,Text,Text> {
		public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) 
				throws IOException, InterruptedException {
							
			//We get a data set with some unique key "lineRef,directionRef,originShortName,destinationShortName,
			//originDepartureTime,dateFrameRef", and a list of values "longitude,latitude,arrivaltime"
			
			//We change the key to "lineRef,directionRef,originShortName,destinationShortName,
			//originDepartureTime,dateFrameRef,stopCode,stopLatitude,stopLongitude",
			//and change values according to each bus stop.
			//A list of "stopcode"s with their longitudes and latitudes for each route will be
			//received from Journey API according to a template:
			//http://data.itsfactory.fi/journeys/api/1/journeys/[line]_[origintime]_[destination]_[origin]
			//(alternative method is to get it from gtfs files).
			
			String[] myKey = key.toString().split(",");
			String urlstr = "http://data.itsfactory.fi/journeys/api/1/journeys/" +
					myKey[0] + "_" + myKey[5] + "_" + myKey[4] + "_" + myKey[3];
			URL url = new URL(urlstr);
			Scanner scan = new Scanner(url.openStream());
		    String str = new String();
		    while (scan.hasNext())
		        str += scan.nextLine();
		    scan.close();
		    
		    JSONArray content = new JSONObject(str).getJSONArray("body");
		    //The link to journey can be empty, e.g. 
		    //http://data.itsfactory.fi/journeys/api/1/journeys/41_1310_8052_8024
		    //In this case we skip reduce task for this key	  
		    if (!content.isNull(0)) {
		    	
		    	double deg2rad = Math.PI/180;
		    	int R1 = 6370000; //earth radius, meters
		    	double R2 = 0;
		    	int max_dist = 100; //define maximum radius area of the bus stop in meters
		    	
		    	//process values for this key
	    		List<Integer> arrtimes = new ArrayList<Integer>();
	    		List<Double> lats = new ArrayList<Double>();
	    		List<Double> lons = new ArrayList<Double>();
	    		List<Double> delays = new ArrayList<Double>();
	    		for (TextArrayWritable val : values) {
					String[] strval = val.toStrings();
					int val1 = 0;
					double val2 = 0; 
					double val3 = 0;
					double val4 = 0;
					try {
						val1 = Integer.parseInt(strval[0]);
					    val2 = Double.parseDouble(strval[1])*deg2rad;
						val3 = Double.parseDouble(strval[2])*deg2rad;
						val4 = Double.parseDouble(strval[3]);
					} catch (Exception e) {}
					if (val2 != 0 & val2 != 0) {
						arrtimes.add(val1);
						lats.add(val2);
						lons.add(val3);
						delays.add(val4);
					}
				}
	    		if (lats.size() > 0) 
	    			R2 = R1*Math.cos(lats.get(0)); // R1*cos(latitude)
	    		
	    		//build a set of busstops for this key
	    		JSONArray calls = content.getJSONObject(0).getJSONArray("calls");
		    	for (int i = 0; i < calls.length(); i++) {
		    		
	    			//receive this busstop's characteristics: name, latitude, longitude
	    			JSONObject stop = calls.getJSONObject(i).getJSONObject("stopPoint");
		    		String stopname = stop.getString("shortName");
		    		double latitude = 0;
		    		double longitude = 0;
		    		String[] location = stop.getString("location").split(",");
		    		try {
		    			latitude = Double.parseDouble(location[0])*deg2rad;
		    			longitude = Double.parseDouble(location[1])*deg2rad;
		    		}
		    		catch (NumberFormatException e) { e.printStackTrace(); }
		    		
		    		int min_time = 86401; //arrival time, initially set to 60*60*24+1 sec
		    		int max_time = 0;     //departure time
		    		double delay_time = 0;
		    		for (int j = 0; j < lats.size(); j++) {
		    			double dist = max_dist + 1;
	    				int a = 0;
	    				double d = 0;
	    				try {
		    				double dlat = R1*(lats.get(j) - latitude);
		    				double dlon = R2*(lons.get(j) - longitude);
		    				dist = Math.sqrt(dlat*dlat + dlon*dlon);
		    				a = arrtimes.get(j);
		    				d = delays.get(j);
		    			} catch (IndexOutOfBoundsException e) {}
		    			if (dist < max_dist) {
		    				if (a < min_time) {
		    					min_time = a;
		    					delay_time = d; //delay at the moment of arrival to this bus stop
		    				}	
		    				if (a > max_time) 
		    					max_time = a;
		    			}	
		    		}
		    		
		    		//reduce result for each bus stop: value = busstopname, arrivaltime, departuretime, delayatarrival
	    			//Note! If there is no data in the area of this bus stop (min_time is equal to its 
		    		//initial value: 1 day + 1 sec), we don't save the result
		    		if (min_time < 86401) {
		    			String myValue = stopname + "," + Integer.toString(min_time) + "," +
		    					Integer.toString(max_time) + "," + Double.toString(delay_time);
		    			context.write(key, new Text(myValue));
		    		}
		    		
		    	}
		    }
		}
	}
	
		
	public void run(HashMap<String, String> config) throws Exception {
				
		//clean the former output if it exists
		Path p = new Path(config.get("hdfs_output_dir"));
		FileSystem fs = FileSystem.get(new Configuration());
	    if (fs.exists(p)) {
	    	fs.delete(p, true);
	    }
	    
		String datefiles = config.get("date_files");
		System.out.println("Processing data: " + datefiles);
		
		//create timestamps (considering time zone!) to limit data
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getDefault());
		Long time1 = sdf.parse(datefiles+" 00:00:00").getTime();
		Long time2 = sdf.parse(datefiles+" 23:59:59").getTime();
		
        //run a job
		Configuration conf = HBaseConfiguration.create();
		conf.set("mapreduce.output.textoutputformat.separator", ","); //set comma as a delimiter
		
		//Job job = Job.getInstance(conf, "bus arrivals from hbase");
		Job job = new Job(conf, "bus arrivals from hbase");
		job.setJarByClass(ArrivalsHbase.class);
		
		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		scan.setMaxVersions(1);
		scan.setTimeRange(time1, time2); //take a day we are interested in 
		//add the specific columns to the output to limit the amount of data
		scan.addFamily(Bytes.toBytes("info"));
		
		TableMapReduceUtil.initTableMapperJob(
				config.get("hbase_table"),	// input HBase table name
				scan,             			// Scan instance to control CF and attribute selection
				TableMap.class,   			// mapper
				Text.class,             	// mapper output key
				TextArrayWritable.class,   	// mapper output value
				job);
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(config.get("hdfs_output_dir")));
		
		job.waitForCompletion(true);
								
	}
	
}	