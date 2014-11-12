package com.hdptest.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KeysMapper extends   Mapper<LongWritable, Text, Text, Text> 
{       
	     
	 
	private String inputSource = "";
	private String inputFileName = "";
	private String keyFileName = "";
	
	public void setUp(Context context)
	{
		FileSplit inputSplit=(FileSplit)context.getInputSplit();
		inputSource = inputSplit.getPath().toString();
		
		System.out.println(" mapper inputsource " + inputSource);

		//inputFileName = context.getConfiguration().get("inputFileName");
		//keyFileName = context.getConfiguration().get("keyFileName");
		//if(fileName.equalsIgnoreCase(inputFileName))
	}
	
	public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException 
	{      
		
		String[] data = value.toString().split("\\,");
		
		if(inputSource.contains("masterkeys"))
			inputSource = "masterkeys";
		else 
			inputSource = "input";
		
		
		String keyStr = data[0]+"|" + data[1];
		String valueStr  = inputSource+"~"+value.toString();
		context.write(new Text(keyStr), new Text(valueStr));         
		
	} 
		
}