package com.hdptest.reducer;

import java.util.List;
import java.util.UUID;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class KeysReducer extends  Reducer<Text, Text, Text, Text> 
{     
		
	private String inputSource = "";
	//private String inputFileName = "";
	//private String keyFileName = "";
	private MultipleOutputs multipleOutputs; 
	String keyFilePath = null;
	String outputPath = null;
	
		
	public void setup(Context context)
	{
		multipleOutputs  = new MultipleOutputs(context);
		keyFilePath = context.getConfiguration().get("keyFilePath");
		outputPath = context.getConfiguration().get("outputPath");
	}


	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{    
		keyFilePath = context.getConfiguration().get("keyFilePath");
		outputPath = context.getConfiguration().get("outputPath");
		
		String keyStr = key.toString();
		String keys[] = keyStr.split("\\|");
		String surrKey = null;
		List<String> inputList = new ArrayList<String>();
		
			       
		for (Text value : values) 
		{
			String valueString = value.toString();
			String valueStrs[] = valueString.split("\\~");
			inputSource = valueStrs[0];
			String record = valueStrs[1];
			
			System.out.println(" rcord are " + record);
			
			if(inputSource.equalsIgnoreCase("masterkeys"))//record is from keyFile
			{
				surrKey = record.split("\\,")[2];
			}
			else
				inputList.add(record);
			        
		} 
		
		String outRecord = null;
		for(String inputRecords : inputList)
		{
			String inputs[] = inputRecords.split("\\,");
			if(surrKey == null) //key not derived from masterfile, new records
			{
				
				surrKey = generateKeys(inputs[0] + inputs[1]);
				String outmasterRecord = inputs[0]+","+ inputs[1]+","+surrKey;
				System.out.println(" outmasterRecord " +outmasterRecord  );
				System.out.println(" masterkeyfiel is  " +keyFilePath  );
				keyFilePath = keyFilePath+"/" +System.currentTimeMillis();
				//multipleOutputs.write("", new Text(outmasterRecord), "/tmp/keys/masterkeys/");
				multipleOutputs.write(NullWritable.get(), new Text(outmasterRecord), keyFilePath);
			}
			//got the keys now write the output file
			outRecord = inputRecords+","+surrKey;
			multipleOutputs.write(NullWritable.get(), new Text(outRecord), outputPath);
		}
		  
	}
	
	private String generateKeys(String key2)
	{
		java.util.UUID idOne = java.util.UUID.randomUUID();
		return key2+"-"+idOne.toString();


	}
	
	 protected void cleanup(Context context) throws IOException, InterruptedException {
		 multipleOutputs.close();
	    }

	

}
