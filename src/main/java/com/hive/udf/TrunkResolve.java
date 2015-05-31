package com.hive.udf;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDF;
import com.hive.Processor.BatchProcessor;

public class TrunkResolve extends UDF{

	//HASHMAP for Reference Data
	public static HashMap<String, String> inTrunkData = BatchProcessor.inTrunkData;
	public static HashMap<String, String> outTrunkData = BatchProcessor.outTrunkData;
	private String seprator = ",";

	
	public String evaluate(Object trunkName,Object switchId,Object direction,Integer colIndex) 
	{
		

		if(trunkName!=null&&switchId!=null){
			String key = trunkName.toString() + seprator +switchId.toString();
			String value;

			int index = colIndex.intValue();

			if(direction.equals("inbound"))
			{
				value = inTrunkData.get(key);
				if(value==null)
					return null;
				if(index==0)
				{
					
					return value.split(seprator)[0];
					//return out[0];
				}
				else if (index ==1)
				{
					return value.split(seprator)[1];
				}
			}
			else if (direction.equals("outbound"))
			{
				value  = outTrunkData.get(key);
				if(value==null)
				{
					return null;					
				}
				
				if(index==0)
				{
					return value.split(seprator)[0];

				}
				else if (index ==1)
				{
					return value.split(seprator)[1];
				}
			}
		}

		return null;
	}
}
