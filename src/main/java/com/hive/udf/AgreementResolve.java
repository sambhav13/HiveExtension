package com.hive.udf;

import java.util.HashMap;

public class AgreementResolve extends UDF{

	public static HashMap<String, String> inAgreementData = BatchProcessor.inAgreementData;
	public static HashMap<String, String> outAgreementData = BatchProcessor.outAgreementData;
	private String seprator =",";


	public String evaluate(Object commericalId,Object direction,Integer colIndex) {
		String key = commericalId.toString();
		String value;

		int index = colIndex.intValue();

		if(direction.equals("inbound"))
		{
			value = inAgreementData.get(key);
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
			value  = outAgreementData.get(key);
			if(index==0)
			{
				return value.split(seprator)[0];

			}
			else if (index ==1)
			{
				return value.split(seprator)[1];
			}
		}

		return null;
	}
}
