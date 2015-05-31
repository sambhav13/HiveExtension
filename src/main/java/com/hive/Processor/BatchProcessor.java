package com.hive.Processor;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;


public class BatchProcessor {



	//HASHMAP for Reference Data
	public static HashMap<String, String> inTrunkData = new HashMap<>(); 
	public static HashMap<String, String> outTrunkData = new HashMap<>(); 
	public static HashMap<String, String> inAgreementData = new HashMap<>();
	public static HashMap<String, String> outAgreementData = new HashMap<>();
	//public static HashMap<Integer, ArrayList<Object>> SwitchData = new HashMap<>();




	public static void main(String[] args)
	{
		BatchProcessor bp = new BatchProcessor();
		bp.start(args[0]);
	}
	public  void start(String timestamp){


		long batchRefData_Start = System.currentTimeMillis();

		getReferenceData(timestamp);
		long batchRefData_End = System.currentTimeMillis();
		long batchrefDataTime = (batchRefData_End - batchRefData_Start)/1000;

	}

	public void getReferenceData(String timestamp)
	{

		System.out.println("-------- MySQL JDBC Connection Testing ------------");
		String query = "select CallDate,InTrunkBERID,InAgreementBERID,InTrunkData,OutTrunkData " +
				"from CDRProcess where callDate = "+timestamp;

		Statement stmt = null;
		ResultSet rs;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("MySQL JDBC Driver Registered!");
			Connection connection = null;
			connection = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/mkyongcom","root", "password");
			if (connection != null) {
				System.out.println("You made it, take control your database now!");
			} else {
				System.out.println("Failed to make connection!");
			}
			stmt = (Statement) connection.createStatement();
			rs = stmt.executeQuery(query);

			int column1Pos = rs.findColumn("CallDate");
			int column2Pos = rs.findColumn("InTrunkData");

			int column3Pos = rs.findColumn("CallDate");
			int column4Pos = rs.findColumn("OutTrunkData");


			int column5Pos = rs.findColumn("CallDate");
			int column6Pos = rs.findColumn("InAgreementData");

			// parsing the column each time is a linear search
			int column7Pos = rs.findColumn("CallDate");
			int column8Pos = rs.findColumn("OutAgreementData");
			
			while (rs.next()) {
				String column1 = rs.getString(column1Pos);
				String column2 = rs.getString(column2Pos);
				
				String column3 = rs.getString(column3Pos);
				String column4 = rs.getString(column4Pos);
				
				String column5 = rs.getString(column5Pos);
				String column6 = rs.getString(column6Pos);
				
				String column7 = rs.getString(column7Pos);
				String column8 = rs.getString(column8Pos);
				
				inTrunkData.put(column1, column2);
				outTrunkData.put(column3, column4);
				inAgreementData.put(column5, column6);
				outAgreementData.put(column7, column8);
			}
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your MySQL JDBC Driver?");
			e.printStackTrace();
			return;
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}


		


	
		
	}





}