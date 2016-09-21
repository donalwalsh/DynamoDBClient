package com.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;

public final class DynamoDBTestClient {

	public static void main(String[] args) {
		System.out.println("start");
		
		try	{
			// This client will default to US West (Oregon)
			AmazonDynamoDBClient client = new AmazonDynamoDBClient();
			client.withRegion(Regions.US_WEST_2);
			//printTableNames(client);
			//getTableAttDataNumber(client, "ProductCatalog","Id", "204", "BicycleType");
			//getTableAttDataStr(client, "Forum","Name", "Amazon S3", "Category");
			getTableAttDataSort(client);
			
			/*DynamoDB dynamoDB = new DynamoDB(client);
			Table table = dynamoDB.getTable("Thread");
			System.out.println("got table "+table.toString());
			Item item = table.getItem("ForumName", "Amazon S3");*/
			
			//GetItemSpec spec = new GetItemSpec().withPrimaryKey("ForumName", "Amazon S3");
			//System.out.println("got item spec");
			//GetItemOutcome gio = table.getItemOutcome(spec);
			//System.out.println("got item outcome");
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("end");
	}
	/**
	 * Print table data
	 * @param client
	 */
	private static void getTableAttDataSort(AmazonDynamoDBClient client) {
		System.out.println("getTableData.start\n");
		try	{
			//set attribute value
			Map<String,AttributeValue> attrValObj = new HashMap<String,AttributeValue>();
			AttributeValue av = createAttributeValueFromStr("AmazonS3");
			attrValObj.put("ForumName", av);
			AttributeValue av1 = createAttributeValueFromStr("S3 Thread 2");
			attrValObj.put("Subject", av1);
			
			// set GetItemRequest
			GetItemRequest getItemRequest = new GetItemRequest();
			getItemRequest.setTableName("Thread");
			getItemRequest.setKey(attrValObj);
			
			GetItemResult itemRes = client.getItem(getItemRequest);
			System.out.println(itemRes.toString());
			Map<String, AttributeValue> dbRes = itemRes.getItem();
			if(dbRes != null) {
				System.out.println("Attribute name is "+dbRes.get("Message"));
			}
		}
		catch(Exception ex) {
				ex.printStackTrace();
		}
		System.out.println("getTableData.end\n");
	}
	/**
	 * Print table names
	 * @param client
	 */
	private static void printTableNames(AmazonDynamoDBClient client) {
		System.out.println("printTableNames.start\n");
		try	{
			ListTablesResult listTab = client.listTables();
			List<String> strListTab = listTab.getTableNames();
			System.out.println("list size is "+strListTab.size());
			Iterator<String> it = strListTab.iterator();
			while(it.hasNext()) {
			System.out.println((String)it.next());
			}
		}
		catch(Exception ex) {
				ex.printStackTrace();
		}
		System.out.println("printTableNames.end\n");
	}
	/**
	 * Print table data
	 * @param client
	 */
	private static void getTableAttDataNumber(AmazonDynamoDBClient client, String tableName, String attr, String attrVal, String attResp) {
		System.out.println("getTableData.start\n");
		try	{
			//set attribute value
			Map<String,AttributeValue> attrValObj = new HashMap<String,AttributeValue>();
			AttributeValue av = createAttributeValueFromInt(attrVal);
			attrValObj.put(attr, av);
			
			// set GetItemRequest
			GetItemRequest getItemRequest = new GetItemRequest();
			getItemRequest.setTableName(tableName);
			getItemRequest.setKey(attrValObj);
			
			GetItemResult itemRes = client.getItem(getItemRequest);
			System.out.println(itemRes.toString());
			Map<String, AttributeValue> dbRes = itemRes.getItem();
			if(dbRes != null) {
				System.out.println("Attribute name is "+dbRes.get(attResp));
			}
		}
		catch(Exception ex) {
				ex.printStackTrace();
		}
		System.out.println("getTableData.end\n");
	}
	
	/**
	 * Print table data
	 * @param client
	 */
	private static void getTableAttDataStr(AmazonDynamoDBClient client, String tableName, String attr, String attrVal, String attResp) {
		System.out.println("getTableAttDataStr.start\n");
		try	{
			//set attribute value
			Map<String,AttributeValue> attrValObj = new HashMap<String,AttributeValue>();
			AttributeValue av1 = createAttributeValueFromStr(attrVal);
			attrValObj.put(attr, av1);
			
			// set GetItemRequest
			GetItemRequest getItemRequest = new GetItemRequest();
			getItemRequest.setTableName(tableName);
			getItemRequest.setKey(attrValObj);
			
			GetItemResult itemRes = client.getItem(getItemRequest);
			System.out.println(itemRes.toString());
			Map<String, AttributeValue> dbRes = itemRes.getItem();
			if(dbRes != null) {
				System.out.println("Attribute name is "+dbRes.get(attResp));
			}
		}
		catch(Exception ex) {
				ex.printStackTrace();
		}
		System.out.println("getTableAttDataStr.end\n");
	}
	
	private static AttributeValue createAttributeValueFromStr(String attrVal) {

		AttributeValue av = new AttributeValue();
		av.setS(attrVal);
		return av;
	}
	private static AttributeValue createAttributeValueFromInt(String attrVal) {
		
		AttributeValue av = new AttributeValue();
		av.setN(attrVal);
		return av;
	}
	
}

