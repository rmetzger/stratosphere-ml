
package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.itemsimilarity.common.ItemPrefVector;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;


/**
 * 
 * UserVectorMapper reads the input file of the format <user,item,rating> and emits 
 * the following <key,value> pair, <item, <user,rating>>
 * ItemPrefVector carries <user,rating> pair 
 *
 */
public class UserVectorMapper extends MapFunction implements Serializable {
	
	private static final long serialVersionUID = 1L;
	 Record recordOut = new Record();
	 LongValue ItemId = new LongValue();
	 ItemPrefVector vector = new ItemPrefVector();
	 double prefValue = 0.0d;
	
	 @Override
	public void map(Record record, Collector<Record> collector) {
		
		String itemuservalue = record.getField(0, StringValue.class).toString();
		String[]  tokens = itemuservalue.split(",");
		
		prefValue = tokens.length > 2 ? Double.parseDouble(tokens[2])  : 1.0d;
		ItemId.setValue( Long.parseLong(tokens[1]));
		vector.setPrefVal( Long.parseLong(tokens[0]), prefValue);
		recordOut.setField(0, ItemId);
		recordOut.setField(1, vector);
		collector.collect(recordOut);

	}
}