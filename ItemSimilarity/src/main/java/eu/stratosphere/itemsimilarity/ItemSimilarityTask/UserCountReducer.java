package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class UserCountReducer extends ReduceFunction implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Record pactOut= new Record();
	Record currentRecord = null;
	LongValue index; 
	IntValue intValue = new IntValue();
	Set<Long> count = new HashSet<Long>();
	
	@Override
	public void reduce(Iterator<Record> records, Collector<Record> out)
			throws Exception {
		
		
		while (records.hasNext()) {
			currentRecord = records.next();
			 index = currentRecord.getField(0,
					LongValue.class);
			count.add(index.getValue());
		}
		intValue.setValue(count.size());
		pactOut.setField(0, intValue);
		out.collect(pactOut);

	}
	

}
