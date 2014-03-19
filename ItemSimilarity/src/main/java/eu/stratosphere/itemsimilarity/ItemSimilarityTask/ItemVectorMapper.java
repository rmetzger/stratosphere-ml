package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;

import org.apache.mahout.math.Vector;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.itemsimilarity.common.ItemPrefVector;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class ItemVectorMapper extends MapFunction implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Record outputRecord = new Record();
	LongValue userId = new LongValue();
	VectorW partialVectorW = new VectorW();
	ItemPrefVector itemPrefV;
	@Override
	public void map(Record record, Collector<Record> out) throws Exception {

		LongValue row = record.getField(0, LongValue.class);
		Vector rowVector = record.getField(2, VectorW.class).get();
		
	    for (Vector.Element element : rowVector.nonZeroes()) {
	    	itemPrefV = new ItemPrefVector();
	    	itemPrefV.setPrefVal((int)row.getValue(), element.get());
	        userId.setValue(element.index());
	        outputRecord.setField(0, userId);
			outputRecord.setField(1, itemPrefV);
			out.collect(outputRecord);
	      }
	}

}
