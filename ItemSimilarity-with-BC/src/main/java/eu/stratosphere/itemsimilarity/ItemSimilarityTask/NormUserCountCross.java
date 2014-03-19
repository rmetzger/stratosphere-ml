package eu.stratosphere.itemsimilarity.ItemSimilarityTask;

import java.io.Serializable;

import eu.stratosphere.api.java.record.functions.CrossFunction;
import eu.stratosphere.itemsimilarity.common.VectorW;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;


public class NormUserCountCross extends CrossFunction implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Record outRecord = new Record();
	@Override
	public void cross(Record record1, Record record2,
			Collector<Record> out) throws Exception {
		
		 VectorW norms =  record1.getField(0, VectorW.class);
		 IntValue noOfCols = record2.getField(0, IntValue.class);
		 
		// System.out.println("Users--"+noOfCols+"  norms--"+norms);
		 outRecord.setField(0, noOfCols);
		 outRecord.setField(1,norms);
		 out.collect(outRecord);

	}

}
