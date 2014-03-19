package eu.stratosphere.itemsimilarity.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.types.Key;
import eu.stratosphere.types.Value;

public class VarLongW implements Key, Value {
	 
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long value;
	
	public VarLongW(){
		
	}
	public VarLongW(long value) {
		this.value = value;
	}
	
	public long get() {
		//System.out.println(this.value);
		return value;
	}

	public void set(long value) {
		this.value = value;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(value);

	}

	@Override
	public void read(DataInput in) throws IOException {
		set(in.readLong());

	}
	
	@Override
	public String toString(){
		return String.valueOf(value);	
	}

	@Override
	public int compareTo(Key o) {
		if(this.hashCode()==o.hashCode())
			return 1;
		else return 0;
	}

}
