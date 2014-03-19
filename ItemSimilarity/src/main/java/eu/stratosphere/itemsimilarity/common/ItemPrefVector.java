package eu.stratosphere.itemsimilarity.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.types.Key;



public class ItemPrefVector extends VarLongW{

	private static final long serialVersionUID = 1L;

   private long userId;
    

	public long getUserId() {
	return userId;
}

public void setUserId(long userId) {
	this.userId = userId;
}


	private double prefVal;


	public double getPrefVal() {
		return prefVal;
	}

	public void setPrefVal(long itemId,double prefVal) {
		set(itemId);
		this.prefVal = prefVal;
	}

	public ItemPrefVector() {
       
	}

	public ItemPrefVector(long itemId, double prefVal) {
	      super(itemId);
	       this.prefVal = prefVal;
	}
	
	/**
	 * Serializes the contents of the vector to DataOutput.
	 * <p>
	 * Use DataOutput to serialize the internal state.
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
           out.writeDouble(this.prefVal);
	}

	/**
	 * Deserializes the contents of the vector from DataInput.
	 * <p>
	 * Use DataInput to deserialize to the internal state.
	 */
	@Override
	public void read(DataInput in) throws IOException {
			super.read(in);
            this.prefVal = in.readDouble();
	}

	/**
	 * String representation of this vector.
	 */
	@Override
	public String toString() {
	return String.valueOf(this.prefVal);
	}

	
	@Override
	public int compareTo(Key o) {
		if(this.hashCode()==o.hashCode())
			return 1;
		else return 0;
	}
}
