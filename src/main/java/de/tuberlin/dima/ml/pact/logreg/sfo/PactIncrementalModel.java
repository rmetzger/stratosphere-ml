package de.tuberlin.dima.ml.pact.logreg.sfo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;

import de.tuberlin.dima.ml.logreg.sfo.IncrementalModel;
import eu.stratosphere.pact.common.type.Value;

/**
 * Wrapper to make an incremental model usable within PACT
 */
public class PactIncrementalModel implements Value {
  
  IncrementalModel model;
  
  public PactIncrementalModel() { }
  
  public PactIncrementalModel(IncrementalModel model) {
    this.model = model;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    List<Integer> used = model.getUsedDimensions();
    out.writeInt(used.size());
    for (int i=0; i<used.size(); ++i) {
      out.writeInt(used.get(i));
    }
    
    VectorWritable w = new VectorWritable(model.getW());
    w.write(out);
  }

  @Override
  public void read(DataInput in) throws IOException {
    int numberUsed = in.readInt();
    List<Integer> used = Lists.newArrayListWithCapacity(numberUsed);
    for (int i=0; i<numberUsed; ++i) {
      used.add(in.readInt());
    }

    Vector w = VectorWritable.readVector(in);
    
    model = new IncrementalModel(used, w);
  }
  
  public IncrementalModel getValue() {
    return model;
  }

}
