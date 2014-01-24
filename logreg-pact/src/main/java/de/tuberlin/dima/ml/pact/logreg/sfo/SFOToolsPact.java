package de.tuberlin.dima.ml.pact.logreg.sfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import com.google.common.collect.Lists;

import de.tuberlin.dima.ml.logreg.sfo.FeatureGain;
import de.tuberlin.dima.ml.pact.logreg.sfo.udfs.MatchGainsAndCoefficients;
import eu.stratosphere.core.fs.FSDataInputStream;
import eu.stratosphere.core.fs.FileStatus;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;


public final class SFOToolsPact {

  /**
   * Read the gains and coefficients from the output of the previously executed job.
   * 
   * @param outputPath Path with 
   */
  public static List<FeatureGain> readGainsAndCoefficients(String outputPath) throws IOException, URISyntaxException {
    
    List<FeatureGain> gains = Lists.newArrayList();
    FileSystem fs = FileSystem.get(new URI(outputPath));
    
    if (fs.exists(new Path(outputPath))) {
      FileStatus[] statusList = fs.listStatus(new Path(outputPath));
      for (FileStatus status : statusList) {
        System.out.println("Output: " + status.getPath());
        FSDataInputStream stream = fs.open(status.getPath());
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line = null;
        while ((line = reader.readLine()) != null) {
          String[] tokens = line.split(" ");
          gains.add(new FeatureGain(
              Integer.parseInt(tokens[MatchGainsAndCoefficients.IDX_OUT_DIMENSION]),
              Double.parseDouble(tokens[MatchGainsAndCoefficients.IDX_OUT_GAIN]),
              Double.parseDouble(tokens[MatchGainsAndCoefficients.IDX_OUT_COEFFICIENT])));
        }
        reader.close();
      }
    } else {
      System.out.println("ERROR: Output folder does not exist, cannot read the gains and coefficients.");
    }
    
    return gains;
  }

}
