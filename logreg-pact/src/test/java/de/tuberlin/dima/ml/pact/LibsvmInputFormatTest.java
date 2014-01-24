package de.tuberlin.dima.ml.pact;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

import de.tuberlin.dima.ml.pact.io.LibsvmInputFormat;
import de.tuberlin.dima.ml.pact.types.PactVector;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileInputSplit;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.IntValue;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
public class LibsvmInputFormatTest {
    
    @Mock
    protected Configuration config;
    
    protected File tempFile;
    
    private final LibsvmInputFormat format = new LibsvmInputFormat();
    
    // --------------------------------------------------------------------------------------------
    
    @Before
    public void setup() {
        initMocks(this);
    }
    
    @After
    public void setdown() throws Exception {
        if (this.format != null) {
            this.format.close();
        }
        if (this.tempFile != null) {
            this.tempFile.delete();
        }
    }

    @Test
    public void testRead() throws IOException {
      String input = "+1  10:1.12345 20:-2.12345\n-1 10:1.12345 20:-2.12345";
      boolean isMultiLabel = false;
      int positiveClass = 2;
      int numFeatures = 21;
      int[] trueLabels = new int[] {1, 0};
      Vector v1 = new RandomAccessSparseVector(21);
      v1.set(10, 1.12345);
      v1.set(20, -2.12345);
      Vector v2 = new RandomAccessSparseVector(21);
      v2.set(10, 1.12345);
      v2.set(20, -2.12345);
      List<Vector> trueVectors = Lists.newArrayList(v1, v2);
      testSingleScenario(input, isMultiLabel, positiveClass, numFeatures, trueLabels, trueVectors);
      
      input = "4,5,2  10:1.12345 20:-2.12345\n1 10:1.12345 20:-2.12345";
      isMultiLabel = true;
      testSingleScenario(input, isMultiLabel, positiveClass, numFeatures, trueLabels, trueVectors);
    }
    
    private void testSingleScenario(String inputString, boolean isMultiLabel, int positiveClass, int numFeatures, int[] trueLabels, List<Vector> trueVectors) throws IOException {
    
      // Config
      final Configuration parameters = new Configuration();
      parameters.setString("pact.input.file.path", "file:///some/file/that/will/not/be/read");
      parameters.setInteger(LibsvmInputFormat.CONF_KEY_NUM_FEATURES, numFeatures);
      parameters.setBoolean(LibsvmInputFormat.CONF_KEY_MULTI_LABEL_INPUT, isMultiLabel);
      if (isMultiLabel) {
        parameters.setInteger(LibsvmInputFormat.CONF_KEY_POSITIVE_CLASS, positiveClass);
      }
      format.configure(parameters);

      // Open input split
      final FileInputSplit split = createTempFile(inputString);
      format.open(split);
      
      Record theRecord = new Record();
      for (int i=0; i<trueLabels.length; ++i) {
        format.nextRecord(theRecord);
        assertEquals(trueLabels[i], theRecord.getField(0, IntValue.class).getValue());
        assertEquals(trueVectors.get(i), theRecord.getField(1, PactVector.class).getValue());
      }
      assertFalse(format.nextRecord(theRecord));
      assertTrue(format.reachedEnd());
    }
    
    private FileInputSplit createTempFile(String contents) throws IOException {
        this.tempFile = File.createTempFile("test_contents", "tmp");
        OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(this.tempFile));
        wrt.write(contents);
        wrt.close();
        
        return new FileInputSplit(0, new Path("file://" + this.tempFile.getAbsolutePath()), 0, this.tempFile.length(), new String[] {"localhost"});
    }
}
