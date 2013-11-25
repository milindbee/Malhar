package com.datatorrent.lib.r;

import com.datatorrent.lib.statistics.StandardDeviation;
import com.datatorrent.lib.testbench.CollectorTestSink;
import junit.framework.Assert;
import org.junit.Test;

public class RStandardDeviationOperatorTest {
    @Test
    public void testWeightedMean()
    {
        RStandardDeviation oper = new RStandardDeviation();
        CollectorTestSink<Object> variance = new CollectorTestSink<Object>();
        oper.variance.setSink(variance);
        CollectorTestSink<Object> deviation = new CollectorTestSink<Object>();
        oper.standardDeviation.setSink(deviation);

        oper.setup(null);
        oper.beginWindow(0);
        oper.data.process(1.0);
        oper.data.process(7.0);
        oper.data.process(3.0);
        oper.data.process(9.0);
        oper.endWindow();

        Assert.assertEquals("Must be one tuple in sink", variance.collectedTuples.size(), 1);
        Assert.assertEquals("Must be one tuple in sink", deviation.collectedTuples.size(), 1);
        System.out.println(variance.collectedTuples.get(0));
        System.out.println(deviation.collectedTuples.get(0));
    }
}
