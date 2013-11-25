package com.datatorrent.lib.r;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import org.rosuda.JRI.Rengine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RStandardDeviation extends BaseOperator {

    private List<Number> values = new ArrayList<Number>();
    private Rengine rengine;

    private static Logger log = LoggerFactory.getLogger(RStandardDeviation.class);
    /**
     * Input data port.
     */
    @InputPortFieldAnnotation(name = "data")
    public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>()
    {
        /**
         * Computes sum and count with each tuple
         */
        @Override
        public void process(Number tuple)
        {
            values.add(tuple.doubleValue());

        }
    };

    /**
     * Variance output port
     */
    @OutputPortFieldAnnotation(name = "variance", optional=true)
    public final transient DefaultOutputPort<Number> variance = new DefaultOutputPort<Number>();

    /**
     * Standard deviation output port
     */
    @OutputPortFieldAnnotation(name = "standardDeviation")
    public final transient DefaultOutputPort<Number> standardDeviation = new DefaultOutputPort<Number>();

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);

        // new R-engine
        rengine=new Rengine(new String [] {"--vanilla"}, false, null);
        if (!rengine.waitForR())
        {
		    log.debug(String.format( "\nCannot load R"));
            return;
        }
    }


    @Override
    public void endWindow() {

       if (values.size() == 0) return;

        double[] vector=new double[values.size()];
        for (int i = 0; i < values.size(); i++){
            vector[i] = values.get(i).doubleValue();
        }
        rengine.assign("values", vector);

        double rStandardDeviation = rengine.eval("sd(values)").asDouble();
        double rVariance = rengine.eval("var(values)").asDouble();

        variance.emit(rVariance);
        standardDeviation.emit(rStandardDeviation);

        values.clear();

    }
}
