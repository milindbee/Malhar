package com.datatorrent.lib.r;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseNumberValueOperator;
import org.rosuda.JRI.Rengine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.List;

/**
 * Emits at end of window, a maximum of all values sub-classed from Number in the incoming stream. <br>
 * <br>
 * <b>StateFull :</b>Yes, max value is computed over application windows. <br>
 * <b>Partitions :</b>Yes, operator is kin unifier operator. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>max</b>: emits V extends Number<br>
 * <br>
 * <br>
 **/


// public class RMax<V extends Number> extends BaseNumberValueOperator {
public class RMax<V extends Number> extends BaseNumberValueOperator<Number> implements Unifier<Number>{
    private List<Number> vector = new ArrayList<Number>();
    private Rengine rengine;

    private static Logger log = LoggerFactory.getLogger(RMax.class);

    @InputPortFieldAnnotation(name = "data")
    public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>()
    {
        /**
         * Adds the tuple to the vector
         */
        @Override
        public void process(Number tuple)
        {
            RMax.this.process(tuple);
        }
    };
    /**
     * Adds the received tuple to the vector
     */

    @Override
    public void process(Number tuple)
    {
        vector.add(tuple);
    }

    @OutputPortFieldAnnotation(name = "max")
    public final transient DefaultOutputPort<Number> max = new DefaultOutputPort<Number>()
    {
        @Override
        public Unifier<Number> getUnifier()
        {
            return RMax.this;
        }
    };

   /* Initialize the R engine
   */
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);

        // new R-engine
        rengine=new Rengine (new String [] {"--vanilla"}, false, null);
        if (!rengine.waitForR())
        {
            log.debug(String.format( "\nCannot load R"));
            return;
        }

    }

    /**
     * Emits a maximum from the values received in the application window.
     * Clears the vector at the end of the function.
     **/

    @Override
    public void endWindow() {

        if (vector.size() == 0) return;

        double[] values = new double[vector.size()];
        for (int i = 0; i < vector.size(); i++){
            values[i] = vector.get(i).doubleValue();
        }


        rengine.assign("vector", values);

        double rMax = rengine.eval("max(vector)").asDouble();

        log.debug(String.format( "\nMax is : \"" + rMax));

        max.emit(rMax);
        vector.clear();

    }
}
