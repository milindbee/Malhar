package com.datatorrent.lib.r;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.lib.util.BaseNumberValueOperator;
import org.rosuda.JRI.Rengine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Emits at end of window, a minimum of all values sub-classed from Number in the incoming stream. <br>
 * <br>
 * <b>StateFull :</b>Yes, min value is computed over application windows. <br>
 * <b>Partitions :</b>Yes, operator is kin unifier operator. , <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>min</b>: emits V extends Number<br>
 * <br>
 * <br>
 **/

//public class RMin<V extends Number> extends BaseNumberValueOperator implements Operator.Unifier<V>
public class RMin<V extends Number> extends BaseNumberValueOperator<Number> implements Unifier<Number>
{
    private List<Number> vector = new ArrayList<Number>();
    private Rengine rengine;

    private static Logger log = LoggerFactory.getLogger(RMin.class);

    //@InputPortFieldAnnotation(name = "data")
    //public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>()

    @InputPortFieldAnnotation(name = "data")

    public final transient DefaultInputPort<Number> data = new DefaultInputPort<Number>()
    {
        /**
         * Each tuple is compared to the min and a new min (if so) is stored.
         */
        @Override
        public void process(Number tuple)
        {
            RMin.this.process(tuple);
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

    @OutputPortFieldAnnotation(name = "min")
    public final transient DefaultOutputPort<Number> min = new DefaultOutputPort<Number>()
    {
        @Override
        public Unifier<Number> getUnifier()
        {
            return RMin.this;
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
     * Emits a minimum from the values received in teh application window.
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

        Number rMin = rengine.eval("min(vector)").asDouble();

        log.debug(String.format( "\nMin is : \"" + rMin));

        min.emit(rMin);
        vector.clear();

    }
}
