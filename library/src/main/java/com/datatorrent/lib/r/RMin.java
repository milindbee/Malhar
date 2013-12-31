/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Emits at the end of window, a minimum of all values sub-classed from Number in the incoming stream. <br>
 * A vector is created in the process method. This vector is passed to the 'min' function in R in the
 * 'endWindow' function.
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

public class RMin<V extends Number> extends BaseNumberValueOperator<Number> implements Unifier<Number>
{
    private List<Number> numList = new ArrayList<Number>();
    private Rengine rengine;

    private static Logger log = LoggerFactory.getLogger(RMin.class);

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
     * Adds the received tuple to the numList
     */

    @Override
    public void process(Number tuple)
    {
        numList.add(tuple);
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
            throw new RuntimeException("Cannot load R");
        }

    }

    /**
     * Invokes the 'min' function from R to emit a maximum from the values received in the application window.
     * Clears the numList at the end of the function.
     **/
    @Override
    public void endWindow() {

        if (numList.size() == 0) return;

        double[] values = new double[numList.size()];
        for (int i = 0; i < numList.size(); i++){
            values[i] = numList.get(i).doubleValue();
        }

        rengine.assign("numList", values);

        Number rMin = rengine.eval("min(numList)").asDouble();

        log.debug(String.format( "\nMin is : \"" + rMin));

        min.emit(rMin);
        numList.clear();

    }
}
