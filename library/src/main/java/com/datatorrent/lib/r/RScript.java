package com.datatorrent.lib.r;


import com.datatorrent.api.*;
import com.datatorrent.lib.script.ScriptOperator;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Map;

public class RScript extends ScriptOperator {

    private Rengine rengine;
    private static Logger log = LoggerFactory.getLogger(RScript.class);

        @Override
    public Map<String, Object> getBindings() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Execute R code with variable value map.
     */
    @Override
    public void process(Map<String, Object> tuple) {
        Map map = tuple;

        REXP result = rengine.eval("source(\"" + super.script + "\")");
        REXP retVal = rengine.eval("data");


        switch (retVal.rtype) {

            case REXP.INTSXP:
                int iData = retVal.asInt();
                log.debug(String.format(" <Int> R: " + iData));
                break;


            case REXP.REALSXP:
                double dData = retVal.asDouble();
                log.debug(String.format(" <Real> R: " + dData));
                break;

            case REXP.STRSXP:
                String sData = retVal.asString();
                log.debug(String.format(" <String> R: " + sData));
                break;

            default:
                log.debug(String.format("Error : Type mismatch of returned value"));
                break;
        }

        log.debug(String.format(" R: " + result));
        return;

    }


    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);

        // new R-engine
        rengine=new Rengine(new String [] {"--vanilla"}, false, null);
        if (!rengine.waitForR())
        {
            log.debug(String.format( "\nCannot load R"));
            return;
        };
    }

};


