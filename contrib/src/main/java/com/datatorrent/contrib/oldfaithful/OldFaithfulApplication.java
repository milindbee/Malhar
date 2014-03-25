package com.datatorrent.contrib.oldfaithful;


import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class OldFaithfulApplication implements StreamingApplication
{
    private final DAG.Locality locality = null;

    /**
     * Create the DAG
     */
    @SuppressWarnings("unchecked")
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
        dag.setAttribute(DAG.APPLICATION_NAME, "OldFaithfulApplication");
        InputGenerator randomInputGenerator = dag.addOperator("rand", new InputGenerator());
        FaithfulRScript rScriptOp = dag.addOperator("rScriptOp", new FaithfulRScript("com/datatorrent/demos/oldfaithful/eruptionModel.R", "eruptionModel", "retVal"));
        ConsoleOutputOperator consoled = dag.addOperator("console", new ConsoleOutputOperator());
        ConsoleOutputOperator consoledA = dag.addOperator("consoledA", new ConsoleOutputOperator());
        ConsoleOutputOperator consoles = dag.addOperator("consoles", new ConsoleOutputOperator());
        ConsoleOutputOperator consolesA = dag.addOperator("consolesA", new ConsoleOutputOperator());

        rScriptOp.setFunctionName("eruptionModel");
        rScriptOp.setReturnVariable("retVal");

        Map<String, FaithfulRScript.REXP_TYPE> argTypeMap = new HashMap<String, FaithfulRScript.REXP_TYPE>();

        argTypeMap.put("ELAPSEDTIME", FaithfulRScript.REXP_TYPE.REXP_INT);
        argTypeMap.put("ERUPTIONS", FaithfulRScript.REXP_TYPE.REXP_ARRAY_DOUBLE);
        argTypeMap.put("WAITING", FaithfulRScript.REXP_TYPE.REXP_ARRAY_INT);

        rScriptOp.setArgTypeMap(argTypeMap);

        dag.addStream("ingen_faithfulRscript", randomInputGenerator.outputPort, rScriptOp.faithfulInput).setLocality(locality);
        dag.addStream("ingen_faithfulRscript_eT", randomInputGenerator.elapsedTime, rScriptOp.inputElapsedTime).setLocality(locality);
        dag.addStream("faithfulRscript_console_d",rScriptOp.doubleOutput, consoled.input).setLocality(locality);
        dag.addStream("faithfulRscript_console_dA",rScriptOp.doubleArrayOutput, consoledA.input).setLocality(locality);
        dag.addStream("faithfulRscript_console_s",rScriptOp.strOutput, consoles.input).setLocality(locality);
        dag.addStream("faithfulRscript_console_sA",rScriptOp.strArrayOutput, consolesA.input).setLocality(locality);

    }
}
