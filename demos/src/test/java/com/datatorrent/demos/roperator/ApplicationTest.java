package com.datatorrent.demos.roperator;


import com.datatorrent.api.LocalMode;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import javax.validation.ConstraintViolationException;


public class ApplicationTest
{
    @Test
    public void testSomeMethod() throws Exception
    {
        LocalMode lma = LocalMode.newInstance();
        Application app = new Application();
        app.populateDAG(lma.getDAG(), new Configuration(false));

        try {
            LocalMode.Controller lc = lma.getController();
            lc.setHeartbeatMonitoringEnabled(false);
            lc.run(50000);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}