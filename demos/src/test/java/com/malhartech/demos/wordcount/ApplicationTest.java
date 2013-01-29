/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.wordcount;

import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class ApplicationTest
{
  public ApplicationTest()
  {
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    Application topology = new Application();
    final StramLocalCluster lc = new StramLocalCluster(topology.getApplication(new Configuration(false)));

//    new Thread("LocalClusterController")
//    {
//      @Override
//      public void run()
//      {
//        try {
//          while(true) {
//          Thread.sleep(1000);
//
//          }
//        }
//        catch (InterruptedException ex) {
//        }
//
//        lc.shutdown();
//      }
//    }.start();
    long start = System.currentTimeMillis();
    lc.run();
    long end = System.currentTimeMillis();
    long time = end -start;
    System.out.println("Test used "+time+" ms");
  }
}