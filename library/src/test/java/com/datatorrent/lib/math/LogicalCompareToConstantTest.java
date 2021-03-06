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
package com.datatorrent.lib.math;

/**
 * Copyright (c) 2012-2013 DataTorrent, Inc. All rights reserved.
 */
import com.datatorrent.lib.testbench.CollectorTestSink;
import junit.framework.Assert;

import org.junit.Test;


/**
 *
 * Functional tests for
 * {@link com.datatorrent.lib.math.LogicalCompareToConstant}
 *
 */
public class LogicalCompareToConstantTest
{
	/**
	 * Test operator logic emits correct results.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
	public void testNodeProcessing()
	{
		LogicalCompareToConstant<Integer> oper = new LogicalCompareToConstant<Integer>()
		{
		};
		CollectorTestSink eSink = new CollectorTestSink();
		CollectorTestSink neSink = new CollectorTestSink();
		CollectorTestSink gtSink = new CollectorTestSink();
		CollectorTestSink gteSink = new CollectorTestSink();
		CollectorTestSink ltSink = new CollectorTestSink();
		CollectorTestSink lteSink = new CollectorTestSink();

		oper.equalTo.setSink(eSink);
		oper.notEqualTo.setSink(neSink);
		oper.greaterThan.setSink(gtSink);
		oper.greaterThanOrEqualTo.setSink(gteSink);
		oper.lessThan.setSink(ltSink);
		oper.lessThanOrEqualTo.setSink(lteSink);
		oper.setConstant(2);

		oper.beginWindow(0); //
		oper.input.process(1);
		oper.input.process(2);
		oper.input.process(3);

		oper.endWindow(); //

		Assert.assertEquals("number emitted tuples", 1,
				eSink.collectedTuples.size());
		Assert.assertEquals("tuples were", eSink.collectedTuples.get(0).equals(2),
				true);

		Assert.assertEquals("number emitted tuples", 2,
				neSink.collectedTuples.size());
		Assert.assertEquals("tuples were", neSink.collectedTuples.get(0).equals(1),
				true);
		Assert.assertEquals("tuples were", neSink.collectedTuples.get(1).equals(3),
				true);

		Assert.assertEquals("number emitted tuples", 1,
				gtSink.collectedTuples.size());
		Assert.assertEquals("tuples were", gtSink.collectedTuples.get(0).equals(1),
				true);

		Assert.assertEquals("number emitted tuples", 2,
				gteSink.collectedTuples.size());
		Assert.assertEquals("tuples were",
				gteSink.collectedTuples.get(0).equals(1), true);
		Assert.assertEquals("tuples were",
				gteSink.collectedTuples.get(1).equals(2), true);

		Assert.assertEquals("number emitted tuples", 1,
				ltSink.collectedTuples.size());
		Assert.assertEquals("tuples were", ltSink.collectedTuples.get(0).equals(3),
				true);

		Assert.assertEquals("number emitted tuples", 2,
				lteSink.collectedTuples.size());
		Assert.assertEquals("tuples were",
				lteSink.collectedTuples.get(0).equals(2), true);
		Assert.assertEquals("tuples were",
				lteSink.collectedTuples.get(1).equals(3), true);
	}
}
