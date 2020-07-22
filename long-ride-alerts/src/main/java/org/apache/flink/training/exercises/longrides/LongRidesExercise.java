/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;

/**
 * The "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 *
 */
public class LongRidesExercise extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

		DataStream<TaxiRide> longRides = rides
				.keyBy((TaxiRide ride) -> ride.rideId)
				.process(new MatchFunction());

		printOrTest(longRides);

		env.execute("Long Taxi Rides");
	}

	public static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

		long twoHours = 2*60*60*1000;

		private ValueState<TaxiRide> ridesStart;
		private ValueState<TaxiRide> ridesEnd;

		@Override
		public void open(Configuration config) throws Exception {
			ridesStart = getRuntimeContext().getState(new ValueStateDescriptor<>("ride_starts", TaxiRide.class));
			ridesEnd = getRuntimeContext().getState(new ValueStateDescriptor<>("ride_end", TaxiRide.class));
		}

		@Override
		public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
			TimerService timerService = context.timerService();

			TaxiRide start = ridesStart.value();
			TaxiRide end = ridesEnd.value();

			// check if the ride is a start and no ride was stored before
			if (start == null && ride.isStart && end == null) {
				ridesStart.update(ride);
				timerService.registerEventTimeTimer(ride.startTime.toEpochMilli() + twoHours);
			}

			// if ride end received we cancel the timer
			if (!ride.isStart) {
				if (start != null) {
					timerService.deleteEventTimeTimer(ride.startTime.toEpochMilli() + twoHours);
				} else {
					ridesEnd.update(ride);
				}
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {
			TaxiRide ride = ridesStart.value();
			out.collect(ride);
		}
	}
}
