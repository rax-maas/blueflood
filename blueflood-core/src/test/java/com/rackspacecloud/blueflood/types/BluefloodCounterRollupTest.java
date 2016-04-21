package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.io.Constants;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class BluefloodCounterRollupTest {
    private static final Random random = new Random(72262L);
    
    @Test
    public void testRateCalculations() throws IOException {
        long[] sample0 = {10L,10L,10L,10L,10L,10L,10L,10L,10L,10L}; // 300 secs
        long[] sample1 = {20L,20L,20L,20L,20L,20L,20L,20L,20L,20L,20L,20L,20L,20L,20L}; // 450 secs
        final BluefloodCounterRollup cr0 = BluefloodCounterRollupTest.buildCounterRollupFromLongs(sample0);
        final BluefloodCounterRollup cr1 = BluefloodCounterRollupTest.buildCounterRollupFromLongs(sample1);
        
        assertEquals(100d / 300d, cr0.getRate(), 0.00001d);
        assertEquals(300d / 450d, cr1.getRate(), 0.00001d);
        
        // great, now combine them.
        BluefloodCounterRollup cr2 = BluefloodCounterRollup.buildRollupFromCounterRollups(asPoints(BluefloodCounterRollup.class, 0, 300, cr0, cr1));
        
        assertEquals(400d / 750d, cr2.getRate(), 0.00001d);
    }
    
    @Test
    public void testCounterRollupIdempotence() throws IOException {
        final BluefloodCounterRollup cr0 = BluefloodCounterRollupTest.buildCounterRollupFromLongs(makeRandomNumbers(1000));
        BluefloodCounterRollup cumulative = BluefloodCounterRollup.buildRollupFromCounterRollups(asPoints(BluefloodCounterRollup.class, 0, 10, cr0));
        assertEquals(cr0, cumulative);
    }
    
    @Test
    public void testCounterRollupGeneration() throws IOException {
        final long[] src0 = makeRandomNumbers(1000);
        final long[] src1 = makeRandomNumbers(500);
        final BluefloodCounterRollup cr0 = BluefloodCounterRollupTest.buildCounterRollupFromLongs(src0);
        final BluefloodCounterRollup cr1 = BluefloodCounterRollupTest.buildCounterRollupFromLongs(src1);
        
        long expectedSum = sum(src0) + sum(src1);
        
        BluefloodCounterRollup cumulative = BluefloodCounterRollup.buildRollupFromCounterRollups(asPoints(BluefloodCounterRollup.class, 0, 1000, cr0, cr1));
        
        assertEquals(expectedSum, cumulative.getCount());
    }

    @Test
    public void testNullCounterRollupVersusZero() throws IOException {
        final long[] data = new long[]{0L, 0L, 0L};
        final long[] no_data = new long[]{};
        final BluefloodCounterRollup crData = BluefloodCounterRollupTest.buildCounterRollupFromLongs(data);
        final BluefloodCounterRollup crNoData = BluefloodCounterRollupTest.buildCounterRollupFromLongs(no_data);
        assertNotSame(crData, crNoData);
    }

    private static <T> Points<T> asPoints(Class<T> type, long initialTime, long timeDelta, T... values) {
        Points<T> points = new Points<T>();
        long time = initialTime;
        for (T v : values) {
            points.add(new Points.Point<T>(time, v));
            time += timeDelta;
        }
        return points;
    }
    
    private static long[] makeRandomNumbers(int count) {
        long[] numbers = new long[count];
        for (int i = 0; i < count; i++)
            numbers[i] = random.nextLong() % 1000000L;
        return numbers;
    }
    
    private static long sum(long[] numbers) {
        long sum = 0;
        for (long num : numbers)
            sum += num;
        return sum;
    }
    
    // assume the samples are 30s apart.
    private static BluefloodCounterRollup buildCounterRollupFromLongs(long... numbers) throws IOException {
        long count = 0;
        for (long number : numbers) {
            count += number;
        }
        long sum = sum(numbers);
        double rate = (double)sum / (double)(Constants.DEFAULT_SAMPLE_INTERVAL * numbers.length);
        return new BluefloodCounterRollup().withCount(count).withRate(rate).withSampleCount(numbers.length);
    }

    @Test
    public void equalsOtherNullReturnsFalse() {
        //given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        //when
        assertFalse(rollup.equals(null));
    }

    @Test
    public void equalsOtherNotRollupReturnsFalse() {
        //given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        //when
        assertFalse(rollup.equals(""));
    }

    @Test
    public void equalsDifferentCountReturnsFalse() {
        //given
        BluefloodCounterRollup a = new BluefloodCounterRollup().withCount(1);
        BluefloodCounterRollup b = new BluefloodCounterRollup().withCount(2);
        //when
        assertFalse(a.equals(b));
    }

    @Test
    public void equalsSameCountReturnsFalse() {
        //given
        BluefloodCounterRollup a = new BluefloodCounterRollup().withCount(1);
        BluefloodCounterRollup b = new BluefloodCounterRollup().withCount(1);
        //when
        assertTrue(a.equals(b));
    }

    @Test
    public void equalsDifferentRateReturnsFalse() {
        //given
        BluefloodCounterRollup a = new BluefloodCounterRollup().withCount(1).withRate(1.0d);
        BluefloodCounterRollup b = new BluefloodCounterRollup().withCount(1).withRate(2.0d);
        //when
        assertFalse(a.equals(b));
    }

    @Test
    public void equalsSameRateReturnsFalse() {
        //given
        BluefloodCounterRollup a = new BluefloodCounterRollup().withCount(1).withRate(1.0d);
        BluefloodCounterRollup b = new BluefloodCounterRollup().withCount(1).withRate(1.0d);
        //when
        assertTrue(a.equals(b));
    }

    @Test
    public void equalsDifferentSampleCountReturnsFalse() {
        //given
        BluefloodCounterRollup a = new BluefloodCounterRollup().withCount(1).withSampleCount(1);
        BluefloodCounterRollup b = new BluefloodCounterRollup().withCount(1).withSampleCount(2);
        //when
        assertFalse(a.equals(b));
    }

    @Test
    public void equalsSameSampleCountReturnsFalse() {
        //given
        BluefloodCounterRollup b = new BluefloodCounterRollup().withCount(1).withSampleCount(1);
        BluefloodCounterRollup a = new BluefloodCounterRollup().withCount(1).withSampleCount(1);
        //when
        assertTrue(a.equals(b));
    }

    @Test
    public void constructorSetsInitialValues() {
        // when
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        // then
        assertNull(rollup.getCount());
        assertEquals(0.0d, rollup.getRate(), 0.000001d);
        assertEquals(0, rollup.getSampleCount());
    }

    @Test
    public void withCountIntegerSetsCount() {
        // given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        // precondition
        assertNull(rollup.getCount());
        // when
        rollup.withCount(123);
        // then
        Number value = rollup.getCount();
        assertThat(value, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(123L, value.longValue());
    }

    @Test
    public void withCountLongSetsCount() {
        // given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        // precondition
        assertNull(rollup.getCount());
        // when
        rollup.withCount(123L);
        // then
        Number value = rollup.getCount();
        assertThat(value, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(123L, value.longValue());
    }

    @Test
    public void withCountFloatSetsCount() {
        // given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        // precondition
        assertNull(rollup.getCount());
        // when
        rollup.withCount(123.45f);
        // then
        Number value = rollup.getCount();
        assertThat(value, is(CoreMatchers.<Number>instanceOf(Double.class)));
        assertEquals(123.45d, value.doubleValue(), 0.00001d);
    }

    @Test
    public void withCountDoubleSetsCount() {
        // given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        // precondition
        assertNull(rollup.getCount());
        // when
        rollup.withCount(123.45d);
        // then
        Number value = rollup.getCount();
        assertThat(value, is(CoreMatchers.<Number>instanceOf(Double.class)));
        assertEquals(123.45d, value.doubleValue(), 0.00001d);
    }

    @Test
    public void withRateSetsRate() {
        // given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        // precondition
        assertEquals(0.0d, rollup.getRate(), 0.00001d);
        // when
        rollup.withRate(123.45d);
        // then
        assertEquals(123.45d, rollup.getRate(), 0.00001d);
    }

    @Test
    public void withSampleCountSetsSampleCount() {
        // given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        // precondition
        assertEquals(0, rollup.getSampleCount());
        // when
        rollup.withSampleCount(123);
        // then
        assertEquals(123, rollup.getSampleCount());
    }

    @Test
    public void rawSampleBuilderWithEmptyInputYieldsAllZeroes() throws IOException {
        // given
        Points<SimpleNumber> input = new Points<SimpleNumber>();
        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromRawSamples(input);
        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(0L, count.longValue());
        assertEquals(0.0d, rollup.getRate(), 0.00001d);
        assertEquals(0, rollup.getSampleCount());
    }

    @Test
    public void rawSampleBuilderWithSingleInputYieldsThatInputWithInfiniteRate() throws IOException {
        // given
        Points<SimpleNumber> input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(3L)));
        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromRawSamples(input);
        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(3L, count.longValue());
        assertEquals(Double.POSITIVE_INFINITY, rollup.getRate(), 0.00001d); //(3)/0=inf
        assertEquals(1, rollup.getSampleCount());
    }

    @Test
    public void rawSampleBuilderWithTwoInputsYieldsRateEqualToSumOfInputs() throws IOException {
        // given
        Points<SimpleNumber> input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(3L)));
        input.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(4L)));
        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromRawSamples(input);
        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(7L, count.longValue());
        assertEquals(7.0d, rollup.getRate(), 0.00001d); // (3+4)/1=7
        assertEquals(2, rollup.getSampleCount());
    }

    @Test
    public void rawSampleBuilderWithThreeInputsYieldsRateEqualToOffByOneAverage() throws IOException {

        // TODO: Clarify what buildRollupFromRawSamples is doing exactly. The
        // buildRollupFromRawSamples seems to have an off-by-one error in it.
        // The below samples average out to (3+4+5)/2=6, rather than a true
        // average rate of values, (3+4+5)/3=4. The discrepancy seems to be
        // caused by an unclear understanding of how much of a given time unit
        // a sample takes up. If the sample is interpreted as residing at an
        // infinitesimal point at the beginning of the time unit, that would
        // give rise to the current calculation. If the sample is instead
        // interpreted as residing on an interval spanning the entire time
        // unit, that would give rise to the second. It may be that this is the
        // intended behavior, or it might not be; neither the code nor the
        // documentation anywhere adequately explain the intended
        // interpretation. Hence the need for clarification. At present, this
        // test only documents the current behavior, and does not try to
        // determine what SHOULD be done.
        //
        //
        // Samples As Points :
        //                                                          o 5
        //                                  o 4                     |
        //          o 3                     |                       |
        //          |                       |                       |
        //          |                       |                       |
        //          | <-- one time unit --> |                       |
        //    ------+-----------------------+-----------------------+-----
        //          1                       1                       1
        //          2                       2                       2
        //          3                       3                       3
        //          4                       5                       6
        //          L                       L                       L
        //          |                                               |
        //          | <-------------- two time units -------------> |
        //                             (3+4+5)/2=6
        //
        //
        // Samples As Intervals:
        //                                                           _______________________
        //                                   _______________________|          5            |
        //           _______________________|          4            |                       |
        //          |          3            |                       |                       |
        //          |                       |                       |                       |
        //          |                       |                       |                       |
        //    ------+-----------------------+-----------------------+-----------------------+-----
        //          1                       1                       1                       1
        //          2                       2                       2                       2
        //          3                       3                       3                       3
        //          4                       5                       6                       7
        //          L                       L                       L                       L
        //          |                                                                       |
        //          | <------------------------- three time units ------------------------> |
        //                                         (3+4+5)/3=4
        //

        // given
        Points<SimpleNumber> input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(3L)));
        input.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(4L)));
        input.add(new Points.Point<SimpleNumber>(1236L, new SimpleNumber(5L)));
        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromRawSamples(input);
        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(12L, count.longValue());
        assertEquals(6.0d, rollup.getRate(), 0.00001d); // (3+4+5)/2=6
        assertEquals(3, rollup.getSampleCount());
    }

    @Test
    public void rawSampleBuilderWithFourInputsYieldsRateEqualToOffByOneAverage() throws IOException {

        // given
        Points<SimpleNumber> input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(3L)));
        input.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(4L)));
        input.add(new Points.Point<SimpleNumber>(1236L, new SimpleNumber(5L)));
        input.add(new Points.Point<SimpleNumber>(1237L, new SimpleNumber(6L)));
        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromRawSamples(input);
        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(18L, count.longValue());
        assertEquals(6.0d, rollup.getRate(), 0.00001d); // (3+4+5+6)/3=6
        assertEquals(4, rollup.getSampleCount());
    }

    @Test(expected = NullPointerException.class)
    public void rawSampleBuilderWithNullInputsThrowsException() throws IOException {

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromRawSamples(null);
        // then
        // the exception is thrown
    }

    @Test
    public void rawSampleBuilderWithOverflowingLongInputsYieldsIncorrectCalculations() throws IOException {

        // given
        Points<SimpleNumber> input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(Long.MAX_VALUE)));
        input.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(3L)));
        long expectedCount = -9223372036854775806L; // Long.MIN_VALUE + 2;
        double expectedRate = -9.223372036854776E18d; // (double)(Long.MIN_VALUE + 2)
        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromRawSamples(input);
        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(expectedCount, count.longValue());
        assertEquals(expectedRate, rollup.getRate(), 0.00001d);
        assertEquals(2, rollup.getSampleCount());
    }

    @Test
    public void rawSampleBuilderWithOverflowingDoubleInputsYieldsIncorrectCalculations() throws IOException {

        // given
        Points<SimpleNumber> input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(Double.MAX_VALUE)));
        input.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(Double.MAX_VALUE)));
        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromRawSamples(input);
        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Double.class)));
        assertEquals(Double.POSITIVE_INFINITY, count.doubleValue(), 0.00001d);
        assertEquals(Double.POSITIVE_INFINITY, rollup.getRate(), 0.00001d);
        assertEquals(2, rollup.getSampleCount());
    }

    @Test
    public void rawSampleBuilderWithUnderflowingDoubleInputsYieldsIncorrectCalculations() throws IOException {

        // given
        Points<SimpleNumber> input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(Double.MIN_NORMAL)));
        input.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(Double.MIN_NORMAL)));
        double expectedCount = Double.MIN_NORMAL * 2; // Long.MIN_VALUE + 2;
        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromRawSamples(input);
        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Double.class)));
        assertEquals(expectedCount, count.doubleValue(), 0.00001d);
        assertEquals(0, rollup.getRate(), 0.00001d);
        assertEquals(2, rollup.getSampleCount());
    }

    @Test
    public void rawSampleBuilderWithNanInputsYieldsNan() throws IOException {

        // given
        Points<SimpleNumber> input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(1.0d)));
        input.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(2.0d)));
        input.add(new Points.Point<SimpleNumber>(1236L, new SimpleNumber(Double.NaN)));
        input.add(new Points.Point<SimpleNumber>(1237L, new SimpleNumber(4.0d)));
        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromRawSamples(input);
        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Double.class)));
        assertTrue(Double.isNaN(count.doubleValue()));
        assertTrue(Double.isNaN(rollup.getRate()));
        assertEquals(4, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithSingleSingleInputYieldsZeroRate() throws IOException {

        // given
        Points<SimpleNumber> inputA = new Points<SimpleNumber>();
        inputA.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(1L)));
        BluefloodCounterRollup rollupA = BluefloodCounterRollup.buildRollupFromRawSamples(inputA);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(1L, count.longValue());
        assertEquals(0.0d, rollup.getRate(), 0.00001d); // (1)/0=0
        assertEquals(1, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithTwoSingleInputYieldsZeroRate() throws IOException {

        // given
        Points<SimpleNumber> inputA = new Points<SimpleNumber>();
        inputA.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(1L)));
        BluefloodCounterRollup rollupA = BluefloodCounterRollup.buildRollupFromRawSamples(inputA);

        Points<SimpleNumber> inputB = new Points<SimpleNumber>();
        inputB.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(2L)));
        BluefloodCounterRollup rollupB = BluefloodCounterRollup.buildRollupFromRawSamples(inputB);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));
        combined.add(new Points.Point<BluefloodCounterRollup>(1235L, rollupB));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(3L, count.longValue()); // 1+2
        assertEquals(0.0d, rollup.getRate(), 0.00001d); // (1+2+3+4)/2=5
        assertEquals(2, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithSingleTwoInputYieldsRateEqualToOffByOneAverage() throws IOException {

        // given
        Points<SimpleNumber> inputA = new Points<SimpleNumber>();
        inputA.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(1L)));
        inputA.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(2L)));
        BluefloodCounterRollup rollupA = BluefloodCounterRollup.buildRollupFromRawSamples(inputA);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(3L, count.longValue()); // 1+2
        assertEquals(3.0d, rollup.getRate(), 0.00001d); // (1+2+3+4)/2=5
        assertEquals(2, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithTwoTwoInputYieldsRateEqualToOffByTwoAverage() throws IOException {

        // given
        Points<SimpleNumber> inputA = new Points<SimpleNumber>();
        inputA.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(1L)));
        inputA.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(2L)));
        BluefloodCounterRollup rollupA = BluefloodCounterRollup.buildRollupFromRawSamples(inputA);

        Points<SimpleNumber> inputB = new Points<SimpleNumber>();
        inputB.add(new Points.Point<SimpleNumber>(1236L, new SimpleNumber(3L)));
        inputB.add(new Points.Point<SimpleNumber>(1237L, new SimpleNumber(4L)));
        BluefloodCounterRollup rollupB = BluefloodCounterRollup.buildRollupFromRawSamples(inputB);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));
        combined.add(new Points.Point<BluefloodCounterRollup>(1236L, rollupB));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(10L, count.longValue()); // 1+2+3+4
        assertEquals(5.0d, rollup.getRate(), 0.00001d); // (1+2+3+4)/(1+1)=5
        assertEquals(4, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithMixedSizeInputYieldsRateEqualToOffByTwoAverage() throws IOException {

        // given
        Points<SimpleNumber> inputA = new Points<SimpleNumber>();
        inputA.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(1L)));
        inputA.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(2L)));
        BluefloodCounterRollup rollupA = BluefloodCounterRollup.buildRollupFromRawSamples(inputA);

        Points<SimpleNumber> inputB = new Points<SimpleNumber>();
        inputB.add(new Points.Point<SimpleNumber>(1236L, new SimpleNumber(3L)));
        BluefloodCounterRollup rollupB = BluefloodCounterRollup.buildRollupFromRawSamples(inputB);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));
        combined.add(new Points.Point<BluefloodCounterRollup>(1236L, rollupB));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(6L, count.longValue()); // 1+2+3
        assertEquals(6.0d, rollup.getRate(), 0.00001d); // (1+2+3)/(1+0)=6
        assertEquals(3, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithSingleThreeInputYieldsRateEqualToOffByOneAverage() throws IOException {

        // given
        Points<SimpleNumber> inputA = new Points<SimpleNumber>();
        inputA.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(1L)));
        inputA.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(2L)));
        inputA.add(new Points.Point<SimpleNumber>(1236L, new SimpleNumber(3L)));
        BluefloodCounterRollup rollupA = BluefloodCounterRollup.buildRollupFromRawSamples(inputA);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(6L, count.longValue()); // 1+2+3
        assertEquals(3.0d, rollup.getRate(), 0.00001d); // (1+2+3)/(2)=3
        assertEquals(3, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithNoInputYieldsRateEqualToOffByOneAverage() throws IOException {

        // given
        Points<SimpleNumber> inputA = new Points<SimpleNumber>();
        BluefloodCounterRollup rollupA = BluefloodCounterRollup.buildRollupFromRawSamples(inputA);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(0L, count.longValue());
        assertEquals(0.0d, rollup.getRate(), 0.00001d); // (0)/(inf)=0
        assertEquals(0, rollup.getSampleCount());
    }

    @Test(expected = NullPointerException.class)
    public void counterRollupBuiderWithNullRollupInputThrowsException() throws IOException {

        // given
        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, null));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        // the exception is thrown
    }

    @Test(expected = NullPointerException.class)
    public void counterRollupBuiderWithNullCombinedInputThrowsException() throws IOException {

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(null);

        // then
        // the exception is thrown
    }
}
