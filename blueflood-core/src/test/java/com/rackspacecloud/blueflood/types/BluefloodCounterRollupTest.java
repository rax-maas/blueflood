package com.rackspacecloud.blueflood.types;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class BluefloodCounterRollupTest {

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
    public void equalsSameCountReturnsTrue() {
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
    public void equalsSameRateReturnsTrue() {
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
    public void equalsSameSampleCountReturnsTrue() {
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
        assertTrue(Double.isInfinite(rollup.getRate())); //(3)/(1-1)=+inf
        assertTrue(rollup.getRate() > 0);
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
        assertEquals(7.0d, rollup.getRate(), 0.00001d); // (3+4)/(2-1)=7
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
        assertEquals(6.0d, rollup.getRate(), 0.00001d); // (3+4+5)/(3-1)=6
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
        assertEquals(6.0d, rollup.getRate(), 0.00001d); // (3+4+5+6)/(4-1)=6
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
        assertTrue(((Double)count).isInfinite());
        assertTrue(((Double)count) > 0);
        assertTrue(Double.isInfinite(rollup.getRate()));
        assertTrue(rollup.getRate() > 0);
        assertEquals(2, rollup.getSampleCount());
    }

    @Test
    public void rawSampleBuilderWithUnderflowingDoubleInputsYieldsIncorrectCalculations() throws IOException {

        // given
        Points<SimpleNumber> input = new Points<SimpleNumber>();
        input.add(new Points.Point<SimpleNumber>(1234L, new SimpleNumber(Double.MIN_NORMAL)));
        input.add(new Points.Point<SimpleNumber>(1235L, new SimpleNumber(Double.MIN_NORMAL)));
        double expectedCount = Double.MIN_NORMAL * 2;
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
        assertEquals(3.0d, rollup.getRate(), 0.00001d); // (1+2+3+4)/(3-1)=5
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
        assertEquals(5.0d, rollup.getRate(), 0.00001d); // (1+2+3+4)/((2-1)+(2-1))=5
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
        assertEquals(6.0d, rollup.getRate(), 0.00001d); // (1+2+3)/((2-1)+(1-1))=6
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
        assertEquals(3.0d, rollup.getRate(), 0.00001d); // (1+2+3)/(3-1)=3
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

    @Test
    public void counterRollupBuiderWithsSingleExplicitInputYieldsSameValues() throws IOException {

        // given
        BluefloodCounterRollup rollupA = new BluefloodCounterRollup()
                .withCount(6)
                .withRate(3)
                .withSampleCount(3);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(6L, count.longValue()); // 1+2+3
        assertEquals(3.0d, rollup.getRate(), 0.00001d); // (1+2+3)/(3-1)=3
        assertEquals(3, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithsSingleExplicitZeroCountInputYieldsZeroRate() throws IOException {

        // given
        BluefloodCounterRollup rollupA = new BluefloodCounterRollup()
                .withCount(0)
                .withRate(3)
                .withSampleCount(3);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(0, count.longValue());
        assertEquals(0.0d, rollup.getRate(), 0.00001d); // (0)/(3-1)=0
        assertEquals(3, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithsSingleExplicitZeroRateInputYieldsSameValues() throws IOException {

        // given
        BluefloodCounterRollup rollupA = new BluefloodCounterRollup()
                .withCount(6)
                .withRate(0)
                .withSampleCount(3);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(6L, count.longValue()); // 1+2+3
        assertEquals(0.0d, rollup.getRate(), 0.00001d); // (1+2+3)/(3-1)=3
        assertEquals(3, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithsSingleExplicitZeroSampleCountInputYieldsSameValues() throws IOException {

        // given
        BluefloodCounterRollup rollupA = new BluefloodCounterRollup()
                .withCount(6)
                .withRate(3)
                .withSampleCount(0);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(6L, count.longValue()); // 1+2+3
        assertEquals(3.0d, rollup.getRate(), 0.00001d); // (1+2+3)/(3-1)=3
        assertEquals(0, rollup.getSampleCount());
    }

    @Test
    public void counterRollupBuiderWithsTwoExplicitInputYieldsSameValues() throws IOException {

        // given
        BluefloodCounterRollup rollupA = new BluefloodCounterRollup()
                .withCount(6) // 1+2+3
                .withRate(3)
                .withSampleCount(3);
        BluefloodCounterRollup rollupB = new BluefloodCounterRollup()
                .withCount(15) // 4+5+6
                .withRate(7.5d)
                .withSampleCount(3);

        Points<BluefloodCounterRollup> combined = new Points<BluefloodCounterRollup>();
        combined.add(new Points.Point<BluefloodCounterRollup>(1234L, rollupA));
        combined.add(new Points.Point<BluefloodCounterRollup>(1235L, rollupB));

        // when
        BluefloodCounterRollup rollup = BluefloodCounterRollup.buildRollupFromCounterRollups(combined);

        // then
        Number count = rollup.getCount();
        assertNotNull(count);
        assertThat(count, is(CoreMatchers.<Number>instanceOf(Long.class)));
        assertEquals(21L, count.longValue()); // 1+2+3+4+5+6
        assertEquals(5.25d, rollup.getRate(), 0.00001d); // (1+2+3+4+5+6)/((3-1)+(3-1))=5.25
        assertEquals(6, rollup.getSampleCount());
    }

    @Test
    public void hasDataWithZeroSampleCountReturnsFalse() {
        // given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup()
                .withCount(1)
                .withRate(1)
                .withSampleCount(0);
        // expect
        assertFalse(rollup.hasData());
    }

    @Test
    public void hasDataWithNonZeroSampleCountReturnsTrue() {
        // given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup()
                .withCount(1)
                .withRate(1)
                .withSampleCount(1);
        // expect
        assertTrue(rollup.hasData());
    }

    @Test
    public void getRollupTypeReturnsCounter() {
        // given
        BluefloodCounterRollup rollup = new BluefloodCounterRollup();
        // expect
        assertEquals(RollupType.COUNTER, rollup.getRollupType());
    }
}
