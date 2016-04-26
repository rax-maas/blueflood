package com.rackspacecloud.blueflood.types;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.*;

public class BluefloodTimerRollupPercentileTest {

    @Test
    public void maybePromoteDoubleYieldsSameValue() {
        // given
        Double value = 123.45d;
        // when
        Number result = BluefloodTimerRollup.Percentile.maybePromote(value);
        // then
        assertNotNull(result);
        assertEquals(Double.class, result.getClass());
        assertEquals(123.45d, (Double)result, 0.00001d);
    }

    @Test
    public void maybePromoteFloatPromotesToDouble() {
        // given
        Float value = 123.45f;
        // when
        Number result = BluefloodTimerRollup.Percentile.maybePromote(value);
        // then
        assertNotNull(result);
        assertEquals(Double.class, result.getClass());
        assertEquals(123.45d, (Double)result, 0.00001d);
    }

    @Test
    public void maybePromoteLongYieldsSameValue() {
        // given
        Long value = 123L;
        // when
        Number result = BluefloodTimerRollup.Percentile.maybePromote(value);
        // then
        assertNotNull(result);
        assertEquals(Long.class, result.getClass());
        assertEquals(123L, (long)(Long)result);
    }

    @Test
    public void maybePromoteIntegerPromotesToLong() {
        // given
        Integer value = 123;
        // when
        Number result = BluefloodTimerRollup.Percentile.maybePromote(value);
        // then
        assertNotNull(result);
        assertEquals(Long.class, result.getClass());
        assertEquals(123L, (long)(Long)result);
    }

    @Test
    public void maybePromoteShortYieldsSameValue() {
        // TODO: This is probably incorrect behavior. The Percentile
        // constructor appears to want only longs and doubles, not shorts.

        // given
        Short value = 123;
        // when
        Number result = BluefloodTimerRollup.Percentile.maybePromote(value);
        // then
        assertNotNull(result);
        assertEquals(Short.class, result.getClass());
        assertEquals(123L, (short)(Short)result);
    }

    @Test
    public void maybePromoteByteYieldsSameValue() {
        // TODO: This is probably incorrect behavior. The Percentile
        // constructor appears to want only longs and doubles, not bytes.

        // given
        Byte value = 123;
        // when
        Number result = BluefloodTimerRollup.Percentile.maybePromote(value);
        // then
        assertNotNull(result);
        assertEquals(Byte.class, result.getClass());
        assertEquals(123L, (byte)(Byte)result);
    }

    @Test
    public void maybePromoteBigIntegerYieldsSameValue() {
        // TODO: This is probably incorrect behavior. The Percentile
        // constructor appears to want only longs and doubles, not BigIntegers.

        // given
        BigInteger value = BigInteger.valueOf(123);
        // when
        Number result = BluefloodTimerRollup.Percentile.maybePromote(value);
        // then
        assertNotNull(result);
        assertEquals(BigInteger.class, result.getClass());
        assertEquals(value, result);
    }

    @Test
    public void maybePromoteBigDecimalYieldsSameValue() {
        // TODO: This is probably incorrect behavior. The Percentile
        // constructor appears to want only longs and doubles, not BigDecimals.

        // given
        BigDecimal value = BigDecimal.valueOf(123.45d);
        // when
        Number result = BluefloodTimerRollup.Percentile.maybePromote(value);
        // then
        assertNotNull(result);
        assertEquals(BigDecimal.class, result.getClass());
        assertEquals(value, result);
    }

    @Test
    public void constructorSetsMeanAndGetMeanReturnsMean() {
        // when
        BluefloodTimerRollup.Percentile value = new BluefloodTimerRollup.Percentile((Number)123L);
        // then
        assertEquals(123L, value.getMean());
    }

    @Test
    public void longConstructorSetsMean() {
        // when
        BluefloodTimerRollup.Percentile value = new BluefloodTimerRollup.Percentile(123L);
        // then
        assertEquals(123L, value.getMean());
    }

    @Test
    public void doubleConstructorSetsMean() {
        // when
        BluefloodTimerRollup.Percentile value = new BluefloodTimerRollup.Percentile(123.45d);
        // then
        assertEquals(123.45d, (Double)value.getMean(), 0.00001d);
    }

    @Test
    public void equalsWithNullReturnsFalse() {
        // given
        BluefloodTimerRollup.Percentile value = new BluefloodTimerRollup.Percentile((Number)123L);
        // expect
        assertFalse(value.equals(null));
    }

    @Test
    public void equalsWithNonPercentileReturnsFalse() {
        // given
        BluefloodTimerRollup.Percentile value = new BluefloodTimerRollup.Percentile((Number)123L);
        // expect
        assertFalse(value.equals(new Object()));
    }

    @Test
    public void equalsWithSelfReturnsTrue() {
        // given
        BluefloodTimerRollup.Percentile value = new BluefloodTimerRollup.Percentile((Number)123L);
        // expect
        assertTrue(value.equals(value));
    }

    @Test
    public void equalsWithOtherRollupWithSameLongValueReturnsTrue() {
        // given
        BluefloodTimerRollup.Percentile a = new BluefloodTimerRollup.Percentile((Number)123L);
        BluefloodTimerRollup.Percentile b = new BluefloodTimerRollup.Percentile((Number)123L);
        // expect
        assertTrue(a.equals(b));
    }

    @Test
    public void equalsWithOtherRollupWithSameDoubleValueReturnsTrue() {
        // given
        BluefloodTimerRollup.Percentile a = new BluefloodTimerRollup.Percentile((Number)123.0d);
        BluefloodTimerRollup.Percentile b = new BluefloodTimerRollup.Percentile((Number)123.0d);
        // expect
        assertTrue(a.equals(b));
    }

    @Test
    public void equalsWithOtherRollupWithSlightlyDifferentDoubleValueReturnsTrue() {
        // given
        BluefloodTimerRollup.Percentile a = new BluefloodTimerRollup.Percentile((Number)123.0d);
        BluefloodTimerRollup.Percentile b = new BluefloodTimerRollup.Percentile((Number)123.00000000000001d); // least-significant bit different
        // expect
        assertFalse(a.equals(b));
    }

    @Test
    public void equalsWithDoubleAndLongOfSameValueReturnsFalse() {
        // given
        BluefloodTimerRollup.Percentile a = new BluefloodTimerRollup.Percentile((Number)123L);
        BluefloodTimerRollup.Percentile b = new BluefloodTimerRollup.Percentile((Number)123.0d);
        // expect
        assertTrue(123L == 123.0d);
        assertFalse(a.equals(b));
    }

    @Test
    public void toStringWithLongPrintsMean() {
        // given
        BluefloodTimerRollup.Percentile value = new BluefloodTimerRollup.Percentile((Number)123L);
        // expect
        assertEquals("{mean:123}", value.toString());
    }

    @Test
    public void toStringWithDoublePrintsMean() {
        // given
        BluefloodTimerRollup.Percentile value = new BluefloodTimerRollup.Percentile((Number)123.45d);
        // expect
        assertEquals("{mean:123.45}", value.toString());
    }
}
