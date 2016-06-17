package com.rackspacecloud.blueflood.types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AbstractRollupStatTest {
    class SimpleStat extends AbstractRollupStat {

        public SimpleStat() { }
        public SimpleStat(long value) {
            setLongValue(value);
        }
        public SimpleStat(double value) {
            setDoubleValue(value);
        }

        @Override
        void handleFullResMetric(Object o) throws RuntimeException {

        }

        @Override
        void handleRollupMetric(IBaseRollup baseRollup) throws RuntimeException {

        }

        @Override
        public byte getStatType() {
            return -1;
        }
    }

    @Test
    public void newlyCreatedObjectIsNotFloatingPoint() {

        // when
        SimpleStat stat = new SimpleStat();

        // then
        assertFalse(stat.isFloatingPoint());
    }

    @Test
    public void newlyCreatedObjectHasValueZero() {

        // when
        SimpleStat stat = new SimpleStat();

        // then
        assertEquals(0L, stat.toLong());
        assertEquals(0d, stat.toDouble(), 0.00001);
    }

    @Test
    public void statsAreEqualIfTheirLongNumericalValuesAreEqual() {

        // given
        SimpleStat a = new SimpleStat(123L);
        SimpleStat b = new SimpleStat(123L);

        // expect
        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
    }

    @Test
    public void statsAreNotEqualIfTheirLongNumericalValuesAreNotEqual() {

        // given
        SimpleStat a = new SimpleStat(123L);
        SimpleStat b = new SimpleStat(124L);

        // expect
        assertFalse(a.equals(b));
        assertFalse(b.equals(a));
    }

    @Test
    public void statsAreEqualIfTheirDoubleNumericalValuesAreEqual() {

        // given
        SimpleStat a = new SimpleStat(123.45d);
        SimpleStat b = new SimpleStat(123.45d);

        // expect
        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
    }

    @Test
    public void statsAreNotEqualIfTheirDoubleNumericalValuesAreNotEqual() {

        // given
        SimpleStat a = new SimpleStat(123.45d);
        SimpleStat b = new SimpleStat(123.7d);

        // expect
        assertFalse(a.equals(b));
        assertFalse(b.equals(a));
    }

    @Test
    public void floatingPointAndNonFloatingPointAreNotEqual() {

        // given
        SimpleStat a = new SimpleStat(123L);
        SimpleStat b = new SimpleStat(123.45d);

        // expect
        assertFalse(a.equals(b));
        assertFalse(b.equals(a));
    }

    @Test
    public void differentSubtypesAreEqualIfTheirLongNumericalValuesAreEqual() {

        // given
        MinValue min = new MinValue(123L);
        MaxValue max = new MaxValue(123L);

        // expect
        assertTrue(min.equals(max));
        assertTrue(max.equals(min));
    }

    @Test
    public void differentSubtypesAreNotEqualIfTheirLongNumericalValuesAreNotEqual() {

        // given
        MinValue min = new MinValue(123L);
        MaxValue max = new MaxValue(124L);

        // expect
        assertFalse(min.equals(max));
        assertFalse(max.equals(min));
    }

    @Test
    public void differentSubtypesAreEqualIfTheirDoubleNumericalValuesAreEqual() {

        // given
        MinValue min = new MinValue(123.45d);
        MaxValue max = new MaxValue(123.45d);

        // expect
        assertTrue(min.equals(max));
        assertTrue(max.equals(min));
    }

    @Test
    public void differentSubtypesAreNotEqualIfTheirDoubleNumericalValuesAreNotEqual() {

        // given
        MinValue min = new MinValue(123.45d);
        MaxValue max = new MaxValue(123.7d);

        // expect
        assertFalse(min.equals(max));
        assertFalse(max.equals(min));
    }

    @Test
    public void notEqualToOtherNonStatTypes() {

        // given
        SimpleStat stat = new SimpleStat();
        String other = "something";

        // expect
        assertFalse(stat.equals(other));
        assertFalse(other.equals(stat));
    }

    @Test
    public void setLongValueSetsValue() {

        // given
        SimpleStat stat = new SimpleStat(123L);

        // precondition
        assertEquals(123L, stat.toLong());

        // when
        stat.setLongValue(124L);

        // then
        assertEquals(124L, stat.toLong());
    }

    @Test
    public void setLongValueChangesToNonFloatingPoint() {

        // given
        SimpleStat stat = new SimpleStat(123.45d);

        // precondition
        assertTrue(stat.isFloatingPoint());

        // when
        stat.setLongValue(124L);

        // then
        assertFalse(stat.isFloatingPoint());
    }

    @Test
    public void setDoubleValueSetsValue() {

        // given
        SimpleStat stat = new SimpleStat(123.45d);

        // precondition
        assertEquals(123.45d, stat.toDouble(), 0.00001d);

        // when
        stat.setDoubleValue(124.67d);

        // then
        assertEquals(124.67d, stat.toDouble(), 0.00001d);
    }

    @Test
    public void setDoubleValueChangesToFloatingPoint() {

        // given
        SimpleStat stat = new SimpleStat(123L);

        // precondition
        assertFalse(stat.isFloatingPoint());

        // when
        stat.setDoubleValue(124.67d);

        // then
        assertTrue(stat.isFloatingPoint());
    }

    @Test
    public void staticSetWithLongSetsValue() {

        // given
        SimpleStat stat = new SimpleStat(123.45d);

        // precondition
        assertTrue(stat.isFloatingPoint());
        assertEquals(123.45d, stat.toDouble(), 0.00001d);

        // when
        AbstractRollupStat.set(stat, 4L);

        // then
        assertFalse(stat.isFloatingPoint());
        assertEquals(4L, stat.toLong());
    }

    @Test
    public void staticSetWithIntSetsValue() {

        // given
        SimpleStat stat = new SimpleStat(123.45d);

        // precondition
        assertTrue(stat.isFloatingPoint());
        assertEquals(123.45d, stat.toDouble(), 0.00001d);

        // when
        AbstractRollupStat.set(stat, (int)3);

        // then
        assertFalse(stat.isFloatingPoint());
        assertEquals(3L, stat.toLong());
    }

    @Test
    public void staticSetWithDoubleSetsValue() {

        // given
        SimpleStat stat = new SimpleStat(123L);

        // precondition
        assertFalse(stat.isFloatingPoint());
        assertEquals(123L, stat.toLong());

        // when
        AbstractRollupStat.set(stat, 5.5d);

        // then
        assertTrue(stat.isFloatingPoint());
        assertEquals(5.5d, stat.toDouble(), 0.00001d);
    }

    @Test
    public void staticSetWithFloatSetsValue() {

        // given
        SimpleStat stat = new SimpleStat(123L);

        // precondition
        assertFalse(stat.isFloatingPoint());
        assertEquals(123L, stat.toLong());

        // when
        AbstractRollupStat.set(stat, 6.5f);

        // then
        assertTrue(stat.isFloatingPoint());
        assertEquals(6.5d, stat.toDouble(), 0.00001d);
    }

    @Test(expected = ClassCastException.class)
    public void staticSetWithOtherTypeThrowsException() {

        // given
        SimpleStat stat = new SimpleStat(123L);

        // precondition
        assertFalse(stat.isFloatingPoint());
        assertEquals(123L, stat.toLong());

        // when
        AbstractRollupStat.set(stat, (byte)7);

        // then
        // the exception is thrown
    }

    @Test
    public void setLongDoesNotChangeDoubleValue() {

        // given
        double value = 123.45d;
        SimpleStat stat = new SimpleStat(value);

        // when
        stat.setLongValue(678L);

        // then
        assertEquals(value, stat.toDouble(), 0.00001d);
    }

    @Test
    public void setDoubleDoesNotChangeLongValue() {

        // given
        long value = 123L;
        SimpleStat stat = new SimpleStat(value);

        // when
        stat.setDoubleValue(678.9d);

        // then
        assertEquals(value, stat.toLong());
    }
}
