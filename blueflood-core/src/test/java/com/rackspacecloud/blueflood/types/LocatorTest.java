package com.rackspacecloud.blueflood.types;

import org.junit.Test;

import static org.junit.Assert.*;

public class LocatorTest {

    final String tenant = "tenant";
    final String metricName = "some.metric.name";
    final String fullyQualifiedName = tenant + "." + metricName;

    @Test
    public void publicConstructorDoesNotSetStringRepresentation() {
        // when
        Locator locator = new Locator();

        // then
        assertNotNull(locator);
        assertNull(locator.toString());
    }

    @Test
    public void publicConstructorDoesNotSetTenant() {
        // when
        Locator locator = new Locator();

        // then
        assertNotNull(locator);
        assertNull(locator.getTenantId());
    }

    @Test
    public void publicConstructorDoesNotSetMetricName() {
        // when
        Locator locator = new Locator();

        // then
        assertNotNull(locator);
        assertNull(locator.getMetricName());
    }

    @Test
    public void setStringRepSetsTenant() {

        // given
        Locator locator = new Locator();

        // when
        locator.setStringRep(fullyQualifiedName);

        // then
        assertEquals(tenant, locator.getTenantId());
    }

    @Test
    public void setStringRepSetsMetricName() {

        // given
        Locator locator = new Locator();

        // when
        locator.setStringRep(fullyQualifiedName);

        // then
        assertEquals(metricName, locator.getMetricName());
    }

    @Test
    public void setStringRepSetsStringRepresentation() {

        // given
        Locator locator = new Locator();

        // when
        locator.setStringRep(fullyQualifiedName);

        // then
        assertEquals(fullyQualifiedName, locator.toString());
    }

    @Test
    public void singleParamFactorySetsTenant() {

        // when
        Locator locator = Locator.createLocatorFromDbKey(fullyQualifiedName);

        // then
        assertEquals(tenant, locator.getTenantId());
    }

    @Test
    public void singleParamFactorySetsMetricName() {

        // when
        Locator locator = Locator.createLocatorFromDbKey(fullyQualifiedName);

        // then
        assertEquals(metricName, locator.getMetricName());
    }

    @Test
    public void singleParamFactorySetsStringRepresentation() {

        // when
        Locator locator = Locator.createLocatorFromDbKey(fullyQualifiedName);

        // then
        assertEquals(fullyQualifiedName, locator.toString());
    }

    @Test
    public void multiParamFactorySetsStringRepresentation() {

        // given
        String expected = tenant + ".a.b.c";

        // when
        Locator locator = Locator.createLocatorFromPathComponents(tenant, "a", "b", "c");

        // then
        assertEquals(expected, locator.toString());
    }

    @Test
    public void multiParamFactorySetsTenantFromFirstParam() {

        // when
        Locator locator = Locator.createLocatorFromPathComponents(tenant, "a", "b", "c");

        // then
        assertEquals(tenant, locator.getTenantId());
    }

    @Test
    public void multiParamFactorySetsMetricNameFromPathComponents() {

        // given
        String expected = "a.b.c";

        // when
        Locator locator = Locator.createLocatorFromPathComponents(tenant, "a", "b", "c");

        // then
        assertEquals(expected, locator.getMetricName());
    }

    @Test(expected = NullPointerException.class)
    public void isValidDBKeyThrowsExceptionOnNullDbKey() {
        // TODO: This is incorrect behavior. The test is merely documenting it. It should be fixed in the future.

        // given
        Locator locator = new Locator();  // the method is not static, so we have to create an instance

        // when
        locator.isValidDBKey(null, "a");

        // then
        // the exception is thrown
    }

    @Test(expected = NullPointerException.class)
    public void isValidDBKeyThrowsExceptionOnNullDelim() {
        // TODO: This is incorrect behavior. The test is merely documenting it. It should be fixed in the future.

        // given
        Locator locator = new Locator();  // the method is not static, so we have to create an instance

        // when
        locator.isValidDBKey("a.b.c", null);

        // then
        // the exception is thrown
    }

    @Test
    public void isValidDBKeyUndelimitedStringReturnsFalse() {

        // given
        Locator locator = new Locator();  // the method is not static, so we have to create an instance

        // expect
        assertFalse(locator.isValidDBKey("abc", "."));
    }

    @Test
    public void isValidDBKeyDelimiterIsNormalCharInDbKeyReturnsTrue() {

        // given
        Locator locator = new Locator();  // the method is not static, so we have to create an instance

        // expect
        assertTrue(locator.isValidDBKey("abc", "b"));
    }

    @Test
    public void isValidDBKeyDbKeyContainsDelimiterReturnsTrue() {

        // given
        Locator locator = new Locator();  // the method is not static, so we have to create an instance

        // expect
        assertTrue(locator.isValidDBKey("a.b.c", "."));
    }

    @Test
    public void isValidDBKeyEmptyDbKeyReturnsFalse() {

        // given
        Locator locator = new Locator();  // the method is not static, so we have to create an instance

        // expect
        assertFalse(locator.isValidDBKey("", "."));
    }

    @Test
    public void isValidDBKeyEmptyDelimiterReturnsTrue() {

        // given
        Locator locator = new Locator();  // the method is not static, so we have to create an instance

        // expect
        assertTrue(locator.isValidDBKey("a.b.c", ""));
    }

    @Test
    public void isValidDBKeyMultiCharDelimiterReturnsTrue() {

        // given
        Locator locator = new Locator();  // the method is not static, so we have to create an instance

        // expect
        assertTrue(locator.isValidDBKey("a.b.c", ".b."));
    }

    @Test
    public void hashCodeNullReturnsZero() {

        // given
        Locator locator = new Locator();

        // expect
        assertEquals(0, locator.hashCode());
    }

    @Test
    public void hashCodeReturnsHashCodeOfStringRepresentation() {

        // given
        Locator locator = Locator.createLocatorFromDbKey(fullyQualifiedName);

        // expect
        assertEquals(fullyQualifiedName.hashCode(), locator.hashCode());
    }

    @Test
    public void equalsNullReturnsFalse() {

        // given
        Locator locator = new Locator();

        // expect
        assertFalse(locator.equals((Object)null));
    }

    @Test
    public void equalsNonLocatorReturnsFalse() {

        // given
        Locator locator = new Locator();

        // expect
        assertFalse(locator.equals(new Object()));
    }

    @Test
    public void equalsDifferentStringReturnsFalse() {

        // given
        Locator locator = Locator.createLocatorFromDbKey("a.b.c");
        Locator other = Locator.createLocatorFromDbKey("a.b.d");

        // expect
        assertFalse(locator.equals((Object)other));
    }

    @Test
    public void equalsSameStringReturnsTrue() {

        // given
        Locator locator = Locator.createLocatorFromDbKey("a.b.c");
        Locator other = Locator.createLocatorFromDbKey("a.b.c");

        // expect
        assertTrue(locator.equals((Object)other));
    }

    @Test
    public void equalsBothUninitializedReturnsTrue() {

        // given
        Locator locator = new Locator();
        Locator other = new Locator();

        // expect
        assertTrue(locator.equals((Object)other));
    }

    @Test
    public void equalsThisUninitializedReturnsFalse() {

        // given
        Locator locator = new Locator();
        Locator other = Locator.createLocatorFromDbKey("a.b.d");

        // expect
        assertFalse(locator.equals((Object)other));
    }

    @Test
    public void equalsOtherUninitializedReturnsFalse() {

        // given
        Locator locator = Locator.createLocatorFromDbKey("a.b.c");
        Locator other = new Locator();

        // expect
        assertFalse(locator.equals((Object)other));
    }

    @Test(expected = NullPointerException.class)
    public void equals2ThisNullStringRepThrowsException() {
        // TODO: This is incorrect behavior. The test is merely documenting it. It should be fixed in the future.

        // given
        Locator locator = new Locator();
        Locator other = Locator.createLocatorFromDbKey("a.b.d");

        // when
        boolean result = locator.equals(other);

        // then
        // the exception is thrown
    }

    @Test
    public void equals2OtherNullStringRepReturnsFalse() {

        // given
        Locator locator = Locator.createLocatorFromDbKey("a.b.c");
        Locator other = new Locator();

        // expect
        assertFalse(locator.equals(other));
    }

    @Test(expected = NullPointerException.class)
    public void equals2OtherNullThrowsException() {
        // TODO: This is incorrect behavior. The test is merely documenting it. It should be fixed in the future.

        // given
        Locator locator = Locator.createLocatorFromDbKey("a.b.c");

        // when
        boolean result = locator.equals((Locator)null);

        // then
        // the exception is thrown
    }

    @Test(expected = NullPointerException.class)
    public void compareToOtherNullThrowsException() {
        // TODO: This is incorrect behavior. The test is merely documenting it. It should be fixed in the future.

        // given
        Locator locator = Locator.createLocatorFromDbKey("a.b.c");

        // when
        int comparison = locator.compareTo(null);

        // then
        // the exception is thrown
    }

    @Test(expected = NullPointerException.class)
    public void compareToThisNullStringRepThrowsException() {
        // TODO: This is incorrect behavior. The test is merely documenting it. It should be fixed in the future.

        // given
        Locator locator = new Locator();
        Locator other = Locator.createLocatorFromDbKey("a.b.d");

        // when
        int comparison = locator.compareTo(other);

        // then
        // the exception is thrown
    }

    @Test
    public void compareToBothEmptyReturnsSame() {

        // given
        Locator locator = Locator.createLocatorFromDbKey("");
        Locator other = Locator.createLocatorFromDbKey("");

        // when
        int comparison = locator.compareTo(other);

        // then
        assertEquals(0, comparison);
    }

    @Test
    public void compareToThisHigherReturnsHigher() {

        // given
        Locator locator = Locator.createLocatorFromDbKey("b");
        Locator other = Locator.createLocatorFromDbKey("a");

        // when
        int comparison = locator.compareTo(other);

        // then
        assertEquals(1, comparison);
    }

    @Test
    public void compareToThisLowerReturnsLower() {

        // given
        Locator locator = Locator.createLocatorFromDbKey("a");
        Locator other = Locator.createLocatorFromDbKey("b");

        // when
        int comparison = locator.compareTo(other);

        // then
        assertEquals(-1, comparison);
    }
}
