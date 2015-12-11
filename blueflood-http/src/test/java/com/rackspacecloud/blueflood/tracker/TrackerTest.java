package com.rackspacecloud.blueflood.tracker;

import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TrackerTest {
    Tracker tracker;
    String testTenant1 = "tenant1";
    String testTenant2 = "tenant2";

    @Before
    public void setUp() {
        tracker = new Tracker();
    }

    @Test
    public void testAddTenant() {
        tracker.addTenant(testTenant1);

        Set tenants = tracker.getTenants();
        assertTrue( "tenant " + testTenant1 + " not added", tracker.isTracking( testTenant1 ) );
        assertTrue( "tenants.size not 1", tenants.size() == 1 );
        assertTrue( "tenants does not contain " + testTenant1, tenants.contains( testTenant1 ) );
    }

    @Test
    public void testDoesNotAddTenantTwice() {
        tracker.addTenant(testTenant1);
        tracker.addTenant(testTenant1);

        Set tenants = tracker.getTenants();
        assertTrue( "tenant " + testTenant1 + " not added", tracker.isTracking( testTenant1 ) );
        assertTrue( "tenants.size not 1", tenants.size() == 1 );
    }

    @Test
    public void testRemoveTenant() {
        tracker.addTenant(testTenant1);
        assertTrue( "tenant " + testTenant1 + " not added", tracker.isTracking( testTenant1 ) );

        tracker.removeTenant(testTenant1);

        Set tenants = tracker.getTenants();
        assertFalse( "tenant " + testTenant1 + " not removed", tracker.isTracking( testTenant1 ) );
        assertEquals( "tenants.size not 0", tenants.size(), 0 );
        assertFalse( "tenants contains " + testTenant1, tenants.contains( testTenant1 ) );
    }

    @Test
    public void testRemoveAllTenant() {
        tracker.addTenant(testTenant1);
        tracker.addTenant(testTenant2);
        assertTrue( "tenant " + testTenant1 + " not added", tracker.isTracking( testTenant1 ) );
        assertTrue( "tenant " + testTenant2 + " not added", tracker.isTracking( testTenant2 ) );

        tracker.removeAllTenants();
        assertFalse( "tenant " + testTenant1 + " not removed", tracker.isTracking( testTenant1 ) );
        assertFalse( "tenant " + testTenant2 + " not removed", tracker.isTracking( testTenant2 ) );

        Set tenants = tracker.getTenants();
        assertEquals( "tenants.size not 0", tenants.size(), 0 );
    }

    @Test
    public void testFindTidFound() {

        assertEquals( Tracker.findTid( "/v2.0/6000/views" ), "6000" );
    }

    @Test
         public void testTrackTenantNoVersion() {

        assertEquals( Tracker.findTid( "/6000/views" ), null );
    }

    @Test
    public void testTrackTenantBadVersion() {

        assertEquals( Tracker.findTid( "blah/6000/views" ), null );
    }

    @Test
    public void testTrackTenantTrailingSlash() {

        assertEquals( Tracker.findTid( "/v2.0/6000/views/" ), "6000" );
    }
}
