package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.utils.DefaultClockImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class ScheduleContextAddRemoveShardsTest {


    long currentTime;
    List<Integer> managedShards;

    ShardStateManager shardStateManager;
    ShardLockManager lockManager;

    ScheduleContext ctx;

    @Before
    public void setUp() {

        // given
        currentTime = 1234000L;
        managedShards = new ArrayList<Integer>() {{
            add(0);
        }};

        shardStateManager = mock(ShardStateManager.class);
        lockManager = mock(ShardLockManager.class);

        ctx = new ScheduleContext(currentTime, managedShards,
                new DefaultClockImpl(), shardStateManager, lockManager);
    }

    @Test
    public void addShardAddsShard() {

        // given
        final Integer[] capture1 = new Integer[1];
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                capture1[0] = (Integer) invocationOnMock.getArguments()[0];
                return null;
            }
        }).when(shardStateManager).add(anyInt());

        final Integer[] capture2 = new Integer[1];
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                capture2[0] = (Integer) invocationOnMock.getArguments()[0];
                return null;
            }
        }).when(lockManager).addShard(anyInt());

        int shard = 123;

        // when
        ctx.addShard(shard);

        // then
        verify(shardStateManager).add(anyInt());
        verifyNoMoreInteractions(shardStateManager);
        assertNotNull(capture1[0]);
        assertEquals(shard, capture1[0].intValue());

        verify(lockManager).addShard(anyInt());
        verifyNoMoreInteractions(lockManager);
        assertNotNull(capture2[0]);
        assertEquals(shard, capture2[0].intValue());
    }

    @Test
    public void removeShardRemovesShard() {

        // given
        final Integer[] capture1 = new Integer[1];
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                capture1[0] = (Integer) invocationOnMock.getArguments()[0];
                return null;
            }
        }).when(shardStateManager).remove(anyInt());

        final Integer[] capture2 = new Integer[1];
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                capture2[0] = (Integer) invocationOnMock.getArguments()[0];
                return null;
            }
        }).when(lockManager).removeShard(anyInt());

        int shard = 123;

        // when
        ctx.removeShard(shard);

        // then
        verify(shardStateManager).remove(anyInt());
        verifyNoMoreInteractions(shardStateManager);
        assertNotNull(capture1[0]);
        assertEquals(shard, capture1[0].intValue());

        verify(lockManager).removeShard(anyInt());
        verifyNoMoreInteractions(lockManager);
        assertNotNull(capture2[0]);
        assertEquals(shard, capture2[0].intValue());
    }
}
