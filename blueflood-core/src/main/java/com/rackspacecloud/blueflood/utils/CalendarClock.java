package com.rackspacecloud.blueflood.utils;

import org.joda.time.Instant;

import java.util.Calendar;

public class CalendarClock implements Clock {
    public CalendarClock() {
        this(Calendar.getInstance());
    }
    public CalendarClock(Calendar calendar) {
        this.calendar = calendar;
    }

    Calendar calendar;

    @Override
    public Instant now() {
        return new Instant(calendar.getTimeInMillis());
    }
}
