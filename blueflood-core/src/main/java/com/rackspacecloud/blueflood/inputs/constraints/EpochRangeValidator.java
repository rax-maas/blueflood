package com.rackspacecloud.blueflood.inputs.constraints;

import com.rackspacecloud.blueflood.utils.Clock;
import com.rackspacecloud.blueflood.utils.DefaultClockImpl;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class EpochRangeValidator implements ConstraintValidator<EpochRange, Long> {

    private final Clock clock = new DefaultClockImpl();

    private long maxPast;
    private long maxFuture;

    @Override
    public void initialize(EpochRange constraintAnnotation) {
        this.maxPast = constraintAnnotation.maxPast().getValue();
        this.maxFuture = constraintAnnotation.maxFuture().getValue();
    }

    @Override
    public boolean isValid(Long value, ConstraintValidatorContext context) {

        long currentTime = clock.now().getMillis();

        if ( value < currentTime - maxPast ) {
            // collectionTime is too far in the past
            return false;
        } else if ( value > currentTime + maxFuture ) {
            // collectionTime is too far in the future
            return false;
        }
        
        return true;
    }
}
