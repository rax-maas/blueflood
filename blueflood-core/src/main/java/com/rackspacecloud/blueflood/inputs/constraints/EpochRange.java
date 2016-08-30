package com.rackspacecloud.blueflood.inputs.constraints;

import javax.validation.Constraint;
import javax.validation.Payload;
import javax.validation.ReportAsSingleViolation;
import javax.validation.constraints.Min;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 *
 * The annotated element has to be in the appropriate range relative to the current time.
 * Applicable for values of type long which represent time in epoch. The range of the
 * element is defined as below.
 *
 * (currentTime - maxPast) <= element <= (currentTime + maxFuture)
 *
 */
@Target({ METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER })
@Retention(RUNTIME)
@Documented
@Constraint(validatedBy = { EpochRangeValidator.class })
@ReportAsSingleViolation
public @interface EpochRange {

    String message() default "{com.rackspacecloud.blueflood.inputs.constraints.EpochRange.message}";

    Class<?>[] groups() default { };

    Class<? extends Payload>[] payload() default { };

    /**
     * @return the element cannot be older than (current time -  maxPast) milli seconds in the past
     */
    EpochRangeLimits maxPast();

    /**
     * @return the element cannot be earlier tha (current time +  maxFuture) milli seconds in the future
     */
    EpochRangeLimits maxFuture();

    /**
     * Defines several {@link Min} annotations on the same element.
     *
     * @see Min
     */
    @Target({ METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER })
    @Retention(RUNTIME)
    @Documented
    @interface List {

        EpochRange[] value();
    }
}
