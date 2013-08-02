CREATE KEYSPACE DATA
    WITH placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy'
    AND strategy_options = {replication_factor:1};
USE DATA;

CREATE COLUMN FAMILY metrics_locator WITH column_type='Standard' AND comparator='UTF8Type' AND key_validation_class='LongType' AND default_validation_class='UTF8Type';
CREATE COLUMN FAMILY metrics_discovery WITH column_type='Standard' AND comparator='UTF8Type' AND key_validation_class='UTF8Type' AND default_validation_class='UTF8Type';
CREATE COLUMN FAMILY metrics_state WITH column_type='Standard' AND comparator='UTF8Type' AND key_validation_class='LongType' AND default_validation_class='LongType';
CREATE COLUMN FAMILY metrics_metadata WITH column_type='Standard' AND comparator='UTF8Type' AND key_validation_class='UTF8Type' AND default_validation_class='BytesType';
CREATE COLUMN FAMILY metrics_string WITH column_type='Standard' AND comparator='LongType' AND key_validation_class='UTF8Type' AND default_validation_class='UTF8Type';
CREATE COLUMN FAMILY metrics_full WITH column_type='Standard' AND comparator='LongType' AND key_validation_class='UTF8Type' AND default_validation_class='BytesType';
CREATE COLUMN FAMILY metrics_5m WITH column_type='Standard' AND comparator='LongType' AND key_validation_class='UTF8Type' AND default_validation_class='BytesType';
CREATE COLUMN FAMILY metrics_20m WITH column_type='Standard' AND comparator='LongType' AND key_validation_class='UTF8Type' AND default_validation_class='BytesType';
CREATE COLUMN FAMILY metrics_60m WITH column_type='Standard' AND comparator='LongType' AND key_validation_class='UTF8Type' AND default_validation_class='BytesType';
CREATE COLUMN FAMILY metrics_240m WITH column_type='Standard' AND comparator='LongType' AND key_validation_class='UTF8Type' AND default_validation_class='BytesType';
CREATE COLUMN FAMILY metrics_1440m WITH column_type='Standard' AND comparator='LongType' AND key_validation_class='UTF8Type' AND default_validation_class='BytesType';
