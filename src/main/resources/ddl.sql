-- A table with all the data types for postgres
CREATE TABLE sample_data (
    id SERIAL PRIMARY KEY,  -- Auto-incremented ID

    -- Numeric Types
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    decimal_col DECIMAL(10, 2),
    numeric_col NUMERIC(10, 2),
    real_col REAL,
    double_precision_col DOUBLE PRECISION,
    serial_col SERIAL,
    bigserial_col BIGSERIAL,

    -- Character Types
    char_col CHAR(1),
    varchar_col VARCHAR(255),
    text_col TEXT,

    -- Date/Time Types
    date_col DATE,
    time_col TIME,
    time_with_tz_col TIME WITH TIME ZONE,
    timestamp_col TIMESTAMP,
    timestamp_with_tz_col TIMESTAMP WITH TIME ZONE,
    interval_col INTERVAL,

    -- Boolean Type
    boolean_col BOOLEAN,

    -- Binary Types
    bytea_col BYTEA,

    -- Network Address Types
    inet_col INET,
    cidr_col CIDR,
    macaddr_col MACADDR,

    -- JSON Types
    json_col JSON,
    jsonb_col JSONB,

    -- UUID Type
    uuid_col UUID,

    -- Geometric Types
    point_col POINT,
    line_col LINE,
    lseg_col LSEG,
    box_col BOX,
    path_col PATH,
    polygon_col POLYGON,
    circle_col CIRCLE,

    -- Range Types
    int4range_col INT4RANGE,
    int8range_col INT8RANGE,
    numrange_col NUMRANGE,
    tsrange_col TSRANGE,
    tstzrange_col TSTZRANGE,
    daterange_col DATERANGE
);