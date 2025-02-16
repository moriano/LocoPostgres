package org.moriano.locopostgres.model;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.math.BigDecimal;
import java.util.*;

/**
 * A (very boring) class that is used simply to map our sample_data table into a java object.
 *
 * Convenient to facilitate testing.
 *
 * We do not use primitives here on purpose as the values in the database could be nulls.
 */
public class SampleData {
    private Integer id;
    private Short smallintCol;
    private Integer integerCol;
    private Long bigintCol;
    private BigDecimal decimalCol;
    private BigDecimal numericCol;
    private Float realCol;
    private Double doublePrecisionCol;
    private String charCol;
    private String varcharCol;
    private String textCol;
    private Date dateCol;
    private Time timeCol;
    private OffsetTime timeWithTzCol;
    private Timestamp timestampCol;
    private OffsetDateTime timestampWithTzCol;
    private Duration intervalCol;
    private Boolean booleanCol;
    private byte[] byteaCol;
    private InetAddress inetCol;
    private String cidrCol;
    private String macaddrCol;
    private String jsonCol;
    private String jsonbCol;
    private UUID uuidCol;
    private String pointCol;
    private String lineCol;
    private String lsegCol;
    private String boxCol;
    private String pathCol;
    private String polygonCol;
    private String circleCol;
    private String int4rangeCol;
    private String int8rangeCol;
    private String numrangeCol;
    private String tsrangeCol;
    private String tstzrangeCol;
    private String daterangeCol;

    public SampleData(Integer id, Short smallintCol, Integer integerCol, Long bigintCol, BigDecimal decimalCol,
                      BigDecimal numericCol, Float realCol, Double doublePrecisionCol, String charCol,
                      String varcharCol, String textCol, Date dateCol, Time timeCol, OffsetTime timeWithTzCol,
                      Timestamp timestampCol, OffsetDateTime timestampWithTzCol, Duration intervalCol,
                      Boolean booleanCol, byte[] byteaCol, InetAddress inetCol, String cidrCol, String macaddrCol,
                      String jsonCol, String jsonbCol, UUID uuidCol, String pointCol, String lineCol, String lsegCol,
                      String boxCol, String pathCol, String polygonCol, String circleCol, String int4rangeCol,
                      String int8rangeCol, String numrangeCol, String tsrangeCol, String tstzrangeCol,
                      String daterangeCol) {
        this.id = id;
        this.smallintCol = smallintCol;
        this.integerCol = integerCol;
        this.bigintCol = bigintCol;
        this.decimalCol = decimalCol;
        this.numericCol = numericCol;
        this.realCol = realCol;
        this.doublePrecisionCol = doublePrecisionCol;
        this.charCol = charCol;
        this.varcharCol = varcharCol;
        this.textCol = textCol;
        this.dateCol = dateCol;
        this.timeCol = timeCol;
        this.timeWithTzCol = timeWithTzCol;
        this.timestampCol = timestampCol;
        this.timestampWithTzCol = timestampWithTzCol;
        this.intervalCol = intervalCol;
        this.booleanCol = booleanCol;
        this.byteaCol = byteaCol;
        this.inetCol = inetCol;
        this.cidrCol = cidrCol;
        this.macaddrCol = macaddrCol;
        this.jsonCol = jsonCol;
        this.jsonbCol = jsonbCol;
        this.uuidCol = uuidCol;
        this.pointCol = pointCol;
        this.lineCol = lineCol;
        this.lsegCol = lsegCol;
        this.boxCol = boxCol;
        this.pathCol = pathCol;
        this.polygonCol = polygonCol;
        this.circleCol = circleCol;
        this.int4rangeCol = int4rangeCol;
        this.int8rangeCol = int8rangeCol;
        this.numrangeCol = numrangeCol;
        this.tsrangeCol = tsrangeCol;
        this.tstzrangeCol = tstzrangeCol;
        this.daterangeCol = daterangeCol;
    }

    /**
     * A static factory to build a list of SampleData from a resultset. Very convenient
     * for testing purposes
     * @param rs
     * @return
     */
    public static List<SampleData> fromResultSet(ResultSet rs) throws SQLException, UnknownHostException {
        if (rs == null) {
            return Collections.emptyList();
        }
        List<SampleData> results = new ArrayList<>();
        while(rs.next()) {
            results.add(new SampleData(
                    rs.getInt("id"),
                    rs.getShort("smallint_col"),
                    rs.getInt("integer_col"),
                    rs.getLong("bigint_col"),
                    rs.getBigDecimal("decimal_col"),
                    rs.getBigDecimal("numeric_col"),
                    rs.getFloat("real_col"),
                    rs.getDouble("double_precision_col"),
                    rs.getString("real_col"),
                    rs.getString("varchar_col"),
                    rs.getString("text_col"),
                    rs.getDate("date_col"),
                    rs.getTime("time_col"),
                    null, // TODO MORIANO rs.getDate("time_with_tz_col"),
                    new Timestamp(rs.getDate("timestamp_col").getTime()),
                    null, // TODO MORIANO rs.getDate("timestamp_with_tz_col"),
                    null, // TODO MORIANOrs.getDate("interval_col"),
                    rs.getBoolean("boolean_col"),
                    rs.getBytes("bytea_col"),
                    InetAddress.getByName(rs.getString("inet_col")),
                    rs.getString("cidr_col"),
                    rs.getString("macaddr_col"),
                    rs.getString("json_col"),
                    rs.getString("jsonb_col"),
                    UUID.fromString(rs.getString("uuid_col")),
                    rs.getString("point_col"),
                    rs.getString("line_col"),
                    rs.getString("lseg_col"),
                    rs.getString("box_col"),
                    rs.getString("path_col"),
                    rs.getString("polygon_col"),
                    rs.getString("circle_col"),
                    rs.getString("int4range_col"),
                    rs.getString("int8range_col"),
                    rs.getString("numrange_col"),
                    rs.getString("tsrange_col"),
                    rs.getString("tstzrange_col"),
                    rs.getString("daterange_col")
            ));
        }
        return results;
    }

    public Integer getId() {
        return id;
    }

    public Short getSmallintCol() {
        return smallintCol;
    }

    public Integer getIntegerCol() {
        return integerCol;
    }

    public Long getBigintCol() {
        return bigintCol;
    }

    public BigDecimal getDecimalCol() {
        return decimalCol;
    }

    public BigDecimal getNumericCol() {
        return numericCol;
    }

    public Float getRealCol() {
        return realCol;
    }

    public Double getDoublePrecisionCol() {
        return doublePrecisionCol;
    }

    public String getCharCol() {
        return charCol;
    }

    public String getVarcharCol() {
        return varcharCol;
    }

    public String getTextCol() {
        return textCol;
    }

    public Date getDateCol() {
        return dateCol;
    }

    public Time getTimeCol() {
        return timeCol;
    }

    public Timestamp getTimestampCol() {
        return timestampCol;
    }

    public OffsetTime getTimeWithTzCol() {
        return timeWithTzCol;
    }



    public OffsetDateTime getTimestampWithTzCol() {
        return timestampWithTzCol;
    }

    public Duration getIntervalCol() {
        return intervalCol;
    }

    public Boolean getBooleanCol() {
        return booleanCol;
    }

    public byte[] getByteaCol() {
        return byteaCol;
    }

    public InetAddress getInetCol() {
        return inetCol;
    }

    public String getCidrCol() {
        return cidrCol;
    }

    public String getMacaddrCol() {
        return macaddrCol;
    }

    public String getJsonCol() {
        return jsonCol;
    }

    public String getJsonbCol() {
        return jsonbCol;
    }

    public UUID getUuidCol() {
        return uuidCol;
    }

    public String getPointCol() {
        return pointCol;
    }

    public String getLineCol() {
        return lineCol;
    }

    public String getLsegCol() {
        return lsegCol;
    }

    public String getBoxCol() {
        return boxCol;
    }

    public String getPathCol() {
        return pathCol;
    }

    public String getPolygonCol() {
        return polygonCol;
    }

    public String getCircleCol() {
        return circleCol;
    }

    public String getInt4rangeCol() {
        return int4rangeCol;
    }

    public String getInt8rangeCol() {
        return int8rangeCol;
    }

    public String getNumrangeCol() {
        return numrangeCol;
    }

    public String getTsrangeCol() {
        return tsrangeCol;
    }

    public String getTstzrangeCol() {
        return tstzrangeCol;
    }

    public String getDaterangeCol() {
        return daterangeCol;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SampleData that = (SampleData) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (smallintCol != null ? !smallintCol.equals(that.smallintCol) : that.smallintCol != null) return false;
        if (integerCol != null ? !integerCol.equals(that.integerCol) : that.integerCol != null) return false;
        if (bigintCol != null ? !bigintCol.equals(that.bigintCol) : that.bigintCol != null) return false;
        if (decimalCol != null ? !decimalCol.equals(that.decimalCol) : that.decimalCol != null) return false;
        if (numericCol != null ? !numericCol.equals(that.numericCol) : that.numericCol != null) return false;
        if (realCol != null ? !realCol.equals(that.realCol) : that.realCol != null) return false;
        if (doublePrecisionCol != null ? !doublePrecisionCol.equals(that.doublePrecisionCol) : that.doublePrecisionCol != null)
            return false;
        if (charCol != null ? !charCol.equals(that.charCol) : that.charCol != null) return false;
        if (varcharCol != null ? !varcharCol.equals(that.varcharCol) : that.varcharCol != null) return false;
        if (textCol != null ? !textCol.equals(that.textCol) : that.textCol != null) return false;
        if (dateCol != null ? !dateCol.equals(that.dateCol) : that.dateCol != null) return false;
        if (timeCol != null ? !timeCol.equals(that.timeCol) : that.timeCol != null) return false;
        if (timeWithTzCol != null ? !timeWithTzCol.equals(that.timeWithTzCol) : that.timeWithTzCol != null)
            return false;
        if (timestampCol != null ? !timestampCol.equals(that.timestampCol) : that.timestampCol != null) return false;
        if (timestampWithTzCol != null ? !timestampWithTzCol.equals(that.timestampWithTzCol) : that.timestampWithTzCol != null)
            return false;
        if (intervalCol != null ? !intervalCol.equals(that.intervalCol) : that.intervalCol != null) return false;
        if (booleanCol != null ? !booleanCol.equals(that.booleanCol) : that.booleanCol != null) return false;
        if (!Arrays.equals(byteaCol, that.byteaCol)) return false;
        if (inetCol != null ? !inetCol.equals(that.inetCol) : that.inetCol != null) return false;
        if (cidrCol != null ? !cidrCol.equals(that.cidrCol) : that.cidrCol != null) return false;
        if (macaddrCol != null ? !macaddrCol.equals(that.macaddrCol) : that.macaddrCol != null) return false;
        if (jsonCol != null ? !jsonCol.equals(that.jsonCol) : that.jsonCol != null) return false;
        if (jsonbCol != null ? !jsonbCol.equals(that.jsonbCol) : that.jsonbCol != null) return false;
        if (uuidCol != null ? !uuidCol.equals(that.uuidCol) : that.uuidCol != null) return false;
        if (pointCol != null ? !pointCol.equals(that.pointCol) : that.pointCol != null) return false;
        if (lineCol != null ? !lineCol.equals(that.lineCol) : that.lineCol != null) return false;
        if (lsegCol != null ? !lsegCol.equals(that.lsegCol) : that.lsegCol != null) return false;
        if (boxCol != null ? !boxCol.equals(that.boxCol) : that.boxCol != null) return false;
        if (pathCol != null ? !pathCol.equals(that.pathCol) : that.pathCol != null) return false;
        if (polygonCol != null ? !polygonCol.equals(that.polygonCol) : that.polygonCol != null) return false;
        if (circleCol != null ? !circleCol.equals(that.circleCol) : that.circleCol != null) return false;
        if (int4rangeCol != null ? !int4rangeCol.equals(that.int4rangeCol) : that.int4rangeCol != null) return false;
        if (int8rangeCol != null ? !int8rangeCol.equals(that.int8rangeCol) : that.int8rangeCol != null) return false;
        if (numrangeCol != null ? !numrangeCol.equals(that.numrangeCol) : that.numrangeCol != null) return false;
        if (tsrangeCol != null ? !tsrangeCol.equals(that.tsrangeCol) : that.tsrangeCol != null) return false;
        if (tstzrangeCol != null ? !tstzrangeCol.equals(that.tstzrangeCol) : that.tstzrangeCol != null) return false;
        return daterangeCol != null ? daterangeCol.equals(that.daterangeCol) : that.daterangeCol == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (smallintCol != null ? smallintCol.hashCode() : 0);
        result = 31 * result + (integerCol != null ? integerCol.hashCode() : 0);
        result = 31 * result + (bigintCol != null ? bigintCol.hashCode() : 0);
        result = 31 * result + (decimalCol != null ? decimalCol.hashCode() : 0);
        result = 31 * result + (numericCol != null ? numericCol.hashCode() : 0);
        result = 31 * result + (realCol != null ? realCol.hashCode() : 0);
        result = 31 * result + (doublePrecisionCol != null ? doublePrecisionCol.hashCode() : 0);
        result = 31 * result + (charCol != null ? charCol.hashCode() : 0);
        result = 31 * result + (varcharCol != null ? varcharCol.hashCode() : 0);
        result = 31 * result + (textCol != null ? textCol.hashCode() : 0);
        result = 31 * result + (dateCol != null ? dateCol.hashCode() : 0);
        result = 31 * result + (timeCol != null ? timeCol.hashCode() : 0);
        result = 31 * result + (timeWithTzCol != null ? timeWithTzCol.hashCode() : 0);
        result = 31 * result + (timestampCol != null ? timestampCol.hashCode() : 0);
        result = 31 * result + (timestampWithTzCol != null ? timestampWithTzCol.hashCode() : 0);
        result = 31 * result + (intervalCol != null ? intervalCol.hashCode() : 0);
        result = 31 * result + (booleanCol != null ? booleanCol.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(byteaCol);
        result = 31 * result + (inetCol != null ? inetCol.hashCode() : 0);
        result = 31 * result + (cidrCol != null ? cidrCol.hashCode() : 0);
        result = 31 * result + (macaddrCol != null ? macaddrCol.hashCode() : 0);
        result = 31 * result + (jsonCol != null ? jsonCol.hashCode() : 0);
        result = 31 * result + (jsonbCol != null ? jsonbCol.hashCode() : 0);
        result = 31 * result + (uuidCol != null ? uuidCol.hashCode() : 0);
        result = 31 * result + (pointCol != null ? pointCol.hashCode() : 0);
        result = 31 * result + (lineCol != null ? lineCol.hashCode() : 0);
        result = 31 * result + (lsegCol != null ? lsegCol.hashCode() : 0);
        result = 31 * result + (boxCol != null ? boxCol.hashCode() : 0);
        result = 31 * result + (pathCol != null ? pathCol.hashCode() : 0);
        result = 31 * result + (polygonCol != null ? polygonCol.hashCode() : 0);
        result = 31 * result + (circleCol != null ? circleCol.hashCode() : 0);
        result = 31 * result + (int4rangeCol != null ? int4rangeCol.hashCode() : 0);
        result = 31 * result + (int8rangeCol != null ? int8rangeCol.hashCode() : 0);
        result = 31 * result + (numrangeCol != null ? numrangeCol.hashCode() : 0);
        result = 31 * result + (tsrangeCol != null ? tsrangeCol.hashCode() : 0);
        result = 31 * result + (tstzrangeCol != null ? tstzrangeCol.hashCode() : 0);
        result = 31 * result + (daterangeCol != null ? daterangeCol.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        return new StringJoiner(", ", SampleData.class.getSimpleName() + "[", "]")
                .add("\nid=" + id)
                .add("\nsmallintCol=" + smallintCol)
                .add("\nintegerCol=" + integerCol)
                .add("\nbigintCol=" + bigintCol)
                .add("\ndecimalCol=" + decimalCol)
                .add("\nnumericCol=" + numericCol)
                .add("\nrealCol=" + realCol)
                .add("\ndoublePrecisionCol=" + doublePrecisionCol)
                .add("\ncharCol='" + charCol + "'")
                .add("\nvarcharCol='" + varcharCol + "'")
                .add("\ntextCol='" + textCol + "'")
                .add("\ndateCol=" + dateCol)
                .add("\ntimeCol=" + timeCol)
                .add("\ntimeWithTzCol=" + timeWithTzCol)
                .add("\ntimestampCol=" + timestampCol)
                .add("\ntimestampWithTzCol=" + timestampWithTzCol)
                .add("\nintervalCol=" + intervalCol)
                .add("\nbooleanCol=" + booleanCol)
                .add("\nbyteaCol=" + Arrays.toString(byteaCol))
                .add("\ninetCol=" + inetCol)
                .add("\ncidrCol='" + cidrCol + "'")
                .add("\nmacaddrCol='" + macaddrCol + "'")
                .add("\njsonCol='" + jsonCol + "'")
                .add("\njsonbCol='" + jsonbCol + "'")
                .add("\nuuidCol=" + uuidCol)
                .add("\npointCol='" + pointCol + "'")
                .add("\nlineCol='" + lineCol + "'")
                .add("\nlsegCol='" + lsegCol + "'")
                .add("\nboxCol='" + boxCol + "'")
                .add("\npathCol='" + pathCol + "'")
                .add("\npolygonCol='" + polygonCol + "'")
                .add("\ncircleCol='" + circleCol + "'")
                .add("\nint4rangeCol='" + int4rangeCol + "'")
                .add("\nint8rangeCol='" + int8rangeCol + "'")
                .add("\nnumrangeCol='" + numrangeCol + "'")
                .add("\ntsrangeCol='" + tsrangeCol + "'")
                .add("\ntstzrangeCol='" + tstzrangeCol + "'")
                .add("\ndaterangeCol='" + daterangeCol + "'")
                .toString();
    }
}
