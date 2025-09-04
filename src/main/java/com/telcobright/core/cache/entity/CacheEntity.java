package com.telcobright.core.cache.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Universal cache entity that can represent any cached object.
 * Uses the same field structure as GenericEntity to work seamlessly with GenericEntityStorage.
 * This entity has enough fields to cover most use cases.
 */
public class CacheEntity {
    // Primary identifier
    private Long id;
    
    // String fields (20 fields like GenericEntity)
    private String string1;  // name, title, description
    private String string2;  // type, category, status
    private String string3;  // code, reference, key
    private String string4;  // email, phone, address
    private String string5;  // city, country, region
    private String string6;  // notes, comments
    private String string7;  // url, link
    private String string8;  // metadata
    private String string9;
    private String string10;
    private String string11;
    private String string12;
    private String string13;
    private String string14;
    private String string15;
    private String string16;
    private String string17;
    private String string18;
    private String string19;
    private String string20;
    
    // Long fields (10 fields)
    private Long long1;  // accountId, customerId, orderId
    private Long long2;  // parentId, groupId
    private Long long3;  // version, revision
    private Long long4;  // count, quantity
    private Long long5;
    private Long long6;
    private Long long7;
    private Long long8;
    private Long long9;
    private Long long10;
    
    // Double fields (10 fields)
    private Double double1;  // amount, balance, price
    private Double double2;  // rate, percentage
    private Double double3;  // latitude, longitude
    private Double double4;  // weight, volume
    private Double double5;
    private Double double6;
    private Double double7;
    private Double double8;
    private Double double9;
    private Double double10;
    
    // BigDecimal fields (10 fields)
    private BigDecimal bigDecimal1;  // precise amounts
    private BigDecimal bigDecimal2;
    private BigDecimal bigDecimal3;
    private BigDecimal bigDecimal4;
    private BigDecimal bigDecimal5;
    private BigDecimal bigDecimal6;
    private BigDecimal bigDecimal7;
    private BigDecimal bigDecimal8;
    private BigDecimal bigDecimal9;
    private BigDecimal bigDecimal10;
    
    // Boolean fields (10 fields)
    private Boolean boolean1;  // active, enabled, visible
    private Boolean boolean2;  // verified, approved
    private Boolean boolean3;  // deleted, archived
    private Boolean boolean4;
    private Boolean boolean5;
    private Boolean boolean6;
    private Boolean boolean7;
    private Boolean boolean8;
    private Boolean boolean9;
    private Boolean boolean10;
    
    // DateTime fields (10 fields)
    private LocalDateTime dateTime1;  // createdAt
    private LocalDateTime dateTime2;  // updatedAt
    private LocalDateTime dateTime3;  // deletedAt
    private LocalDateTime dateTime4;  // expiresAt
    private LocalDateTime dateTime5;
    private LocalDateTime dateTime6;
    private LocalDateTime dateTime7;
    private LocalDateTime dateTime8;
    private LocalDateTime dateTime9;
    private LocalDateTime dateTime10;
    
    // Constructors
    public CacheEntity() {}
    
    public CacheEntity(Long id) {
        this.id = id;
    }
    
    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    // String fields
    public String getString1() { return string1; }
    public void setString1(String string1) { this.string1 = string1; }
    
    public String getString2() { return string2; }
    public void setString2(String string2) { this.string2 = string2; }
    
    public String getString3() { return string3; }
    public void setString3(String string3) { this.string3 = string3; }
    
    public String getString4() { return string4; }
    public void setString4(String string4) { this.string4 = string4; }
    
    public String getString5() { return string5; }
    public void setString5(String string5) { this.string5 = string5; }
    
    public String getString6() { return string6; }
    public void setString6(String string6) { this.string6 = string6; }
    
    public String getString7() { return string7; }
    public void setString7(String string7) { this.string7 = string7; }
    
    public String getString8() { return string8; }
    public void setString8(String string8) { this.string8 = string8; }
    
    public String getString9() { return string9; }
    public void setString9(String string9) { this.string9 = string9; }
    
    public String getString10() { return string10; }
    public void setString10(String string10) { this.string10 = string10; }
    
    public String getString11() { return string11; }
    public void setString11(String string11) { this.string11 = string11; }
    
    public String getString12() { return string12; }
    public void setString12(String string12) { this.string12 = string12; }
    
    public String getString13() { return string13; }
    public void setString13(String string13) { this.string13 = string13; }
    
    public String getString14() { return string14; }
    public void setString14(String string14) { this.string14 = string14; }
    
    public String getString15() { return string15; }
    public void setString15(String string15) { this.string15 = string15; }
    
    public String getString16() { return string16; }
    public void setString16(String string16) { this.string16 = string16; }
    
    public String getString17() { return string17; }
    public void setString17(String string17) { this.string17 = string17; }
    
    public String getString18() { return string18; }
    public void setString18(String string18) { this.string18 = string18; }
    
    public String getString19() { return string19; }
    public void setString19(String string19) { this.string19 = string19; }
    
    public String getString20() { return string20; }
    public void setString20(String string20) { this.string20 = string20; }
    
    // Long fields
    public Long getLong1() { return long1; }
    public void setLong1(Long long1) { this.long1 = long1; }
    
    public Long getLong2() { return long2; }
    public void setLong2(Long long2) { this.long2 = long2; }
    
    public Long getLong3() { return long3; }
    public void setLong3(Long long3) { this.long3 = long3; }
    
    public Long getLong4() { return long4; }
    public void setLong4(Long long4) { this.long4 = long4; }
    
    public Long getLong5() { return long5; }
    public void setLong5(Long long5) { this.long5 = long5; }
    
    public Long getLong6() { return long6; }
    public void setLong6(Long long6) { this.long6 = long6; }
    
    public Long getLong7() { return long7; }
    public void setLong7(Long long7) { this.long7 = long7; }
    
    public Long getLong8() { return long8; }
    public void setLong8(Long long8) { this.long8 = long8; }
    
    public Long getLong9() { return long9; }
    public void setLong9(Long long9) { this.long9 = long9; }
    
    public Long getLong10() { return long10; }
    public void setLong10(Long long10) { this.long10 = long10; }
    
    // Double fields
    public Double getDouble1() { return double1; }
    public void setDouble1(Double double1) { this.double1 = double1; }
    
    public Double getDouble2() { return double2; }
    public void setDouble2(Double double2) { this.double2 = double2; }
    
    public Double getDouble3() { return double3; }
    public void setDouble3(Double double3) { this.double3 = double3; }
    
    public Double getDouble4() { return double4; }
    public void setDouble4(Double double4) { this.double4 = double4; }
    
    public Double getDouble5() { return double5; }
    public void setDouble5(Double double5) { this.double5 = double5; }
    
    public Double getDouble6() { return double6; }
    public void setDouble6(Double double6) { this.double6 = double6; }
    
    public Double getDouble7() { return double7; }
    public void setDouble7(Double double7) { this.double7 = double7; }
    
    public Double getDouble8() { return double8; }
    public void setDouble8(Double double8) { this.double8 = double8; }
    
    public Double getDouble9() { return double9; }
    public void setDouble9(Double double9) { this.double9 = double9; }
    
    public Double getDouble10() { return double10; }
    public void setDouble10(Double double10) { this.double10 = double10; }
    
    // BigDecimal fields
    public BigDecimal getBigDecimal1() { return bigDecimal1; }
    public void setBigDecimal1(BigDecimal bigDecimal1) { this.bigDecimal1 = bigDecimal1; }
    
    public BigDecimal getBigDecimal2() { return bigDecimal2; }
    public void setBigDecimal2(BigDecimal bigDecimal2) { this.bigDecimal2 = bigDecimal2; }
    
    public BigDecimal getBigDecimal3() { return bigDecimal3; }
    public void setBigDecimal3(BigDecimal bigDecimal3) { this.bigDecimal3 = bigDecimal3; }
    
    public BigDecimal getBigDecimal4() { return bigDecimal4; }
    public void setBigDecimal4(BigDecimal bigDecimal4) { this.bigDecimal4 = bigDecimal4; }
    
    public BigDecimal getBigDecimal5() { return bigDecimal5; }
    public void setBigDecimal5(BigDecimal bigDecimal5) { this.bigDecimal5 = bigDecimal5; }
    
    public BigDecimal getBigDecimal6() { return bigDecimal6; }
    public void setBigDecimal6(BigDecimal bigDecimal6) { this.bigDecimal6 = bigDecimal6; }
    
    public BigDecimal getBigDecimal7() { return bigDecimal7; }
    public void setBigDecimal7(BigDecimal bigDecimal7) { this.bigDecimal7 = bigDecimal7; }
    
    public BigDecimal getBigDecimal8() { return bigDecimal8; }
    public void setBigDecimal8(BigDecimal bigDecimal8) { this.bigDecimal8 = bigDecimal8; }
    
    public BigDecimal getBigDecimal9() { return bigDecimal9; }
    public void setBigDecimal9(BigDecimal bigDecimal9) { this.bigDecimal9 = bigDecimal9; }
    
    public BigDecimal getBigDecimal10() { return bigDecimal10; }
    public void setBigDecimal10(BigDecimal bigDecimal10) { this.bigDecimal10 = bigDecimal10; }
    
    // Boolean fields
    public Boolean getBoolean1() { return boolean1; }
    public void setBoolean1(Boolean boolean1) { this.boolean1 = boolean1; }
    
    public Boolean getBoolean2() { return boolean2; }
    public void setBoolean2(Boolean boolean2) { this.boolean2 = boolean2; }
    
    public Boolean getBoolean3() { return boolean3; }
    public void setBoolean3(Boolean boolean3) { this.boolean3 = boolean3; }
    
    public Boolean getBoolean4() { return boolean4; }
    public void setBoolean4(Boolean boolean4) { this.boolean4 = boolean4; }
    
    public Boolean getBoolean5() { return boolean5; }
    public void setBoolean5(Boolean boolean5) { this.boolean5 = boolean5; }
    
    public Boolean getBoolean6() { return boolean6; }
    public void setBoolean6(Boolean boolean6) { this.boolean6 = boolean6; }
    
    public Boolean getBoolean7() { return boolean7; }
    public void setBoolean7(Boolean boolean7) { this.boolean7 = boolean7; }
    
    public Boolean getBoolean8() { return boolean8; }
    public void setBoolean8(Boolean boolean8) { this.boolean8 = boolean8; }
    
    public Boolean getBoolean9() { return boolean9; }
    public void setBoolean9(Boolean boolean9) { this.boolean9 = boolean9; }
    
    public Boolean getBoolean10() { return boolean10; }
    public void setBoolean10(Boolean boolean10) { this.boolean10 = boolean10; }
    
    // DateTime fields
    public LocalDateTime getDateTime1() { return dateTime1; }
    public void setDateTime1(LocalDateTime dateTime1) { this.dateTime1 = dateTime1; }
    
    public LocalDateTime getDateTime2() { return dateTime2; }
    public void setDateTime2(LocalDateTime dateTime2) { this.dateTime2 = dateTime2; }
    
    public LocalDateTime getDateTime3() { return dateTime3; }
    public void setDateTime3(LocalDateTime dateTime3) { this.dateTime3 = dateTime3; }
    
    public LocalDateTime getDateTime4() { return dateTime4; }
    public void setDateTime4(LocalDateTime dateTime4) { this.dateTime4 = dateTime4; }
    
    public LocalDateTime getDateTime5() { return dateTime5; }
    public void setDateTime5(LocalDateTime dateTime5) { this.dateTime5 = dateTime5; }
    
    public LocalDateTime getDateTime6() { return dateTime6; }
    public void setDateTime6(LocalDateTime dateTime6) { this.dateTime6 = dateTime6; }
    
    public LocalDateTime getDateTime7() { return dateTime7; }
    public void setDateTime7(LocalDateTime dateTime7) { this.dateTime7 = dateTime7; }
    
    public LocalDateTime getDateTime8() { return dateTime8; }
    public void setDateTime8(LocalDateTime dateTime8) { this.dateTime8 = dateTime8; }
    
    public LocalDateTime getDateTime9() { return dateTime9; }
    public void setDateTime9(LocalDateTime dateTime9) { this.dateTime9 = dateTime9; }
    
    public LocalDateTime getDateTime10() { return dateTime10; }
    public void setDateTime10(LocalDateTime dateTime10) { this.dateTime10 = dateTime10; }
    
    /**
     * Helper method to set a field value by common field name.
     * Maps common field names to the appropriate storage field.
     */
    public void setField(String fieldName, Object value) {
        switch (fieldName.toLowerCase()) {
            case "id":
                setId(value instanceof Long ? (Long) value : Long.valueOf(value.toString()));
                break;
            case "name":
            case "title":
            case "description":
                setString1(value != null ? value.toString() : null);
                break;
            case "type":
            case "category":
            case "status":
                setString2(value != null ? value.toString() : null);
                break;
            case "code":
            case "reference":
            case "key":
                setString3(value != null ? value.toString() : null);
                break;
            case "email":
            case "phone":
            case "address":
                setString4(value != null ? value.toString() : null);
                break;
            case "accountid":
            case "account_id":
            case "customerid":
            case "customer_id":
                setLong1(value instanceof Long ? (Long) value : 
                    value instanceof Number ? ((Number) value).longValue() : 
                    Long.valueOf(value.toString()));
                break;
            case "amount":
            case "balance":
            case "price":
            case "total":
                setDouble1(value instanceof Double ? (Double) value : 
                    value instanceof Number ? ((Number) value).doubleValue() : 
                    Double.valueOf(value.toString()));
                break;
            case "active":
            case "enabled":
            case "visible":
                setBoolean1(value instanceof Boolean ? (Boolean) value : 
                    Boolean.valueOf(value.toString()));
                break;
            // Add more mappings as needed
        }
    }
    
    /**
     * Helper method to get a field value by common field name.
     */
    public Object getField(String fieldName) {
        switch (fieldName.toLowerCase()) {
            case "id": return getId();
            case "name":
            case "title":
            case "description": return getString1();
            case "type":
            case "category":
            case "status": return getString2();
            case "code":
            case "reference":
            case "key": return getString3();
            case "email":
            case "phone":
            case "address": return getString4();
            case "accountid":
            case "account_id":
            case "customerid":
            case "customer_id": return getLong1();
            case "amount":
            case "balance":
            case "price":
            case "total": return getDouble1();
            case "active":
            case "enabled":
            case "visible": return getBoolean1();
            default: return null;
        }
    }
}