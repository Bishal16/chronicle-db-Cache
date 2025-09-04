package com.telcobright.core.cache;

import com.telcobright.db.genericentity.api.IEntityTypeSet;

/**
 * Entity type definitions for the cache system.
 * Each entity type that can be stored in the cache must have an entry here.
 * This enum enables the GenericEntityStorage to handle multiple entity types
 * in a single unified storage, enabling atomic multi-entity transactions.
 */
public enum CacheEntityTypeSet implements IEntityTypeSet {
    
    // Telecom domain entities
    PACKAGE_ACCOUNT(1),
    PACKAGE_ACCOUNT_RESERVE(2),
    PACKAGE_PURCHASE(3),
    
    // Customer domain entities
    CUSTOMER(10),
    CUSTOMER_SUBSCRIPTION(11),
    CUSTOMER_PROFILE(12),
    
    // Order and transaction entities
    ORDER(20),
    ORDER_ITEM(21),
    TRANSACTION_LOG(22),
    PAYMENT(23),
    
    // Product and inventory entities
    PRODUCT(30),
    INVENTORY(31),
    PRICE_PLAN(32),
    
    // Session and authentication entities
    USER_SESSION(40),
    AUTH_TOKEN(41),
    
    // Billing entities
    INVOICE(50),
    BILLING_CYCLE(51),
    
    // Generic entities for dynamic usage
    GENERIC_ENTITY(100);
    
    private final int typeId;
    
    CacheEntityTypeSet(int typeId) {
        this.typeId = typeId;
    }
    
    @Override
    public int getTypeId() {
        return typeId;
    }
    
    /**
     * Get entity type by table name.
     * This is useful for converting WAL entries to entity types.
     * 
     * @param tableName The database table name
     * @return The corresponding entity type, or GENERIC_ENTITY if not found
     */
    public static CacheEntityTypeSet fromTableName(String tableName) {
        if (tableName == null) {
            return GENERIC_ENTITY;
        }
        
        // Convert table_name to ENUM_NAME format
        String enumName = tableName.toUpperCase().replace("-", "_");
        
        try {
            return CacheEntityTypeSet.valueOf(enumName);
        } catch (IllegalArgumentException e) {
            // If exact match not found, try some common mappings
            switch (tableName.toLowerCase()) {
                case "package_account":
                    return PACKAGE_ACCOUNT;
                case "package_account_reserve":
                    return PACKAGE_ACCOUNT_RESERVE;
                case "customer":
                case "customers":
                    return CUSTOMER;
                case "order":
                case "orders":
                    return ORDER;
                case "product":
                case "products":
                    return PRODUCT;
                case "transaction_log":
                case "transactions":
                    return TRANSACTION_LOG;
                case "user_session":
                case "session":
                case "sessions":
                    return USER_SESSION;
                default:
                    return GENERIC_ENTITY;
            }
        }
    }
    
    /**
     * Get the database table name for this entity type.
     * 
     * @return The table name in lowercase with underscores
     */
    public String getTableName() {
        return this.name().toLowerCase();
    }
}