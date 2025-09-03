package com.telcobright.examples.oltp.mapper;

import com.telcobright.examples.oltp.entity.PackageAccount;
import com.telcobright.examples.oltp.entity.PackageAccountReserve;
import com.telcobright.examples.oltp.entity.PackageAccDelta;
import com.telcobright.examples.oltp.entity.PackageAccountReserveDelta;
import com.telcobright.examples.oltp.grpc.batch.*;

import java.time.format.DateTimeFormatter;

/**
 * Utility class for converting entity objects to proto messages and vice versa
 */
public class EntityProtoMapper {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    
    // ==================== ENTITY TO PROTO CONVERTERS ====================
    
    /**
     * Convert PackageAccount entity to proto message
     */
    public static com.telcobright.examples.oltp.grpc.batch.PackageAccount toProto(PackageAccount entity) {
        var builder = com.telcobright.examples.oltp.grpc.batch.PackageAccount.newBuilder()
            .setId(entity.getId() != null ? entity.getId() : 0L)
            .setPackagePurchaseId(entity.getPackagePurchaseId() != null ? entity.getPackagePurchaseId() : 0L)
            .setName(entity.getName() != null ? entity.getName() : "")
            .setUom(entity.getUom() != null ? entity.getUom() : "")
            .setIsSelected(entity.getIsSelected() != null ? entity.getIsSelected() : false);
        
        if (entity.getLastAmount() != null) {
            builder.setLastAmount(entity.getLastAmount().toString());
        }
        if (entity.getBalanceBefore() != null) {
            builder.setBalanceBefore(entity.getBalanceBefore().toString());
        }
        if (entity.getBalanceAfter() != null) {
            builder.setBalanceAfter(entity.getBalanceAfter().toString());
        }
        
        return builder.build();
    }
    
    /**
     * Convert PackageAccountReserve entity to proto message
     */
    public static com.telcobright.examples.oltp.grpc.batch.PackageAccountReserve toProto(PackageAccountReserve entity) {
        var builder = com.telcobright.examples.oltp.grpc.batch.PackageAccountReserve.newBuilder()
            .setId(entity.getId() != null ? entity.getId() : 0L)
            .setPackageAccountId(entity.getPackageAccountId() != null ? entity.getPackageAccountId() : 0L)
            .setSessionId(entity.getSessionId() != null ? entity.getSessionId() : "")
            .setStatus(entity.getStatus() != null ? entity.getStatus() : "");
        
        if (entity.getReservedAmount() != null) {
            builder.setReservedAmount(entity.getReservedAmount().toString());
        }
        if (entity.getCurrentReserve() != null) {
            builder.setCurrentReserve(entity.getCurrentReserve().toString());
        }
        if (entity.getReservedAt() != null) {
            builder.setReservedAt(entity.getReservedAt().format(DATE_TIME_FORMATTER));
        }
        if (entity.getReleasedAt() != null) {
            builder.setReleasedAt(entity.getReleasedAt().format(DATE_TIME_FORMATTER));
        }
        
        return builder.build();
    }
    
    /**
     * Convert PackageAccDelta entity to proto message
     */
    public static PackageAccountDelta toProto(PackageAccDelta entity) {
        var builder = PackageAccountDelta.newBuilder()
            .setDbName(entity.getDbName() != null ? entity.getDbName() : "")
            .setAccountId(entity.getAccountId() != null ? entity.getAccountId().intValue() : 0);
        
        if (entity.getAmount() != null) {
            builder.setAmount(entity.getAmount().toString());
        }
        
        return builder.build();
    }
    
    /**
     * Convert PackageAccountReserveDelta entity to proto message
     */
    public static com.telcobright.examples.oltp.grpc.batch.PackageAccountReserveDelta toProto(PackageAccountReserveDelta entity) {
        var builder = com.telcobright.examples.oltp.grpc.batch.PackageAccountReserveDelta.newBuilder()
            .setDbName(entity.getDbName() != null ? entity.getDbName() : "")
            .setReserveId((int) entity.reserveId)
            .setSessionId(entity.getSessionId() != null ? entity.getSessionId() : "");
        
        if (entity.getAmount() != null) {
            builder.setAmount(entity.getAmount().toString());
        }
        
        return builder.build();
    }
    
    // ==================== PROTO TO ENTITY CONVERTERS ====================
    
    /**
     * Convert proto PackageAccount to entity
     */
    public static PackageAccount fromProto(com.telcobright.examples.oltp.grpc.batch.PackageAccount proto) {
        PackageAccount entity = new PackageAccount();
        entity.setId(proto.getId());
        entity.setPackagePurchaseId(proto.getPackagePurchaseId());
        entity.setName(proto.getName());
        entity.setUom(proto.getUom());
        entity.setIsSelected(proto.getIsSelected());
        
        if (!proto.getLastAmount().isEmpty()) {
            entity.setLastAmount(new java.math.BigDecimal(proto.getLastAmount()));
        }
        if (!proto.getBalanceBefore().isEmpty()) {
            entity.setBalanceBefore(new java.math.BigDecimal(proto.getBalanceBefore()));
        }
        if (!proto.getBalanceAfter().isEmpty()) {
            entity.setBalanceAfter(new java.math.BigDecimal(proto.getBalanceAfter()));
        }
        
        return entity;
    }
    
    /**
     * Convert proto PackageAccountReserve to entity
     */
    public static PackageAccountReserve fromProto(com.telcobright.examples.oltp.grpc.batch.PackageAccountReserve proto) {
        PackageAccountReserve entity = new PackageAccountReserve();
        entity.setId(proto.getId());
        entity.setPackageAccountId(proto.getPackageAccountId());
        entity.setSessionId(proto.getSessionId());
        entity.setStatus(proto.getStatus());
        
        if (!proto.getReservedAmount().isEmpty()) {
            entity.setReservedAmount(new java.math.BigDecimal(proto.getReservedAmount()));
        }
        if (!proto.getCurrentReserve().isEmpty()) {
            entity.setCurrentReserve(new java.math.BigDecimal(proto.getCurrentReserve()));
        }
        if (!proto.getReservedAt().isEmpty()) {
            entity.setReservedAt(java.time.LocalDateTime.parse(proto.getReservedAt(), DATE_TIME_FORMATTER));
        }
        if (!proto.getReleasedAt().isEmpty()) {
            entity.setReleasedAt(java.time.LocalDateTime.parse(proto.getReleasedAt(), DATE_TIME_FORMATTER));
        }
        
        return entity;
    }
}