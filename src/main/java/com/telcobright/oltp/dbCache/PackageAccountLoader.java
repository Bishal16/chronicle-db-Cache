//package com.telcobright.oltp.dbCache;
//
//import com.telcobright.oltp.entity.PackageAccount;
//import jakarta.enterprise.context.ApplicationScoped;
//import jakarta.inject.Inject;
//import jakarta.transaction.Transactional;
//import jakarta.persistence.EntityManager;
//import jakarta.persistence.TypedQuery;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//@ApplicationScoped
//public class PackageAccountLoader {
//
//    @Inject
//    EntityManager entityManager;
//
//    @Transactional
//    public Map<Long, PackageAccount> loadPackageIdWiseAccountMap() {
//        Map<Long, PackageAccount> packageAccountMap = new HashMap<>();
//
//        TypedQuery<PackageAccount> query = entityManager.createQuery(
//                "SELECT pa FROM PackageAccount pa", PackageAccount.class
//        );
//
//        List<PackageAccount> accounts = query.getResultList();
//
//        for (PackageAccount account : accounts) {
//            packageAccountMap.put(account.getId(), account);
//        }
//
//        return packageAccountMap;
//    }
//}
