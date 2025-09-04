// Simple entity classes for testing

public class SimplePackageAccount {
    private Long id;
    private String name;
    private Double balance;
    
    public SimplePackageAccount() {}
    
    public SimplePackageAccount(Long id, String name, Double balance) {
        this.id = id;
        this.name = name;
        this.balance = balance;
    }
    
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public Double getBalance() { return balance; }
    public void setBalance(Double balance) { this.balance = balance; }
}

public class SimpleReserve {
    private Long id;
    private Long accountId;
    private Double amount;
    
    public SimpleReserve() {}
    
    public SimpleReserve(Long id, Long accountId, Double amount) {
        this.id = id;
        this.accountId = accountId;
        this.amount = amount;
    }
    
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public Long getAccountId() { return accountId; }
    public void setAccountId(Long accountId) { this.accountId = accountId; }
    
    public Double getAmount() { return amount; }
    public void setAmount(Double amount) { this.amount = amount; }
}

public class SimpleCustomer {
    private Long id;
    private String name;
    private String email;
    
    public SimpleCustomer() {}
    
    public SimpleCustomer(Long id, String name, String email) {
        this.id = id;
        this.name = name;
        this.email = email;
    }
    
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
}

public class SimpleOrder {
    private Long id;
    private Long customerId;
    private Double total;
    private String status;
    
    public SimpleOrder() {}
    
    public SimpleOrder(Long id, Long customerId, Double total) {
        this.id = id;
        this.customerId = customerId;
        this.total = total;
    }
    
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public Long getCustomerId() { return customerId; }
    public void setCustomerId(Long customerId) { this.customerId = customerId; }
    
    public Double getTotal() { return total; }
    public void setTotal(Double total) { this.total = total; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}