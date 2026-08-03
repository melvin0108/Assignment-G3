### 1. Table: customers                                                                                                                                                  
                                                                                                                                                                           
  Defined in silver_customers.py and registered in silver_masking_policies.py:                                                                                             
                                                                                                                                                                           
   Column Name                     │ Classification                  │ Protection / Masking Method     │ Code Implementation Details
  ─────────────────────────────────┼─────────────────────────────────┼─────────────────────────────────┼───────────────────────────────────────────────────────────────────
   first_name                      │ Direct ID                       │ Tokenize (FPE / Salted SHA256)  │ Tokenized with prefix: TOK_<SHA256(first_name + salt)> (16 chars)
   last_name                       │ Direct ID                       │ Tokenize (FPE / Salted SHA256)  │ Tokenized with prefix: TOK_<SHA256(last_name + salt)> (16 chars)
   email                           │ Contact                         │ Partial Masking                 │ Shows first char + masked domain (j***@***.com)
   phone                           │ Contact                         │ Partial Masking                 │ Masks all but last 4 digits (******1234)
   address                         │ Sensitive                       │ Hash (SHA256)                   │ Full salted SHA256 hash (SHA256(address + salt))
   dob                             │ Sensitive                       │ Generalization (Age Band)       │ Converted to age brackets (Under 18, 18-25, 26-35, ..., 66+)
   tax_id                          │ Sensitive                       │ Hash (SHA256)                   │ Full salted SHA256 hash (SHA256(tax_id + salt))
  ──────                                                                                                                                                                   
  ### 2. Table: cards                                                                                                                                                      
                                                                                                                                                                           
  Defined in silver_cards.py and registered in silver_masking_policies.py:                                                                                                 
                                                                                                                                                                           
   Column Name                 │ Classification              │ Protection / Masking Method │ Code Implementation Details
  ─────────────────────────────┼─────────────────────────────┼─────────────────────────────┼───────────────────────────────────────────────────────────────────────────────
   pan                         │ Payment                     │ Partial Masking             │ Masks credit card number, preserving only last 4 digits (XXXX-XXXX-XXXX-1234)
  ──────                                                                                                                                                                   
  ### 3. Table: employees                                                                                                                                                  
                                                                                                                                                                           
  Defined in silver_employees.py and registered in silver_masking_policies.py:                                                                                             
                                                                                                                                                                           
   Column Name                          │ Classification                       │ Protection / Masking Method          │ Code Implementation Details
  ──────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────┼────────────────────────────────────────────────────
   full_name                            │ Staff                                │ Hash (SHA256)                        │ Full salted SHA256 hash (SHA256(full_name + salt))
   email                                │ Staff                                │ Hash (SHA256)                        │ Full salted SHA256 hash (SHA256(email + salt))
  ──────                                                                                                                                                                   
  ### 4. Table: transaction_devices                                                                                                                                        
                                                                                                                                                                           
  Defined and registered in silver_transaction_devices.py:                                                                                                                 
                                                                                                                                                                           
   Column Name             │ Classification          │ Protection / Masking Method │ Code Implementation Details
  ─────────────────────────┼─────────────────────────┼─────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────
   device_id               │ Device Identifier       │ Tokenize with salted SHA256 │ Tokenized with prefix: DEV_<SHA256(device_id + salt)> (16 chars)
   ip                      │ Network Identifier      │ Subnet Redaction / Hashing  │ Truncates IPv4 to /24 subnet (xxx.xxx.xxx.0/24) or hashes non-IPv4 (IP_HASH_<SHA256>)