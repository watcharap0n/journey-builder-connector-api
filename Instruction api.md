| Transform | พฤติกรรม | ตัวอย่าง |
|---|---|---|
| `trim` | แปลงเป็น string แล้วตัดช่องว่างหัวท้าย | `"  Alice "` → `"Alice"` |
| `lowercase` | แปลงเป็น string ตัวพิมพ์เล็ก | `"TEST@MAIL.COM"` → `"test@mail.com"` |
| `uppercase` | แปลงเป็น stringตัวพิมพ์ใหญ่ | `"th"` → `"TH"` |
| `stringify` | แปลงค่าเป็น string | `12345` → `"12345"` |
| `parse_date` | อ่านวันที่รูปแบบ ISO | `"2026-08-31"` → date |
| `parse_timestamp` | อ่านวันเวลารูปแบบ ISO | `"2026-08-31T10:30:00+07:00"` → datetime |
| Transform | พฤติกรรม | ตัวอย่าง |
|---|---|---|
| `trim` | แปลงเป็น string แล้วตัดช่องว่างหัวท้าย | `"  Alice "` → `"Alice"` |
| `lowercase` | แปลงเป็น string ตัวพิมพ์เล็ก | `"TEST@MAIL.COM"` → `"test@mail.com"` |
| `uppercase` | แปลงเป็น stringตัวพิมพ์ใหญ่ | `"th"` → `"TH"` |
| `stringify` | แปลงค่าเป็น string | `12345` → `"12345"` |
| `parse_date` | อ่านวันที่รูปแบบ ISO | `"2026-08-31"` → date |
| `parse_timestamp` | อ่านวันเวลารูปแบบ ISO | `"2026-08-31T10:30:00+07:00"` → datetime |


Example PostgreSQL connection configuration:
```json
{
  "engine": "postgresql",
  "name": "PostgreSQL Customer POC",
  "source_code": "postgres_customer_poc",
  "endpoint_label": "PostgreSQL Customer Database",
  "safe_config": {},
  "credentials": {
    "host": "postgres.example.internal",
    "port": 5432,
    "username": "existing_user",
    "password": "replace-with-password",
    "database": "postgres",
    "options": {
      "sslmode": "require"
    }
  }
}
```

Example MySQL connection configuration:
```json
{
  "engine": "mysql",
  "name": "MySQL Customer POC",
  "source_code": "mysql_customer_poc",
  "endpoint_label": "MySQL Customer Database",
  "safe_config": {},
  "credentials": {
    "host": "mysql.example.internal",
    "port": 3306,
    "username": "existing_user",
    "password": "replace-with-password",
    "database": "customer_db",
    "options": {}
  }
}
```

Example MariaDB connection configuration:
```json
{
  "engine": "mariadb",
  "name": "MariaDB Customer POC",
  "source_code": "mariadb_customer_poc",
  "endpoint_label": "MariaDB Customer Database",
  "safe_config": {},
  "credentials": {
    "host": "mariadb.example.internal",
    "port": 3306,
    "username": "existing_user",
    "password": "replace-with-password",
    "database": "customer_db",
    "options": {}
  }
}
```

Example MongoDB connection configuration:
```json
{
  "engine": "mongodb",
  "name": "MongoDB Customer POC",
  "source_code": "mongodb_customer_poc",
  "endpoint_label": "MongoDB Customer Database",
  "safe_config": {},
  "credentials": {
    "host": "mongo.example.internal",
    "port": 27017,
    "username": "existing_user",
    "password": "replace-with-password",
    "database": "customer_db",
    "options": {
      "authSource": "admin",
      "tls": false
    }
  }
}
```

Configure Tunneling for PostgreSQL, MySQL, and MariaDB connections:
```json
{
  "engine": "postgresql",
  "name": "PostgreSQL via SSH",
  "source_code": "postgres_ssh_poc",
  "endpoint_label": "Customer DB through bastion",
  "safe_config": {},
  "credentials": {
    "host": "private-postgres.internal",
    "port": 5432,
    "username": "database_user",
    "password": "database_password",
    "database": "postgres",
    "options": {
      "sslmode": "require"
    },
    "tunnel": {
      "type": "ssh",
      "host": "43.210.220.193",
      "port": 22,
      "username": "ubuntu",
      "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\n...\n-----END OPENSSH PRIVATE KEY-----", # password 
      "host_key": "optional-known-host-key" 
    }
  }
}
```
