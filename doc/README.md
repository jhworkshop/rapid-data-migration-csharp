## 配置说明
本项目默认采用文件配置方式，典型的配置文件类似这样：

```
{
    "instances": [
        {
            "source": {
                "dbms": "MSSQL",
                "server": "127.0.0.1",
                "port": 1433,
                "user": "sa",
                "password": "",
                "charset": ""
            },
            "dest": {
                "dbms": "MySQL",
                "server": "127.0.0.1",
                "port": 3306,
                "user": "thomas",
                "password": "",
                "charset": "utf8",
                "compress": 1
            },
            "dbs": [
                "db1"
            ],
            "readPages": 25,
            "tables": "Table-MSSQL2MySQL-Migration.json",
            "threads": 4
        }
    ],
    "mode": "Once",
    "runner": "Migration",
    "runtime": "15:00",
    "threads": 2
}
```

### 模式配置
配置文件的第一层格式是固定的。

```
{
    "instances": [],       # 实例，不同实例的配置格式可能不同，实例之间是并发执行的，并发数由 threads 控制
    "mode": "Once",        # 执行模式，可以是 Once, Daily, 或者 Continuous
    "runner": "Migration", # 执行器，可以是 Migration，或者 Integration
    "runtime": "15:00",    # 执行时间或间隔，格式 hh:mm:ss，Daily 模式下为时间，Continuous 模式下为间隔
    "threads": 2           # 实例并行数
}
```

### 迁移实例配置

```
{
    "source": {                # 源库配置
        "dbms": "MSSQL",       # 数据库类型
        "server": "127.0.0.1", # 服务器地址
        "port": 1433,          # 端口号
        "user": "sa",          # 登录用户名
        "password": "",        # 登录密码
        "charset": "",         # 字符集
        "compress": 0,         # 压缩传输，默认 0
        "encrypt": 0           # 加密传输，默认 0
    },
    "dest": {                  # 目标库配置
        "dbms": "MySQL",       # 数据库类型
        "server": "127.0.0.1", # 服务器地址
        "port": 3306,          # 端口号
        "user": "thomas",      # 登录用户名
        "password": "",        # 登录密码
        "charset": "utf8",     # 字符集
        "compress": 1，        # 压缩传输，默认 0
        "encrypt": 0           # 加密传输，默认 0
    },
    "dbs": [                   # 数据库列表，源库和目标库名称不同，可用逗号分隔，如 db1,db11
        "db1"
    ],
    "readPages": 25,           # 每次读取数据页数
    "tables": "Tables.json",   # 数据表配置文件名
    "threads": 4               # 数据表并行数
}
```

### 汇集实例配置

```
{
    "sources": [                   # 源库配置列表
        {
            "dbms": "MSSQL",       # 数据库类型
            "server": "127.0.0.1", # 服务器地址
            "port": 1433,          # 端口号
            "dbs": [               # 源库名称列表
                "db3"
            ],
            "user": "sa",          # 登录用户名
            "password": "",        # 登录密码
            "charset": "",         # 字符集
            "compress": 0,         # 压缩传输，默认 0
            "encrypt": 0           # 加密传输，默认 0
        }
    ],
    "dest": {                      # 目标库配置
        "dbms": "MySQL",           # 数据库类型
        "server": "127.0.0.1",     # 服务器地址
        "port": 3306,              # 端口号
        "db": "db2",               # 目标库名称
        "user": "thomas",          # 登录用户名
        "password": "",            # 登录密码
        "charset": "utf8",         # 字符集
        "compress": 1，            # 压缩传输，默认 0
        "encrypt": 0               # 加密传输，默认 0
    },
    "readPages": 25,               # 每次读取数据页数
    "tables": "Tables.json",       # 数据表配置文件名
    "threads": 4                   # 数据表并行数
}
```

### 数据表配置

```
{
    "params": "SELECT DATE_FORMAT(NOW(), \"%Y-%m-%d\") AS \"AccDate\"",   # 参数值获取脚本
    "tables": [                                                           # 数据表配置列表
        {
            "name": "Folios",                                             # 表名，源表和目标表名不同，可用逗号分隔，如 t1,t2
            "order": 100,                                                 # 执行次序，A-Z 排序，相同次序的可并行处理
            "orderSQL": "FolioID ASC",                                    # 取数排序脚本，分页取数，要求稳定的排序逻辑
            "whereSQL": "FolioState IN (2, 3, 5) AND AccDate < @AccDate", # 取数条件，可带参数
            "pageSize": 1000,                                             # 取数每页记录数
            "mode": "Append",                                             # 数据模式，可以是 Append，或者 Update
            "keyFields": "FolioID",                                       # 关键字段，多个字段之间用逗号分隔
            "skipFields": "",                                             # 忽略字段，多个字段之间用逗号分隔
            "filter": "",                                                 # 数据过滤器名称
            "dest.mssql.keepIdentity": 1,                                 # 保留自增字段值，MSSQL 批量复制专用
            "references": ""                                              # 外键引用表，多个表名之间用逗号分隔
        }
    ]
}
```
