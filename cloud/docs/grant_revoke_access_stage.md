## 描述
此文档描述，在存算分离版本用户访问external stage权限相关问题

注意：此文档说的用户名，都是sql的用户名，比如mysql -ujack，其中jack为用户名

### grant stage访问权限给用户

1. 使用mysql client创建一个新用户
2. 语法
```
GRANT USAGE_PRIV ON STAGE {stage_name} TO {user}
```

3. 示例：
```
// 使用root账号在mysql client中创建jack用户
mysql> CREATE USER jack IDENTIFIED BY '123456' DEFAULT ROLE "admin";
Query OK, 0 rows affected (0.01 sec)

mysql> GRANT USAGE_PRIV ON STAGE not_exist_stage TO jack;
Query OK, 0 rows affected (0.01 sec)

mysql> show all grants;
+--------------+----------+-------------------------------+--------------+---------------------------------------------------+------------+---------------+--------------+--------------------------------------+
| UserIdentity | Password | GlobalPrivs                   | CatalogPrivs | DatabasePrivs                                     | TablePrivs | ResourcePrivs | CloudCluster | CloudStage                           |
+--------------+----------+-------------------------------+--------------+---------------------------------------------------+------------+---------------+--------------+--------------------------------------+
| 'jack'@'%'   | Yes      |  (false)                      | NULL         | internal.information_schema: Select_priv  (false) | NULL       | NULL          | NULL         | not_exist_stage: Usage_priv  (false) |
| 'root'@'%'   | No       | Node_priv Admin_priv  (false) | NULL         | NULL                                              | NULL       | NULL          | NULL         | NULL                                 |
| 'admin'@'%'  | No       | Admin_priv  (false)           | NULL         | NULL                                              | NULL       | NULL          | NULL         | NULL                                 |                             |
+--------------+----------+-------------------------------+--------------+---------------------------------------------------+------------+---------------+--------------+--------------------------------------+
3 rows in set (0.00 sec)

```

### revoke 用户访问stage权限

1. 语法
```
REVOKE USAGE_PRIV ON STAGE {stage_name} FROM {user}
```

2. 示例：
```
// 使用root账号在mysql client中创建jack用户
mysql> revoke USAGE_PRIV ON STAGE not_exist_stage FROM jack;
Query OK, 0 rows affected (0.01 sec)

mysql> show all grants;
+--------------+----------+-------------------------------+--------------+---------------------------------------------------+------------+---------------+--------------+------------+
| UserIdentity | Password | GlobalPrivs                   | CatalogPrivs | DatabasePrivs                                     | TablePrivs | ResourcePrivs | CloudCluster | CloudStage |
+--------------+----------+-------------------------------+--------------+---------------------------------------------------+------------+---------------+--------------+------------+
| 'root'@'%'   | No       | Node_priv Admin_priv  (false) | NULL         | NULL                                              | NULL       | NULL          | NULL         | NULL       |
| 'admin'@'%'  | No       | Admin_priv  (false)           | NULL         | NULL                                              | NULL       | NULL          | NULL         | NULL       |
| 'jack'@'%'   | Yes      |  (false)                      | NULL         | internal.information_schema: Select_priv  (false) | NULL       | NULL          | NULL         | NULL       |
+--------------+----------+-------------------------------+--------------+---------------------------------------------------+------------+---------------+--------------+------------+
3 rows in set (0.00 sec)
```