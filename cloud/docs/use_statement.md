## 描述
在存算分离版本中，指定使用的数据库和计算集群
 
## 语法
 
```
USE { [catalog_name.]database_name[@cluster_name] | @cluster_name }
```
Note: 如果database名字或者cluster名字是保留的关键字, 需要用backtick
```
` `
```
括起来
 
## 举例
 
1. 指定使用该数据库test_database
```
USE test_database
或者
USE `test_database`
```

2. 指定使用该计算集群test_cluster

```
USE @test_cluster
或者
USE @`test_cluster`
```

3. 同时指定使用该数据库test_database和计算集群test_cluster

```
USE test_database@test_cluster
USE `test_database`@`test_cluster`
```

