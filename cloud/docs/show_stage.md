## 描述

展示登录用户有权限访问的全部external stage信息。

相关文档：[create stage](create_stage.md), [drop stage](drop_stage.md)
 
## 语法

```
SHOW STAGES
```

展示出stage的`name`,`id`,`endpoint`,`region`,`bucket`,`prefix`,`ak`,`sk`和默认的参数`defaultProperties`

## 举例

```
mysql> SHOW STAGES;
+----------------------------+--------------------------------------+-----------------------------+------------+------------------------+---------------------+--------------------------------------+----------------------------------+----------+-----------------------------------------------------------------+
| StageName                  | StageId                              | Endpoint                    | Region     | Bucket                 | Prefix              | AK                                   | SK                               | Provider | DefaultProperties                                               |
+----------------------------+--------------------------------------+-----------------------------+------------+------------------------+---------------------+--------------------------------------+----------------------------------+----------+-----------------------------------------------------------------+
| regression_test_copy_stage | e8ed6ea0-33c8-4381-b7a9-c19ea1801bca | cos.ap-beijing.myqcloud.com | ap-beijing | doris-build-1308700295 | regression/tpch/sf1 | AKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX | SKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX | COS      | {"default.file.column_separator":"|"}                           |
| root_stage                 | 8b8329de-be1a-40a8-9eab-91d31f9798bf | cos.ap-beijing.myqcloud.com | ap-beijing | justtmp-bj-1308700295  |                     | AKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX | SKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX | COS      | {"default.file.type":"CSV","default.file.column_separator":","} |
| admin_stage                | 9284a9ec-3ba7-47b9-b276-1ccde875469c | cos.ap-beijing.myqcloud.com | ap-beijing | justtmp-bj-1308700295  | meiyi_cloud_test    | AKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX | SKXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX | COS      | {"default.file.column_separator":",","default.file.type":"CSV"} |
+----------------------------+--------------------------------------+-----------------------------+------------+------------------------+---------------------+--------------------------------------+----------------------------------+----------+-----------------------------------------------------------------+
3 rows in set (0.15 sec)
```

