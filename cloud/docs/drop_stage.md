## 描述

删除external stage，用户需要有stage的ADMIN权限

相关文档:[show stage](show_stage.md), [create stage](create_stage.md)

## 语法

```
DROP STAGE [IF EXISTS] <stage_name>
```

## 举例
 
1. 删除名为`test_stage`的stage:
```
DROP STAGE test_stage
```

