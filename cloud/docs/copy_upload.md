## 描述

如果用户没有外部的对象存储，可以把数据文件暂存在SelectDB提供的默认对象存储中，称之为internal stage。

不同于[external stage](create_stage.md)：

1. internal stage无需手动创建，在用户第一次使用时会自动创建。名字是固定的，为`~`；

2. 只能当前owner访问，不支持grant权限给其他user；

3. 无法进行list，用户需要自己记住上传了哪些文件；

然后调用导入语句[copy into](copy_into.md)，把文件导入到表中。
 
## 语法

上传文件:

```
curl -u {user}:{password} -H "filename: {file_name_in_storage}" -T {local_file_path} -L '{selectdb_host}:{selectdb_copy_port}/copy/upload'
```

将文件导入到SelectDB的表中（注意：请求body的内容为`json`格式，需要对sql中的部分字符进行转义）：

```
curl -X POST -u {user}:{password} '{selectdb_host}:{selectdb_copy_port}/copy/query'  -H "Content-Type: application/json" -d '{"sql": "{copy_into_sql}"}'
```

`copy_sql`参考：[copy into](copy_into.md)

## 举例
 
1. 用户`bob`(密码为`123456`)把本地文件`data/2022-10-20/1.csv`上传到internal stage中，上传后的文件命名为`2020-10-20/1.csv`:

```
curl -u bob:123456 -H "filename: 2022-10-20/1.csv" -T data/2022-10-20/1.csv -L '172.21.21.12:8035/copy/upload'
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0    14    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0
100    14    0     0  100    14      0     28 --:--:-- --:--:-- --:--:--    28
```

执行导入：

```
curl -X POST -u bob:123456 '172.21.21.12:8035/copy/query'  -H "Content-Type: application/json" -d '{"sql": "copy into db1.t5 from @~(\"2022-10-20/1.csv\") properties(\"file.type\"=\"csv\",\"file.column_separator\"=\",\",\"copy.async\"=\"false\")"}'
```

