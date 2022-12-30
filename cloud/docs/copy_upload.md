## 描述

如果用户没有外部的对象存储，可以把数据文件暂存在SelectDB提供的默认对象存储中，称之为internal stage。

不同于[external stage](create_stage.md)：

1. internal stage无需手动创建，在用户第一次使用时会自动创建。名字是固定的，为`~`；

2. 只能当前owner访问，不支持grant权限给其他user；

3. 无法进行list，用户需要自己记住上传了哪些文件；

然后调用导入语句[copy into](copy_into.md)，把文件导入到表中。
 
## 语法

### 上传文件

语法：

```
curl -u {user}:{password} -H "filename: {file_name_in_storage}" -T {local_file_path} -L '{selectdb_host}:{selectdb_copy_port}/copy/upload'
```

首先会从SelectDB获得一个预签名URL，然后把本地文件上传到这个预签名URL。

如果获取预签名URL出错，输出如下，`code`非`0`表示出错，出错的原因在`msg`和`data`中。下面是权限出错的示例：
```
{
	"msg": "Unauthorized",
	"code": 401,
	"data": "Access denied for user 'root@127.0.0.1' (using password: YES)",
	"count": 0
}
```

上传成功，一般会显示统计信息，或没有输出(配置`-s`):
```
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     6    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0
100     6    0     0  100     6      0      7 --:--:-- --:--:-- --:--:--    16
```

上传失败，可根据输出查看出错原因。

### 执行导入

将文件导入到SelectDB的表中（注意：请求body的内容为`json`格式，需要对sql中的部分字符进行转义）：

```
curl -X POST -u {user}:{password} '{selectdb_host}:{selectdb_copy_port}/copy/query'  -H "Content-Type: application/json" -d '{"sql": "{copy_into_sql}"}'
```

其中，`copy_into_sql`参考：[copy into](copy_into.md)

如果导入成功，输出如下。其中，`code`为`0`表示服务端执行了导入语句，`state`为`FINISHED`表示导入成功，`loadedRows`表示导入的行数：
```
{
	"msg": "success",
	"code": 0,
	"data": {
		"result": {
			"msg": "",
			"loadedRows": "2",
			"id": "2fc862f6759c4e38-b185ce042d564273",
			"state": "FINISHED",
			"type": "",
			"filterRows": "0",
			"unselectRows": "0",
			"url": null
		},
		"time": 2224,
		"type": "result_set"
	},
	"count": 0
}
```

如果导入失败，输出如下。其中，`code`为`0`表示服务端执行了导入语句，`state`为`CANCELLED`表示导入失败，`msg`表示导入出错的原因：
```
{
	"msg": "success",
	"code": 0,
	"data": {
		"result": {
			"msg": "errCode = 2, detailMessage = No available backends, cluster is: c3",
			"loadedRows": "",
			"id": "a2714c73732a4cb6-88675eb141d21508",
			"state": "CANCELLED",
			"type": "ETL_RUN_FAIL",
			"filterRows": "",
			"unselectRows": "",
			"url": null
		},
		"time": 3153,
		"type": "result_set"
	},
	"count": 0
}
```

如果输入的文件有数据质量问题，输出如下。其中，`code`为`0`表示服务端执行了导入语句，`state`为`CANCELLED`表示导入失败，`msg`表示导入失败的原因，`url`中的信息说明了不符合数据质量要求的行及错误原因：
```
{
	"msg": "success",
	"code": 0,
	"data": {
		"result": {
			"msg": "quality not good enough to cancel",
			"loadedRows": "",
			"id": "c14da29aeb40426d-bbb8326a9c6b8f2b",
			"state": "CANCELLED",
			"type": "ETL_QUALITY_UNSATISFIED",
			"filterRows": "",
			"unselectRows": "",
			"url": "http://doris-build-1308700295.cos.ap-beijing.myqcloud.com/meiyi_cloud_test/error_log/5c1e4e6ae36d4fe3-a14da7465d2e8764_5c1e4e6ae36d4fe3_a14da7465d2e8764?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDAE2aqpY0B7oFPIvHMBj01lFSO3RYOxFH%2F20221223%2Fap-beijing%2Fs3%2Faws4_request&X-Amz-Date=20221223T073711Z&X-Amz-Expires=604799&X-Amz-SignedHeaders=host&X-Amz-Signature=d2130b7c277775e6d67e60fb16d54f181c80cda407dc77513501aa5ae410679c"
		},
		"time": 3682,
		"type": "result_set"
	},
	"count": 0
}
```

如果因为其它原因服务端未执行导入，`code`非`0`，出错的原因在`msg`和`data`中。下面是权限问题导致没有导入的示例：
```
{
	"msg": "Unauthorized",
	"code": 401,
	"data": "Access denied for user 'root@127.0.0.1' (using password: YES)",
	"count": 0
}
```

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

