
## 接口目录

## 创建instance

### 接口描述

本接口用于创建一个instance. 这个instance不包含任何节点信息

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/create_instance?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": "123456",
    "name": "name",
    "user_id": "abcdef",
    "obj_info": {
        "ak": "test-ak1",
        "sk": "test-sk1",
        "bucket": "test-bucket",
        "prefix": "test-prefix",
        "endpoint": "test-endpoint",
        "region": "test-region"
    }
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
instance_id | instance_id | 是 | 全局唯一(包括历史上)
name | instance 别名 | 否 |
...

* 请求示例

```
PUT /MetaService/http/create_instance?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": "123456",
    "name": "name",
    "user_id": "abcdef",
    "obj_info": {
        "ak": "test-ak1",
        "sk": "test-sk1",
        "bucket": "test-bucket",
        "prefix": "test-prefix",
        "endpoint": "test-endpoint",
        "region": "test-region"
    }
}
```



## 创建cluster

...
