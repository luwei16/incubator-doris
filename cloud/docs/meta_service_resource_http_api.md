
## 接口目录

## 创建instance

### 接口描述

本接口用于创建一个instance. 这个instance不包含任何节点信息，不能多次创建同一个instance_id的instance

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/create_instance?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": string,
    "name": string,
    "user_id": string,
    "obj_info": {
        "ak": string,
        "sk": string,
        "bucket": string,
        "prefix": string,
        "endpoint": string,
        "region": string
    }
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
instance_id | instance_id | 是 | 全局唯一(包括历史上)
name | instance 别名 | 否 |
user_id | 用户id | 是 |
obj_info |  S3链接配置信息 | 是
obj_info.ak | S3的access key | 是
obj_info.sk | S3的secret key | 是
obj_info.bucket | S3的bucket名 | 是
obj_info.prefix | S3上数据存放位置前缀 | 否 | 不填的话，在bucket的根目录
obj_info.endpoint | S3的enpoint信息 | 是
obj_info.region | S3的region信息 | 是


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
* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串


* 成功返回示例

```
{
 "code": "OK",
 "msg": ""
}

```

* 失败返回示例
```
{
 "code": "ALREADY_EXISTED",
 "msg": "instance already existed, instance_id=instance_id_deadbeef"
}
```


## 创建cluster

### 接口描述

本接口用于创建一个属于instance的cluster. 这个cluster中包含若干（大于等于0个）相同类型节点信息, 此接口不能用同一参数调用

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/add_cluster?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": string,
    "cluster": object {
        "cluster_name": string,
        "cluster_id": string,
        "type": enum,
        "nodes": [
            {
                "cloud_unique_id": string,
                "ip": string,
                "heartbeat_port": int
            }
        ]
    }
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
instance_id | instance_id | 是 | 全局唯一(包括历史上)
cluster | cluster对象信息 | 是 |
cluster.cluster_name | cluster的名字 | 是 | 其中fe的cluster名字特殊，默认RESERVED_CLUSTER_NAME_FOR_SQL_SERVER，可在fe.conf中配置cloud_observer_cluster_name修改
cluster.cluster_id |  cluster的id | 是 | 其中fe的cluster id特殊，默认RESERVED_CLUSTER_ID_FOR_SQL_SERVER，可在fe.conf中配置cloud_observer_cluster_id修改
cluster.type | cluster中节点的类型 | 是 | 支持："SQL","COMPUTE"两种type，"SQL"表示sql service对应fe， "COMPUTE"表示计算机节点对应be
cluster.nodes | cluster中的节点数组 | 是
cluster.nodes.cloud_unique_id | 节点的cloud_unique_id | 是 | 是fe.conf、be.conf中的cloud_unique_id配置项
cluster.nodes.ip | 节点的ip | 是 | 
cluster.nodes.heartbeat_port | be的heartbeat port | 是 | 是be.conf中的heartbeat_service_port配置项
cluster.nodes.edit_log_port | fe节点的edit log port | 是 | 是fe.conf中的edit_log_port配置项
cluster.nodes.node_type | fe节点的类型|是| 当cluster的type为SQL时，需要填写，分为"FE_MASTER" 和 "FE_OBSERVER", 其中"FE_MASTER" 表示此节点为master， "FE_OBSERVER"表示此节点为observer，注意：一个type为"SQL"的cluster的nodes数组中只能有一个"FE_MASTER"节点，和若干"FE_OBSERVER"节点

* 请求示例

```
PUT /MetaService/http/add_cluster?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": "123456",
    "cluster": {
        "cluster_name": "cluster_name1",
        "cluster_id": "cluster_id1",
        "type": "COMPUTE",
        "nodes": [
            {
                "cloud_unique_id": "cloud_unique_id_compute_node1",
                "ip": "172.21.0.5",
                "heartbeat_port": 9050
            }
        ]
    }
}
```

* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串


* 成功返回示例

```
{
 "code": "OK",
 "msg": ""
}

```

* 失败返回示例
```
{
 "code": "INTERANAL_ERROR",
 "msg": "cluster is SQL type, must have only one master node, now master count: 0"
}
```

## 获取cluster

### 接口描述

本接口用于获取一个cluster的信息，此接口可以多次重复调用

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/get_cluster?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id":string,
    "cloud_unique_id":string,
    "cluster_name":string,
    "cluster_id":string
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
instance_id | instance_id | 是 |
cluster_name | cluster的名字 | 否 | 注：cluster_name、cluster_id、mysql_user_name三选一
cluster_id |  cluster的id | 否 | 注：cluster_name、cluster_id、mysql_user_name三选一
mysql_user_name | mysql用户名配置的可用cluster| 否 |注：cluster_name、cluster_id、mysql_user_name三选一


* 请求示例

```
PUT /MetaService/http/get_cluster?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id":"regression_instance0",
    "cloud_unique_id":"regression-cloud-unique-id-fe-1",
    "cluster_name":"RESERVED_CLUSTER_NAME_FOR_SQL_SERVER",
    "cluster_id":"RESERVED_CLUSTER_ID_FOR_SQL_SERVER"
}
```

* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串
result | 查询结果对象 | 是 |


* 成功返回示例

```
{
    "code": "OK",
    "msg": "",
    "result": {
        "cluster_id": "cluster_id1",
        "cluster_name": "cluster_name1",
        "type": "COMPUTE",
        "nodes": [
            {
                "cloud_unique_id": "cloud_unique_id_compute_node0",
                "ip": "172.21.16.42",
                "ctime": "1662695469",
                "mtime": "1662695469",
                "heartbeat_port": 9050
            }
        ]
    }
}

```

* 失败返回示例
```
{
 "code": "NOT_FOUND",
 "msg": "fail to get cluster with instance_id: \"instance_id_deadbeef\" cloud_unique_id: \"dengxin_cloud_unique_id_compute_node0\" cluster_name: \"cluster_name\" "
}
```


## 删除cluster

### 接口描述

本接口用于删除一个instance下的某个cluster信息， 多次用相同参数删除失败报错

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/drop_cluster?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id":string,
    "cluster": {
        "cluster_name": string,
        "cluster_id": string,
    }
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
instance_id | instance_id | 是 |
cluster | cluster对象 | 是 | 
cluster.cluster_name |  将删除的cluster name | 是 |
cluster.cluster_id | 将删除的cluster id| 是 |


* 请求示例

```
PUT /MetaService/http/drop_cluster?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id":"regression_instance0",
    "cluster": {
        "cluster_name": "cluster_name1",
        "cluster_id": "cluster_id1",
    }
}
```

* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串


* 成功返回示例

```
{
 "code": "OK",
 "msg": ""
}

```

* 失败返回示例
```
{
 "code": "INTERANAL_ERROR",
 "msg": "failed to find cluster to drop, instance_id=dx_dnstance_id_deadbeef cluster_id=11111 cluster_name=2222"
}
```


## cluster改名

### 接口描述

本接口用于将instance下的某cluster改名，依据传入的cluster_id寻找cluster_name去rename，此接口多次相同参数调用报错

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/rename_cluster?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id":string,
    "cluster": {
        "cluster_name": string,
        "cluster_id": string,
    }
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
instance_id | instance_id | 是 |
cluster | cluster对象 | 是 | 
cluster.cluster_name |  将改名的cluster name | 是 | 新的cluster_name
cluster.cluster_id | 将改名的cluster id | 是 | 依据此id去寻找cluster，然后rename cluster_name


* 请求示例

```
PUT /MetaService/http/rename_cluster?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id":"regression_instance0",
    "cluster": {
        "cluster_name": "cluster_name2",
        "cluster_id": "cluster_id1",
    }
}
```

* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串


* 成功返回示例

```
{
 "code": "OK",
 "msg": ""
}

```

* 失败返回示例
```
{
 "code": "INTERANAL_ERROR",
 "msg": "failed to rename cluster, name eq original name, original cluster is {\"cluster_id\":\"3333333\",\"cluster_name\":\"444444\",\"type\":\"COMPUTE\"}"
}
```

## cluster添加节点

### 接口描述

本接口用于将instance下的某cluster添加若干相同类型的节点，此接口多次相同参数调用报错


### 请求(Request)

* 请求语法

```
PUT /MetaService/http/add_node?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": string,
    "cluster": {
        "cluster_name": string,
        "cluster_id": string,
        "type": enum,
        "nodes": [
            {
                "cloud_unique_id": string,
                "ip": string,
                "heartbeat_port": int
            },
            {
                "cloud_unique_id": string,
                "ip": string,
                "heartbeat_port": int
            }
        ]
    }
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
instance_id | instance_id | 是 |
cluster | cluster对象 | 是 | 
cluster.cluster_name |  将添加mysql user name的cluster name | 是 |
cluster.cluster_id | 将添加mysql user name的cluster id | 是 |
cluster.type | cluster的类型，与上文中add_cluster处解释一致
cluster.nodes | cluster中的节点数组 | 是 | 与上文add_cluster处字段解释一致


* 请求示例

```
PUT /MetaService/http/add_node?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": "instance_id_deadbeef_1",
    "cluster": {
        "cluster_name": "cluster_name1",
        "cluster_id": "cluster_id1",
        "type": "COMPUTE",
        "nodes": [
            {
                "cloud_unique_id": "cloud_unique_id_compute_node2",
                "ip": "172.21.0.50",
                "heartbeat_port": 9051
            },
            {
                "cloud_unique_id": "cloud_unique_id_compute_node3",
                "ip": "172.21.0.52",
                "heartbeat_port": 9052
            }
        ]
    }
}
```

* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串


* 成功返回示例

```
{
 "code": "OK",
 "msg": ""
}

```

* 失败返回示例
```
{
 "code": "INTERANAL_ERROR",
 "msg": "cloud_unique_id is already occupied by an instance, instance_id=instance_id_deadbeef_1 cluster_name=dx_cluster_name1 cluster_id=cluster_id1 cloud_unique_id=cloud_unique_id_compute_node2"
}
```


## cluster减少节点

### 接口描述

本接口用于将instance下的某cluster减少若干相同类型的节点

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/drop_node?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": string,
    "cluster": {
        "cluster_name": string,
        "cluster_id": string,
        "type": enum,
        "nodes": [
            {
                "cloud_unique_id": string,
                "ip": string,
                "heartbeat_port": int
            },
            {
                "cloud_unique_id": string,
                "ip": string,
                "heartbeat_port": int
            }
        ]
    }
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
instance_id | instance_id | 是 |
cluster | cluster对象 | 是 | 
cluster.cluster_name |  将添加mysql user name的cluster name | 是 |
cluster.cluster_id | 将添加mysql user name的cluster id | 是 |
cluster.type | cluster类型| 是 | 
cluster.node | cluster中节点信息 | 是 | 数组


* 请求示例

```
PUT /MetaService/http/drop_node?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": "instance_id_deadbeef_1",
    "cluster": {
        "cluster_name": "cluster_name1",
        "cluster_id": "cluster_id1",
        "type": "COMPUTE",
        "nodes": [
            {
                "cloud_unique_id": "cloud_unique_id_compute_node2",
                "ip": "172.21.0.50",
                "heartbeat_port": 9051
            },
            {
                "cloud_unique_id": "cloud_unique_id_compute_node3",
                "ip": "172.21.0.52",
                "heartbeat_port": 9052
            }
        ]
    }
}
```


* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串


* 成功返回示例

```
{
 "code": "OK",
 "msg": ""
}

```

* 失败返回示例
```
{
 "code": "INTERANAL_ERROR",
 "msg": "cloud_unique_id can not find to drop node, instance_id=instance_id_deadbeef_1 cluster_name=cluster_name1 cluster_id=cluster_id1 cloud_unique_id=cloud_unique_id_compute_node2"
}
```

## 为cluster添加默认user name

### 接口描述

本接口用于将instance下的某cluster添加一些用户名，这些用户使用mysql client登录进系统，可以使用默认cluster

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/update_cluster_mysql_user_name?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": string,
    "cluster": {
        "cluster_name": "string",
        "cluster_id": "string",
        "mysql_user_name": [
            string
        ]
    }
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
instance_id | instance_id | 是 |
cluster | cluster对象 | 是 | 
cluster.cluster_name |  将添加mysql user name的cluster name | 是 |
cluster.cluster_id | 将添加mysql user name的cluster id | 是 |
cluster.mysql_user_name | mysql user name | 是 | 字符串数组


* 请求示例

```
PUT /MetaService/http/update_cluster_mysql_user_name?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "instance_id": "instance_id_deadbeef",
    "cluster": {
        "cluster_name": "cluster_name2",
        "cluster_id": "cluster_id1",
        "mysql_user_name": [
            "jack",
            "root"
        ]
    }
}
```

* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串


* 成功返回示例

```
{
 "code": "OK",
 "msg": ""
}

```

* 失败返回示例
```
{
 "code": "INTERANAL_ERROR",
 "msg": "no mysql user name to change"
}
```


## 获取cluster配置的S3信息

### 接口描述

本接口用于获取instance配置的S3的ak、sk信息，可相同参数调用多次

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/get_obj_store_info?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{"cloud_unique_id": "cloud_unique_id_compute_node1"}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
cloud_unique_id | 节点的cloud_unique_id | 是 | instance下某节点的unique_id查询整个instance配置的S3信息


* 请求示例

```
PUT /MetaService/http/get_obj_store_info?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{"cloud_unique_id": "cloud_unique_id_compute_node1"}
```

* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串
result | 查询结果对象 | 是 |


* 成功返回示例

```
{
    "code": "OK",
    "msg": "",
    "result": {
        "obj_info": [
            {
                "ctime": "1662543056",
                "mtime": "1662543056",
                "id": "1",
                "ak": "xxxx",
                "sk": "xxxxx",
                "bucket": "doris-xxx-1308700295",
                "prefix": "selectdb-xxxx-regression-prefix",
                "endpoint": "cos.ap-yyy.xxxx.com",
                "region": "ap-xxx"
            }
        ]
    }
}

```

* 失败返回示例
```
{
 "code": "INVALID_ARGUMENT",
 "msg": "empty instance_id"
}
```


## 更新instance的ak、sk信息

### 接口描述

本接口用于更新instance配置的S3的ak、sk信息，使用id去查询修改项，id可以用get_obj_store_info查询得到，多次相同参数调用此接口会报错

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/update_ak_sk?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "cloud_unique_id": string,
    "obj": {
        "id": string,
        "ak": string,
        "sk": string,
    }
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
cloud_unique_id | 节点的cloud_unique_id | 是 |
obj | obj对象 | 是 | S3信息对象
obj.id |  将添加mysql user name的cluster name | 是 | id支持从1到10
obj.ak | 将添加mysql user name的cluster id | 是 |
obj.sk | mysql user name | 是 | 字符串数组


* 请求示例

```
PUT /MetaService/http/update_ak_sk?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "cloud_unique_id": "cloud_unique_id_compute_node1",
    "obj": {
        "id": "1",
        "ak": "test-ak",
        "sk": "test-sk",
    }
}
```

* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串


* 成功返回示例

```
{
 "code": "OK",
 "msg": ""
}

```

* 失败返回示例
```
{
 "code": "INVALID_ARGUMENT",
 "msg": "ak sk eq original, please check it"
}
```


## 添加instance的S3信息

### 接口描述

本接口用于添加instance配置的S3的信息，最多支持添加10条s3信息，每条配置最多不超过1024字节大小

### 请求(Request)

* 请求语法

```
PUT /MetaService/http/add_obj_info?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "cloud_unique_id": string,
    "obj": {
        "ak": string,
        "sk": string,
        "bucket": string,
        "prefix": string,
        "endpoint": string,
        "region": string
    }
}
```
* 请求参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
cloud_unique_id | 节点的cloud_unique_id | 是 |
obj | obj对象 | 是 | S3信息对象
obj.ak | 将添加S3的ak | 是 |
obj.sk | 将添加S3的sk | 是 |
obj.bucket | 将添加S3的bucket | 是 |
obj.prefix | 将添加S3的prefix | 是 |
obj.endpoint | 将添加S3的endpoint | 是 |
obj.region | 将添加S3的region | 是 |

* 请求示例

```
PUT /MetaService/http/add_obj_info?token=<token> HTTP/1.1
Content-Length: <ContentLength>
Content-Type: text/plain

{
    "cloud_unique_id": "cloud_unique_id_compute_node1",
    "obj": {
        "ak": "test-ak91",
        "sk": "test-sk1",
        "bucket": "test-bucket",
        "prefix": "test-prefix",
        "endpoint": "test-endpoint",
        "region": "test-region"
    }
}
```

* 返回参数

参数名 | 描述 | 是否必须 | 备注
-----| -----| -----| -----
code | 返回状态码 | 是 | 枚举值，包括OK、INVALID_ARGUMENT、INTERANAL_ERROR、ALREADY_EXISTED
msg | 出错原因 | 是 | 若出错返回错误原因，未出错返回空字符串


* 成功返回示例

```
{
 "code": "OK",
 "msg": ""
}

```

* 失败返回示例
```
{
 "code": "INVALID_ARGUMENT",
 "msg": "s3 conf info err, please check it"
}
```