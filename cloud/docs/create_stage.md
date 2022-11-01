## 描述

创建一个external stage，用于将其中的数据文件导入到SelectDB的表中.

建议：用户可以建立一个专门用于数据导入的子账号，使用bucket policy授予该子账号特定prefix的读权限，便于SelectDB读取需要导入的对象数据源。

相关文档:[show stage](show_stage.md), [drop stage](drop_stage.md)

## 语法

```
CREATE STAGE [IF NOT EXISTS] <stage_name> PROPERTIES (
    {stage_properties}
)
```

- `stage_properties`

  指定stage相关的参数。目前支持以下参数：

    - `endpoint`

      对象存储的`Endpoint`。必需。

    - `region`

      对象存储的`Region`。必需。

    - `bucket`

      对象存储的`Bucket`。必需。

    - `prefix`

      用户数据文件在该`Bucket`下的前缀路径。非必需，默认为`Bucket`下的根路径。

    - `provider`

      指定提供对象存储的云厂商。必需。目前支持:
      - `OSS`：阿里云
      - `COS`：腾讯云
      - `BOS`：百度云
      - `OBS`：华为云
      - `S3`：亚马逊云

    - `ak`

      对象存储的`Access Key ID`。必需。

    - `sk`

      对象存储的`Secret Access Key`。必需。

    - `default.file.type`

      该stage存储文件的默认类型，目前支持`csv`,`json`,`orc`,`parquet`。非必需，导入时可覆盖该参数。

    - `default.file.compression`

      该stage存储文件的默认压缩类型，目前支持`gz`,`bz2`,`lz4`,`lzo`,`deflate`。非必需，导入时可覆盖该参数。

    - `default.file.column_separator`

      该stage存储文件的默认列分隔符，默认`\t`。非必需，导入时可覆盖该参数。

    - `default.file.line_delimiter`

      该stage存储文件的默认行分隔符，默认`\n`。非必需，导入时可覆盖该参数。

    - `default.copy.size_limit`

      导入该stage下的文件时，默认的导入大小，单位为Byte，默认为不限制。非必需，导入时可覆盖该参数。

    - `default.copy.on_error`

      导入该stage下的文件时，当数据质量不合格时，默认的错误处理方式。非必需，导入时可覆盖该参数。目前支持:

      - `max_filter_ratio_{number}`：设置最大错误率为`{number}`，其中，`{number}`为`[0-1]`区间的浮点数。如果导入的数据的错误率低于阈值，则这些错误行将被忽略，其他正确的数据将被导入。
      - `abort_statement`：当数据有错误行时，中断导入，等价于`max_filter_ratio_0`。默认行为
      - `continue`：忽略错误行，导入正确行，等价于`max_filter_ratio_1`

    - `default.copy.strict_mode`

      对于导入过程中的列类型转换进行严格过滤，参考[导入严格模式](https://doris.apache.org/zh-CN/docs/data-operate/import/import-scenes/load-strict-mode?_highlight=stric)。默认为`false`。非必需，导入时可覆盖该参数。

## 举例
 
1. 创建名为`test_stage`的stage:
```
CREATE STAGE test_stage PROPERTIES (
    'endpoint' = 'cos.ap-beijing.myqcloud.com',
    'region' = 'ap-beijing',
    'bucket' = 'selectdb_test',
    'prefix' = 'test_stage',
    'provider' = 'cos',
    'ak' = 'XX',
    'sk' = 'XX'
)
```

2. 创建名为`test_stage`的stage，并指定默认的文件类型和列分隔符：

```
CREATE STAGE test_stage PROPERTIES (
    'endpoint' = 'cos.ap-beijing.myqcloud.com',
    'region' = 'ap-beijing',
    'bucket' = 'selectdb_test',
    'prefix' = 'test_stage',
    'provider' = 'cos',
    'ak' = 'XX',
    'sk' = 'XX',
    'default.file.type' = 'csv',
    'default.file.column_separator' = ','
)
```
