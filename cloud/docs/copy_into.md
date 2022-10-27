- [描述](#描述)
- [语法](#语法)
- [输出](#输出)
- [举例](#举例)

## 描述

将stage中的数据文件导入到SelectDB的表中.

注意：一个stage下同名且相同内容的文件只能导入到一个table中一次，不能重复导入。

- external stage：参考[create external stage](create_stage.md)

- internal stage：参考[向internal stage上传文件](copy_upload.md)

## 语法

```
COPY INTO [<db_name>.]<table_name> FROM {copy_from_param} PROPERTIES (
    {copy_into_properties}
)
```

其中:

- `copy_from_param`

  指定了导入的stage名，文件，列的转换、映射、过滤规则等.

  ```
  copy_from_param ::=
      {stage_and_glob}
    | ( SELECT {copy_select_expr_list} FROM {stage_and_glob} {copy_where_expr_list} )
  ```

  ```
  stage_and_glob ::=
      @{stage_name}
    | @{stage_name}('{file_glob}')
  ```

  `stage_name`

  - 用户创建的[external stage](create_stage.md)名

  - 属于用户的默认[internal stage](copy_upload.md)，名为`~`

  `file_glob`

  - 使用[glob语法]()指定需要导入的文件

  `copy_select_expr_list`

  - 进行列的转换，映射等。可以通过调整输入数据源的列顺序来实现与目标表的不同列的进行映射（注意：只能进行整行映射）：

    ```
    copy_select_expr_list ::=
        *
      | { $<file_col_num> | <expr> }[ , ... ]
    ```

      `file_col_num`

      - 列在导入文件中按照指定分隔符分隔后的序号(如`1`表示第1列)

      `expr`

      - 指定一个表达式，比如算数运算等


  `copy_where_expr_list`

  - 对文件中的列按照表达式进行过滤，被过滤的行不会被导入到表中

    ```
    copy_where_expr_list ::=
        WHERE <predicate_expr>
    ```

- `copy_into_properties`

  指定CopyInto相关的参数。目前支持以下参数：

    - `file.type`

      导入文件的类型，目前支持`csv`,`json`,`orc`,`parquet`。

      非必需。如未设置，优先使用stage配置的默认文件类型；如果stage上未设置，系统自动推断类型。

    - `file.compression`

      导入文件的压缩类型，目前支持`gz`,`bz2`,`lz4`,`lzo`,`deflate`。

      非必需。如未设置，优先使用stage配置的默认压缩类型；如果stage上未设置，系统自动推断类型。

    - `file.column_separator`

      导入文件的列分隔符。

      非必需。如未设置，优先使用stage配置的默认列分隔符；如果stage上未设置，使用系统默认值`\t`。

    - `file.line_delimiter`

      导入文件的行分隔符。

      非必需。如未设置，优先使用stage配置的默认行分隔符；如果stage上未设置，使用系统默认值`\n`。

    - `copy.size_limit`

      导入的文件大小，单位为Byte。如果匹配的待导入文件超出大小限制，只导入满足大小限制的部分文件。

      非必需。如未设置，优先使用stage配置的默认导入大小；如果stage上未设置，默认不限制。

    - `copy.on_error`

      导入时，当数据质量不合格时的错误处理方式。目前支持:

      - `max_filter_ratio_{number}`：设置最大错误率为`{number}`，其中，`{number}`为`[0-1]`区间的浮点数。如果导入的数据的错误率低于阈值，则这些错误行将被忽略，其他正确的数据将被导入。
      - `abort_statement`：当数据有错误行时，中断导入，等价于`max_filter_ratio_0`。默认行为
      - `continue`：忽略错误行，导入正确行，等价于`max_filter_ratio_1`

      非必需。如未设置，优先使用stage配置的默认错误处理策略；如果stage上未设置，使用系统默认策略。

    - `copy.async`

      导入是否异步执行。支持`true`, `false`。 默认值为`true`，即异步执行，通过`show copy`查看异步执行的copy任务。

## 输出

Copy into默认异步执行，返回一个`queryId`，如：

```
mysql> copy into db.t1 from @exs('2.csv');
+-----------------------------------+---------+------+------+------------+------------+--------------+------+
| id                                | state   | type | msg  | loadedRows | filterRows | unselectRows | url  |
+-----------------------------------+---------+------+------+------------+------------+--------------+------+
| 8fcf20b156dc4f66_99aa062042941aff | PENDING |      |      |            |            |              |      |
+-----------------------------------+---------+------+------+------------+------------+--------------+------+
1 row in set (0.14 sec)
```

根据`id`，使用`SHOW COPY`命令查询执行结果:

```
mysql> SHOW COPY WHERE id = '8fcf20b156dc4f66_99aa062042941aff';
+-----------------------------------+-------+----------------------------------------+----------+---------------------+------+-----------------------------------------------------+-----------------------------------------------------+----------+---------------------+---------------------+---------------------+---------------------+---------------------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+--------------+-------------------------------------------------------+
| Id                                | JobId | Label                                  | State    | Progress            | Type | EtlInfo                                             | TaskInfo                                            | ErrorMsg | CreateTime          | EtlStartTime        | EtlFinishTime       | LoadStartTime       | LoadFinishTime      | URL  | JobDetails                                                                                                                                                                                               | TransactionId    | ErrorTablets | Files                                                 |
+-----------------------------------+-------+----------------------------------------+----------+---------------------+------+-----------------------------------------------------+-----------------------------------------------------+----------+---------------------+---------------------+---------------------+---------------------+---------------------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+--------------+-------------------------------------------------------+
| 8fcf20b156dc4f66_99aa062042941aff | 17012 | copy_f8a124900f7d42f6_91dad473d45a34bd | FINISHED | ETL:100%; LOAD:100% | COPY | unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=2 | cluster:N/A; timeout(s):14400; max_filter_ratio:0.0 | NULL     | 2022-10-21 09:06:48 | 2022-10-21 09:06:54 | 2022-10-21 09:06:54 | 2022-10-21 09:06:54 | 2022-10-21 09:06:55 | NULL | {"Unfinished backends":{"3e2fc170198240c0-929be46e8ca47838":[]},"ScannedRows":2,"TaskNumber":1,"LoadBytes":30,"All backends":{"3e2fc170198240c0-929be46e8ca47838":[10003]},"FileNumber":1,"FileSize":14} | 6141324627542016 | {}           | ["s3://justtmp-bj-1308700295/meiyi_cloud_test/2.csv"] |
+-----------------------------------+-------+----------------------------------------+----------+---------------------+------+-----------------------------------------------------+-----------------------------------------------------+----------+---------------------+---------------------+---------------------+---------------------+---------------------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+--------------+-------------------------------------------------------+
1 row in set (0.01 sec)
```

其中，`files`列为本次Copy任务导入的文件。

## 举例

* 把名为`ext_stage`的stage中的数据，导入到表`test_table`中

  ```
  COPY INTO test_table FROM @ext_stage
  ```

  系统会自动扫描stage下未导入到表`test_table`中的部分文件，进行导入

* 把名为`ext_stage`的stage中的数据文件`1.csv`，导入到表`test_table`中

  ```
  COPY INTO test_table FROM @ext_stage('1.csv')
  ```

* 把名为`ext_stage`的stage中的数据文件`dir1/subdir_2/1.csv`，导入到表`test_table`中

  如果创建`ext_stage`时，prefix为空，则导入语句为：

  ```
  COPY INTO test_table FROM @ext_stage('dir1/subdir_2/1.csv')
  ```

  如果创建`ext_stage`时，prefix为`dir1`，则导入语句为：

  ```
  COPY INTO test_table FROM @ext_stage('subdir_2/1.csv')
  ```

  如果创建`ext_stage`时，prefix为`dir1/subdir_2`，则导入语句为：

  ```
  COPY INTO test_table FROM @ext_stage('1.csv')
  ```

* 把名为`ext_stage`的stage中的`dir1/subdir_2/`路径下`.csv`结尾的文件，导入到表`test_table`中

  如果创建`ext_stage`时，prefix为空，则导入语句为：

  ```
  COPY INTO test_table FROM @ext_stage('dir1/subdir_2/*.csv')
  ```

  如果创建`ext_stage`时，prefix为`dir1`，则导入语句为：

  ```
  COPY INTO test_table FROM @ext_stage('subdir_2/*.csv')
  ```

  如果创建`ext_stage`时，prefix为`dir1/subdir_2`，则导入语句为：

  ```
  COPY INTO test_table FROM @ext_stage('*.csv')
  ```

* 把名为`ext_stage`的stage中的`dir1`目录下的各级子目录中以`.csv`结尾的文件，导入到表`test_table`中

  ```
  COPY INTO test_table FROM @ext_stage('dir1/**.csv')
  ```

* 把名为`ext_stage`的stage中的数据文件`1.csv`，导入到表`test_table`中，并指定文件的列分隔符为`,`，行分隔符为`\n`:

  ```
  COPY INTO test_table FROM @ext_stage('1.csv') PROPERTIES (
      'file.column_separator' = ',',
      'file.line_delimiter' = '\n'
  )
  ```

* 把名为`ext_stage`的stage中的数据文件`1.csv`，导入到表`test_table`中，并指定同步执行:

  ```
  COPY INTO test_table FROM @ext_stage('1.csv') PROPERTIES (
      'copy.async' = 'false'
  )
  ```

* 把用户默认的[internal stage](copy_upload.md)中的数据文件`1.csv`，导入到表`test_table`中

  ```
  COPY INTO test_table FROM @~('1.csv')
  ```

* 列的映射，转换，过滤等

  假如文件有3列，分别为`$1`(第1列),`$2`(第2列),`$3`(第3列)，分别导入到表的三列：`id`,`name`,`score`， 即`$1`$\rightarrow$`id`, `$2`$\rightarrow$`name`, `$3`$\rightarrow$`score`, 以下几个语句等价：

  ```
  COPY INTO test_table FROM (SELECT * FROM @ext_stage('1.csv'))
  COPY INTO test_table FROM (SELECT $1, $2, $3 FROM @ext_stage('1.csv'))
  ```

  假如文件有4列，分别为`$1`(第1列),`$2`(第2列),`$3`(第3列),`$4`(第4列)，分别将1,3,4列导入到表的三列：`id`,`name`,`score`，即`$1`$\rightarrow$`id`, `$3`$\rightarrow$`name`, `$4`$\rightarrow$`score`：

  ```
  COPY INTO test_table FROM (SELECT $1, $3, $4 FROM @ext_stage('1.csv'))
  ```

  假如文件有2列，分别为`$1`(第1列),`$2`(第2列)，分别导入到表的前两列：`id`,`name`，`score`使用表的默认值或`NULL`，即`$1`$\rightarrow$`id`, `$2`$\rightarrow$`name`, `NULL`$\rightarrow$`score`

  ```
  COPY INTO test_table FROM (SELECT $1, $2, NULL FROM @ext_stage('1.csv'))
  ```

  假如文件有3列，分别为`$1`(第1列),`$2`(第2列),`$3`(第3列)，分别将1,3,2列导入到表的三列：`id`,`name`,`score`，即`$1`$\rightarrow$`id`, `$3`$\rightarrow$`name`, `$2`$\rightarrow$`score`：

  ```
  COPY INTO test_table FROM (SELECT $1, $3, $2 FROM @ext_stage('1.csv'))
  ```

  假如文件有3列，分别为`$1`(第1列),`$2`(第2列),`$3`(第3列)，过滤出第三列大于`60`的，然后分别导入到表的三列：`id`, `name`, `score`：

  ```
  COPY INTO test_table FROM (SELECT $1, $2, $3 FROM @ext_stage('1.csv') WHERE $3 > 60)
  ```

  假如文件有3列，分别为`$1`(第1列),`$2`(第2列),`$3`(第3列)，对第三列统一增加`10`的，然后分别导入到表的三列：`id`,`name`,`score`：

  ```
  COPY INTO test_table FROM (SELECT $1, $2, $3 + 10 FROM @ext_stage('1.csv'))
  ```

  假如文件有3列，分别为`$1`(第1列),`$2`(第2列),`$3`(第3列)，过滤出第三列小于`60`的，然后分别加`10`，再导入到表的三列：`id`,`name`,`score`：

  ```
  COPY INTO test_table FROM (SELECT $1, $2, $3 + 10 FROM @ext_stage('1.csv') WHERE $3 < 60)
  ```

  假如文件有3列，分别为`$1`(第1列),`$2`(第2列),`$3`(第3列)，对第二列的字符串进行截取，再导入到表的三列：`id`,`name`,`score`：

  ```
  COPY INTO test_table FROM (SELECT $1, substring($2, 2), $3 FROM @ext_stage('1.csv'))
  ```