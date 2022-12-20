---
{
<<<<<<< HEAD:docs/en/docs/sql-manual/sql-functions/string-functions/uuid.md
    "title": "uuid",
=======
    "title": "to_base64",
>>>>>>> 1.2.0-rc04-origin:docs/en/docs/sql-manual/sql-functions/string-functions/to_base64.md
    "language": "en"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<<<<<<< HEAD:docs/en/docs/sql-manual/sql-functions/string-functions/uuid.md
## uuid
### description
#### Syntax

`VARCHAR uuid()`

return a random uuid string

=======
## to_base64
### description
#### Syntax

`VARCHAR to_base64(VARCHAR str)`


Returns the result of Base64 encoding the input string
>>>>>>> 1.2.0-rc04-origin:docs/en/docs/sql-manual/sql-functions/string-functions/to_base64.md

### example

```
<<<<<<< HEAD:docs/en/docs/sql-manual/sql-functions/string-functions/uuid.md
mysql> select uuid();
+--------------------------------------+
| uuid()                               |
+--------------------------------------+
| 29077778-fc5e-4603-8368-6b5f8fd55c24 |
+--------------------------------------+

=======
mysql> select to_base64('1');
+----------------+
| to_base64('1') |
+----------------+
| MQ==           |
+----------------+

mysql> select to_base64('234');
+------------------+
| to_base64('234') |
+------------------+
| MjM0             |
+------------------+
>>>>>>> 1.2.0-rc04-origin:docs/en/docs/sql-manual/sql-functions/string-functions/to_base64.md
```
### keywords
<<<<<<< HEAD:docs/en/docs/sql-manual/sql-functions/string-functions/uuid.md
    UUID
=======
    to_base64
>>>>>>> 1.2.0-rc04-origin:docs/en/docs/sql-manual/sql-functions/string-functions/to_base64.md
