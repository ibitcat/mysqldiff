# mysqldiff(mysql 数据库对比维护工具)

## 安装

go 版本：go 1.12
```
git clone https://github.com/domixcat/mysqldiff.git
cd mysqldiff
go mod tidy
go build
```

## 特性

- 对比sql文件和指定数据库之间的差异，测试并应用差异
- 支持MyIsam引擎分表的差异对比
- 支持like语句（eg.: create table `xxx` like `yyy`;）

**注意：sql文件的语法一定要和与使用`show create table xxx`命令查询出来的语法一致，例如：关键字一定要大写，字符串varchar需要指定字符集等**

## 使用

连接mysql的参数与**mysql命令行**一致，目前只提供基本的参数支持，使用`mysqldiff -help`查看参数。

### 参数

- `-u dbuser -p password -h host -P port -charset=utf8mb4 -collate=utf8mb4_general_ci`
- `-d dbname` 表示要维护的数据库名，如果不存在，则自动创建
- `-f file.sql` 表示更新的sql文件
- `-only-check` 表示只检查差异，但不执行差异到数据库
- `-modify` 表示执行成功后修正源文件

### 示例

- 使用game.sql 更新数据库domi

  ```
  mysqldiff -u root -p 123456 -h 127.0.0.1 -P 3306 -d domi -f game.sql
  ```

- 使用game.sql 对比与数据库的差异

  ```
  mysqldiff -u root -p 123456 -h 127.0.0.1 -P 3306 -d domi -f game.sql  -only-check
  ```
