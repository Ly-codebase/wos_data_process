# ESIMatcher 快速使用指南

## 参数修改说明

### 1. 数据库配置
修改 `db_config` 中的连接信息：
```python
db_config = {
    'username': '你的数据库用户名',  # 修改为实际用户名
    'password': '你的数据库密码',    # 修改为实际密码
    'host': '数据库地址',           # 如 localhost 或 IP
    'database': '数据库名'          # 如 wos_data
}```
### 2. 指定匹配表
修改 `citing_tables`和 `cited_tables`：
```
# 施引文献表列表（支持多表,个人建议一个表一个表处理哈，多表我也没试过）
citing_tables = ['2023esi_Physics_of_Fluids', '2024esi_Chemistry']

# 被引文献表列表（支持多表）
cited_tables = ['2023esi_Physics_of_Fluids', '2020esi_Biology']
```
### 3. 结果表命名格式（可选）
```
result_table_format = "{citing_table}_citing_{cited_table}"  # 默认格式
# 示例结果表名：2023esi_Physics_of_Fluids_citing_2020esi_Biology
```
### 4. 记得安装相应的依赖项
`pip install pandas sqlalchemy pymysql`
