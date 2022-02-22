#### Environment file: `.env`
```
MONGO_URI=mongodb://localhost:27017
MYSQL_URI=jdbc:mysql://localhost/employees?user=root&password=root123456
TABLE=departments
TABLE_ID=dept_no
QUERY_LIMIT=2
MONGO_DB=employees
TABLE_ID_TYPE=string
INTERVAL=5000
```

#### Usage
```
java -Denv="<Environment file directory>" -jar mysql2mongo.jar
