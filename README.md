# simpledb-go

Go implementation of SimpleDB in [Database Design and Implementation: Second Edition](https://www.amazon.co.jp/dp/B085DZM79S)

## Usage

### REPL mode (default)

```
$ BASE_DIR=/tmp/simpledb ./simpledb
```

### PostgreSQL wire protocol mode

Start the server and connect with `psql`:

```
$ MODE=server LISTEN_ADDR=:5432 BASE_DIR=/tmp/simpledb ./simpledb
$ psql -h localhost -p 5432
```

## Example

### INSERT/SELECT
```

> CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))
0 row(s) affected

> INSERT INTO students (id, name, class) VALUES (1, "sheep", "A")
1 row(s) affected
> INSERT INTO students (id, name, class) VALUES (2, "goat", "B")
1 row(s) affected
> INSERT INTO students (id, name, class) VALUES (3, "cow", "B")
1 row(s) affected
> INSERT INTO students (id, name, class) VALUES (4, "cat", "C")
1 row(s) affected   

> SELECT id, name, class FROM students
id | name  | class
---+-------+------
1  | sheep | A    
2  | goat  | B    
3  | cow   | B    
4  | cat   | C    
4 row(s)

> SELECT id, name, class FROM students WHERE class = "B"
id | name | class
---+------+------
2  | goat | B    
3  | cow  | B    
2 row(s)
```

### JOIN

```
> CREATE TABLE results (student_id INT, score INT)
0 row(s) affected
> INSERT INTO results (student_id, score) VALUES (1, 100)
1 row(s) affected
> INSERT INTO results (student_id, score) VALUES (2, 70)
1 row(s) affected
> INSERT INTO results (student_id, score) VALUES (3, 80)
1 row(s) affected

> SELECT id, name, score FROM students, results WHERE id = student_id AND score > 70
id | name  | score
---+-------+------
1  | sheep | 100  
3  | cow   | 80   
2 row(s)
```

### TRANSACTION
```
> START TRANSACTION
> INSERT INTO students (id, name, class) VALUES (5, "gorilla", "D")
1 row(s) affected
> INSERT INTO students (id, name, class) VALUES (6, "monkey", "E")
1 row(s) affected
> SELECT id, name, class FROM students
id | name    | class
---+---------+------
1  | sheep   | A    
2  | goat    | B    
3  | cow     | B    
4  | cat     | C    
5  | gorilla | D    
6  | monkey  | E    
6 row(s)
> ROLLBACK
> SELECT id, name, class FROM students
id | name  | class
---+-------+------
1  | sheep | A    
2  | goat  | B    
3  | cow   | B    
4  | cat   | C    
4 row(s)
```

### INDEX
```
> CREATE INDEX idx_name on students (name)
0 row(s) affected
> SELECT id, name, class FROM students WHERE name = "goat"
id | name | class
---+------+------
2  | goat | B    
1 row(s)
```

### UPDATE/DELETE
```
> UPDATE students SET class = "F" WHERE id = 4
1 row(s) affected
> SELECT id, name, class FROM students
id | name  | class
---+-------+------
1  | sheep | A    
2  | goat  | B    
3  | cow   | B    
4  | cat   | F    
4 row(s)

> DELETE FROM students WHERE class = "B"
2 row(s) affected
> SELECT id, name, class FROM students
id | name  | class
---+-------+------
1  | sheep | A    
4  | cat   | F    
2 row(s)
```
