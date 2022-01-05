# minihive

Code for Scaling Database Systems 2021WS

## Get Started

### minihive docker

run

```sh
docker run -d --name minihive -p 2222:22 minihive-docker
```

login (password: minihive)

```
ssh -p 2222 minihive@localhost
```

copy file to mihive docker

```bash
scp -P 2222 -r minihive minihive@localhost:/home/minihive
```

### HDFS

create default dir (used as cwd)

```sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/minihive
```

put file on cwd

```
hdfs dfs -put local/Person.js Person.json
```

```
hdfs dfs -ls tmp1
```

more see: [HDFS file system](https://hadoop.apache.org/docs/r3.2.0/hadoop-project-dist/hadoop-common/FileSystemShell.html)

### Test in MS3

#### Test Simple Select

```sh
PYTHONPATH=. luigi --module minihive.ra2mr SelectTask --querystring "\select_{Person.gender='female'}(Person);" --exec-environment HDFS --local-scheduler

```

```
Amy, 16, female
Fay, 21, female
Hil, 30, female
```

```
Person  {"Person.name": "Fay", "Person.age": 21, "Person.gender": "female"}
Person  {"Person.name": "Hil", "Person.age": 30, "Person.gender": "female"}
Person  {"Person.name": "Amy", "Person.age": 16, "Person.gender": "female"}
```

#### Test Project

```sh
PYTHONPATH=. luigi --module minihive.ra2mr ProjectTask --querystring "\project_{Person.name, Eats.pizza} (Person \join_{Person.name = Eats.name} Eats);" --exec-environment HDFS --local-scheduler

```

```
Amy, mushroom
Amy, pepperoni
Ben, cheese
Ben, pepperoni
Cal, supreme
Dan, cheese
Dan, mushroom
Dan, pepperoni
Dan, sausage
Dan, supreme
Eli, cheese
Eli, supreme
Fay, mushroom
Gus, cheese
Gus, mushroom
Gus, supreme
Hil, cheese
Hil, supreme
Ian, pepperoni
Ian, supreme
```

```
{"Person.name": "Dan", "Eats.pizza": "supreme"} {"Person.name": "Dan", "Eats.pizza": "supreme"}
{"Person.name": "Dan", "Eats.pizza": "mushroom"}        {"Person.name": "Dan", "Eats.pizza": "mushroom"}
{"Person.name": "Dan", "Eats.pizza": "pepperoni"}       {"Person.name": "Dan", "Eats.pizza": "pepperoni"}
{"Person.name": "Dan", "Eats.pizza": "sausage"} {"Person.name": "Dan", "Eats.pizza": "sausage"}
{"Person.name": "Gus", "Eats.pizza": "mushroom"}        {"Person.name": "Gus", "Eats.pizza": "mushroom"}
{"Person.name": "Ian", "Eats.pizza": "pepperoni"}       {"Person.name": "Ian", "Eats.pizza": "pepperoni"}
{"Person.name": "Dan", "Eats.pizza": "cheese"}  {"Person.name": "Dan", "Eats.pizza": "cheese"}
{"Person.name": "Amy", "Eats.pizza": "pepperoni"}       {"Person.name": "Amy", "Eats.pizza": "pepperoni"}
{"Person.name": "Eli", "Eats.pizza": "cheese"}  {"Person.name": "Eli", "Eats.pizza": "cheese"}
{"Person.name": "Hil", "Eats.pizza": "cheese"}  {"Person.name": "Hil", "Eats.pizza": "cheese"}
{"Person.name": "Ben", "Eats.pizza": "pepperoni"}       {"Person.name": "Ben", "Eats.pizza": "pepperoni"}
{"Person.name": "Fay", "Eats.pizza": "mushroom"}        {"Person.name": "Fay", "Eats.pizza": "mushroom"}
{"Person.name": "Ian", "Eats.pizza": "supreme"} {"Person.name": "Ian", "Eats.pizza": "supreme"}
{"Person.name": "Gus", "Eats.pizza": "cheese"}  {"Person.name": "Gus", "Eats.pizza": "cheese"}
{"Person.name": "Gus", "Eats.pizza": "supreme"} {"Person.name": "Gus", "Eats.pizza": "supreme"}
{"Person.name": "Ben", "Eats.pizza": "cheese"}  {"Person.name": "Ben", "Eats.pizza": "cheese"}
{"Person.name": "Cal", "Eats.pizza": "supreme"} {"Person.name": "Cal", "Eats.pizza": "supreme"}
{"Person.name": "Eli", "Eats.pizza": "supreme"} {"Person.name": "Eli", "Eats.pizza": "supreme"}
{"Person.name": "Hil", "Eats.pizza": "supreme"} {"Person.name": "Hil", "Eats.pizza": "supreme"}
{"Person.name": "Amy", "Eats.pizza": "mushroom"}        {"Person.name": "Amy", "Eats.pizza": "mushroom"}
```

#### Test Simple Join and Rename

```sh
PYTHONPATH=. luigi --module minihive.ra2mr JoinTask --querystring "(\rename_{P:*} Person) \join_{P.gender = Q.gender and P.age = Q.age} (\rename_{Q:*} Person);" --exec-environment HDFS --local-scheduler
```

```
Amy, 16, female, Amy, 16, female
Ben, 21, male, Ben, 21, male
Cal, 33, male, Cal, 33, male
Dan, 13, male, Dan, 13, male
Eli, 45, male, Eli, 45, male
Fay, 21, female, Fay, 21, female
Gus, 24, male, Gus, 24, male
Hil, 30, female, Hil, 30, female
Ian, 18, male, Ian, 18, male
```

```
foo     {"P.name": "Fay", "P.age": 21, "P.gender": "female", "Q.name": "Fay", "Q.age": 21, "Q.gender": "female"}
foo     {"P.name": "Hil", "P.age": 30, "P.gender": "female", "Q.name": "Hil", "Q.age": 30, "Q.gender": "female"}
foo     {"Q.name": "Ben", "Q.age": 21, "Q.gender": "male", "P.name": "Ben", "P.age": 21, "P.gender": "male"}
foo     {"Q.name": "Eli", "Q.age": 45, "Q.gender": "male", "P.name": "Eli", "P.age": 45, "P.gender": "male"}
foo     {"Q.name": "Dan", "Q.age": 13, "Q.gender": "male", "P.name": "Dan", "P.age": 13, "P.gender": "male"}
foo     {"Q.name": "Amy", "Q.age": 16, "Q.gender": "female", "P.name": "Amy", "P.age": 16, "P.gender": "female"}
foo     {"P.name": "Gus", "P.age": 24, "P.gender": "male", "Q.name": "Gus", "Q.age": 24, "Q.gender": "male"}
foo     {"P.name": "Ian", "P.age": 18, "P.gender": "male", "Q.name": "Ian", "Q.age": 18, "Q.gender": "male"}
foo     {"Q.name": "Cal", "Q.age": 33, "Q.gender": "male", "P.name": "Cal", "P.age": 33, "P.gender": "male"}

```

### Test multilevel Join

```
PYTHONPATH=. luigi --module minihive.ra2mr ProjectTask --querystring "\project_{Person.name} (\select_{Person.age = 16} (((Person \join_{Person.name = Eats.name} Eats) \join_{Person.name = P2.name} (\rename_{P2: *} Person)) \join_{P2.name = Eats2.name} (\rename_{Eats2: *} Eats)));"  --exec-environment HDFS --local-scheduler
```

```

Amy

```

```

{"Person.name": "Amy"} {"Person.name": "Amy"}

```

```

hdfs dfs -cat tmp1/part-\*
hdfs dfs -rm -r tmp\*

```
