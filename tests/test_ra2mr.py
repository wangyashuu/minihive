import pytest
import json

import luigi

import radb
import minihive.ra2mr as ra2mr

"""
Requires that pytest and pytest-repeat are installed.

To run all the tests, suppressing warnings and output, run

python3 -m pytest test_ra2mr.py -p no:warnings --show-capture=no
"""


def prepareMockFileSystem():
    # Ensure mock file system is empty and all temporary files have been cleared.
    luigi.mock.MockFileSystem().clear()

    f1 = luigi.mock.MockTarget("Person.json").open("w")
    f1.write(
        'Person\t{"Person.name": "Amy", "Person.age": 16, "Person.gender":'
        ' "female"}\n'
    )
    f1.write(
        'Person\t{"Person.name": "Ben", "Person.age": 21, "Person.gender":'
        ' "male"}\n'
    )
    f1.write(
        'Person\t{"Person.name": "Cal", "Person.age": 33, "Person.gender":'
        ' "male"}\n'
    )
    f1.write(
        'Person\t{"Person.name": "Dan", "Person.age": 13, "Person.gender":'
        ' "male"}\n'
    )
    f1.write(
        'Person\t{"Person.name": "Eli", "Person.age": 45, "Person.gender":'
        ' "male"}\n'
    )
    f1.write(
        'Person\t{"Person.name": "Fay", "Person.age": 21, "Person.gender":'
        ' "female"}\n'
    )
    f1.write(
        'Person\t{"Person.name": "Gus", "Person.age": 24, "Person.gender":'
        ' "male"}\n'
    )
    f1.write(
        'Person\t{"Person.name": "Hil", "Person.age": 30, "Person.gender":'
        ' "female"}\n'
    )
    f1.write(
        'Person\t{"Person.name": "Ian", "Person.age": 18, "Person.gender":'
        ' "male"}\n'
    )
    f1.close()

    f2 = luigi.mock.MockTarget("Eats.json").open("w")
    f2.write('Eats\t{"Eats.name" : "Amy", "Eats.pizza": "mushroom"}\n')
    f2.write('Eats\t{"Eats.name" : "Amy", "Eats.pizza": "pepperoni"}\n')
    f2.write('Eats\t{"Eats.name" : "Ben", "Eats.pizza": "cheese"}\n')
    f2.write('Eats\t{"Eats.name" : "Ben", "Eats.pizza": "pepperoni"}\n')
    f2.write('Eats\t{"Eats.name" : "Cal", "Eats.pizza": "supreme"}\n')
    f2.write('Eats\t{"Eats.name" : "Dan", "Eats.pizza": "cheese"}\n')
    f2.write('Eats\t{"Eats.name" : "Dan", "Eats.pizza": "mushroom"}\n')
    f2.write('Eats\t{"Eats.name" : "Dan", "Eats.pizza": "pepperoni"}\n')
    f2.write('Eats\t{"Eats.name" : "Dan", "Eats.pizza": "sausage"}\n')
    f2.write('Eats\t{"Eats.name" : "Dan", "Eats.pizza": "supreme"}\n')
    f2.write('Eats\t{"Eats.name" : "Eli", "Eats.pizza": "cheese"}\n')
    f2.write('Eats\t{"Eats.name" : "Eli", "Eats.pizza": "supreme"}\n')
    f2.write('Eats\t{"Eats.name" : "Fay", "Eats.pizza": "mushroom"}\n')
    f2.write('Eats\t{"Eats.name" : "Gus", "Eats.pizza": "cheese"}\n')
    f2.write('Eats\t{"Eats.name" : "Gus", "Eats.pizza": "mushroom"}\n')
    f2.write('Eats\t{"Eats.name" : "Gus", "Eats.pizza": "supreme"}\n')
    f2.write('Eats\t{"Eats.name" : "Hil", "Eats.pizza": "cheese"}\n')
    f2.write('Eats\t{"Eats.name" : "Hil", "Eats.pizza": "supreme"}\n')
    f2.write('Eats\t{"Eats.name" : "Ian", "Eats.pizza": "pepperoni"}\n')
    f2.write('Eats\t{"Eats.name" : "Ian", "Eats.pizza": "supreme"}\n')
    f2.close()

    f3 = luigi.mock.MockTarget("Frequents.json").open("w")
    f3.write(
        'Frequents\t{"Frequents.name" : "Amy", "Frequents.pizzeria": "Pizza'
        ' Hut"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Ben", "Frequents.pizzeria": "Pizza'
        ' Hut"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Ben", "Frequents.pizzeria": "Chicago'
        ' Pizza"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Cal", "Frequents.pizzeria": "Pizza'
        ' Hut"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Cal", "Frequents.pizzeria": "New York'
        ' Pizza"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Dan", "Frequents.pizzeria": "Straw'
        ' Hat"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Dan", "Frequents.pizzeria": "New York'
        ' Pizza"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Eli", "Frequents.pizzeria": "Straw'
        ' Hat"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Eli", "Frequents.pizzeria": "Chicago'
        ' Pizza"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Fay", "Frequents.pizzeria":'
        ' "Dominos"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Fay", "Frequents.pizzeria": "Little'
        ' Ceasars"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Gus", "Frequents.pizzeria": "Chicago'
        ' Pizza"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Gus", "Frequents.pizzeria": "Pizza'
        ' Hut"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Hil", "Frequents.pizzeria":'
        ' "Dominos"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Hil", "Frequents.pizzeria": "Straw'
        ' Hat"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Hil", "Frequents.pizzeria": "Pizza'
        ' Hut"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Ian", "Frequents.pizzeria": "New York'
        ' Pizza"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Ian", "Frequents.pizzeria": "Straw'
        ' Hat"}\n'
    )
    f3.write(
        'Frequents\t{"Frequents.name" : "Ian", "Frequents.pizzeria":'
        ' "Dominos"}\n'
    )
    f3.close()

    f4 = luigi.mock.MockTarget("Serves.json").open("w")
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Chicago Pizza", "Serves.pizza" :'
        ' "cheese", "Serves.price" : 7.75}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Chicago Pizza", "Serves.pizza" :'
        ' "supreme", "Serves.price" : 8.5}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Dominos", "Serves.pizza" : "cheese",'
        ' "Serves.price" : 9.75}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Dominos", "Serves.pizza" : "mushroom",'
        ' "Serves.price" : 11}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Little Ceasars", "Serves.pizza" :'
        ' "cheese", "Serves.price" : 7}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Little Ceasars", "Serves.pizza" :'
        ' "mushroom", "Serves.price" : 9.25}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Little Ceasars", "Serves.pizza" :'
        ' "pepperoni", "Serves.price" : 9.75}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Little Ceasars", "Serves.pizza" :'
        ' "sausage", "Serves.price" : 9.5}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "New York Pizza", "Serves.pizza" :'
        ' "cheese", "Serves.price" : 7}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "New York Pizza", "Serves.pizza" :'
        ' "pepperoni", "Serves.price" : 8}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "New York Pizza", "Serves.pizza" :'
        ' "supreme", "Serves.price" : 8.5}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Pizza Hut", "Serves.pizza" : "cheese",'
        ' "Serves.price" : 9}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Pizza Hut", "Serves.pizza" :'
        ' "pepperoni", "Serves.price" : 12}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Pizza Hut", "Serves.pizza" : "sausage",'
        ' "Serves.price" : 12}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Pizza Hut", "Serves.pizza" : "supreme",'
        ' "Serves.price" : 12}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Straw Hat", "Serves.pizza" : "cheese",'
        ' "Serves.price" : 9.25}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Straw Hat", "Serves.pizza" :'
        ' "pepperoni", "Serves.price" : 8}\n'
    )
    f4.write(
        'Serves\t{"Serves.pizzeria" : "Straw Hat", "Serves.pizza" : "sausage",'
        ' "Serves.price" : 9.17}\n'
    )
    f4.close()


def setup_function():
    prepareMockFileSystem()


person_amy = (
    '{"Person.name": "Amy", "Person.age": 16, "Person.gender": "female"}'
)
person_fay = (
    '{"Person.name": "Fay", "Person.age": 21, "Person.gender": "female"}'
)
person_hil = (
    '{"Person.name": "Hil", "Person.age": 30, "Person.gender": "female"}'
)
person_ben = (
    '{"Person.name": "Ben", "Person.age": 21, "Person.gender": "male"}'
)


def _evaluate(querystring):
    raquery = radb.parse.one_statement_from_string(querystring)

    task = ra2mr.task_factory(raquery, env=ra2mr.ExecEnv.MOCK)
    luigi.build([task], local_scheduler=True)

    f = task.output()

    f = f.open("r")
    lines = []
    for line in f:
        lines.append(line)

    f.close()
    return lines


def _check(querystring, expected):
    computed = _evaluate(querystring)
    assert len(computed) is len(expected)

    for e in expected:
        item_found = False
        e_json_tuple = json.loads(e)

        for c in computed:
            c_relation, c_tuple = c.split("\t")
            c_json_tuple = json.loads(c_tuple)

            if c_json_tuple == e_json_tuple:
                item_found = True
                break
        assert item_found


def test_select_person_gender_female_person():
    querystring = "\\select_{Person.gender='female'}(Person);"
    expected = [person_amy, person_fay, person_hil]
    _check(querystring, expected)


def test_select_gender_female_person():
    querystring = "\\select_{gender='female'}(Person);"
    expected = [person_amy, person_fay, person_hil]
    _check(querystring, expected)


def test_select_female_gender_person():
    querystring = "\\select_{'female'=gender}(Person);"
    expected = [person_amy, person_fay, person_hil]
    _check(querystring, expected)


def test_select_age_21_Person():
    querystring = "\\select_{age=21}(Person);"
    result = [person_fay, person_ben]
    _check(querystring, result)


def test_select_price_9_Serves():
    querystring = "\\select_{price=9}(Serves);"
    computed = _evaluate(querystring)
    assert len(computed) == 1


def test_select_person_female_age_16():
    querystring = "\\select_{gender='female' and age=16}(Person);"
    result = [person_amy]
    _check(querystring, result)


def test_select_person_age_3():
    querystring = "\\select_{age=3}(Person);"
    _check(querystring, [])


def test_rename_person():
    querystring = "\\rename_{P:*} (Person);"
    computed = _evaluate(querystring)

    assert len(computed) == 9

    for r in computed:
        relation, tuple = r.split("\t")
        json_tuple = json.loads(tuple)

        for k in json_tuple.keys():
            assert k.startswith("P.")


def test_select_rename_p_gender_female_p():
    querystring = "\\select_{P.gender='female'} \\rename_{P:*} (Person);"
    computed = _evaluate(querystring)
    assert len(computed) == 3


def test_person_join_eats_mushroom():
    querystring = (
        "Person \\join_{Person.name = Eats.name} (\\select_{pizza='mushroom'}"
        " Eats);"
    )
    computed = _evaluate(querystring)
    assert len(computed) == 4

    relation, tuple = computed[0].split("\t")
    json_tuple = json.loads(tuple)
    assert len(json_tuple.keys()) == 5


def test_female_person_join_eats():
    querystring = (
        "(\\select_{gender='female'} Person) \\join_{Person.name = Eats.name}"
        " Eats;"
    )
    computed = _evaluate(querystring)
    assert len(computed) == 5

    for line in computed:
        relation, tuple = line.split("\t")
        json_tuple = json.loads(tuple)
        assert json_tuple["Person.name"] in ["Amy", "Fay", "Hil"]
        assert json_tuple["Person.name"] == json_tuple["Eats.name"]
        assert json_tuple["Person.age"] in [16, 21, 30]
        assert json_tuple["Person.gender"] == "female"


def test_empty_join():
    querystring = "Person \\join_{Person.name = Serves.pizzeria} Serves;"
    computed = _evaluate(querystring)
    assert computed == []


def test_person_join_eats_then_join_frequents():
    querystring = (
        "(Person \\join_{Person.name = Eats.name} Eats) \\join_{Eats.name ="
        " Frequents.name} Frequents;"
    )
    computed = _evaluate(querystring)
    assert len(computed) == 42


def test_eats_join_person_then_join_frequents():
    querystring = (
        "(Eats \\join_{Person.name = Eats.name} Person) \\join_{Eats.name ="
        " Frequents.name} Frequents;"
    )
    computed = _evaluate(querystring)
    assert len(computed) == 42


def test_person_then_join_eats_join_frequents():
    querystring = (
        "Person \\join_{Person.name = Eats.name} (Eats \\join_{Eats.name ="
        " Frequents.name} Frequents);"
    )
    computed = _evaluate(querystring)
    assert len(computed) == 42


def test_person_join_eats_join_serves():
    querystring = (
        "Person \\join_{Person.name = Eats.name} Eats "
        "\\join_{Eats.pizza = Serves.pizza} \\select_{price=8}Serves;"
    )
    computed = _evaluate(querystring)
    assert len(computed) == 8


def test_person_join_eats_join_serves_dominos():
    querystring = (
        "(Person \\join_{Person.name = Eats.name} Eats) \\join_{Eats.pizza ="
        " Serves.pizza} (\\select_{pizzeria='Dominos'} Serves);"
    )
    computed = _evaluate(querystring)
    assert len(computed) == 9


def test_person_join_rename():
    querystring = (
        "(\\rename_{A:*} Eats) \\join_{A.pizza = B.pizza} (\\rename_{B:*}"
        " Eats);"
    )
    computed = _evaluate(querystring)
    assert len(computed) == 94


def test_person_join_conjunction():
    querystring = (
        "(\\rename_{P:*} Person) \\join_{P.gender = Q.gender and P.age ="
        " Q.age} (\\rename_{Q:*} Person);"
    )
    computed = _evaluate(querystring)
    assert len(computed) == 9


def test_project_select_pizza_mushroom():
    querystring = "\\project_{pizza} \\select_{pizza='mushroom'} Eats;"
    computed = _evaluate(querystring)
    assert len(computed) == 1


def test_project_Person_gender():
    querystring = "\\project_{gender} Person;"
    result = ['{"Person.gender": "female"}', '{"Person.gender": "male"}']
    _check(querystring, result)


def test_project_person_join_eats():
    querystring = (
        "\\project_{Person.name, Eats.pizza} (Person \\join_{Person.name ="
        " Eats.name} Eats);"
    )
    computed = _evaluate(querystring)
    assert len(computed) == 20
