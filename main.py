"""
Задание 1:
Создать дата фрейм на основе запроса в котором содержаться
имена и фамилии студентов, название группы в которой студенты
учатся, оценки студентов и предметы, по которым студенты
получили оценки.

Задание 2:
Используя библиотеку pandas посчитать среднюю арифметическую оценку
по каждой группе студентов. Результат записать в новый DataFrame

Задание:
Используя библиотеку pandas необходимо получить цены
автомобилей из базы данных Auto и подсчитать суммарную
стоимость автомобилей, которые являются новыми, и которые
являются Б/У.
"""

import mysql.connector as connection
import pandas as pd

mydb = None
auto = None
try:
    mydb = connection.connect(
        host="127.0.0.1", user="root", passwd="1111", database="mydb"
    )
    print("Connection to mydb was successful")
    auto = connection.connect(
        host="127.0.0.1", user="root", passwd="1111", database="auto"
    )
    print("Connection to auto was successful")

    query = """select s.Name, s.Surname, g.Name as "Group", subj.Name as "Subject", ssg.Grade
            from students as s, mydb.groups as g, studentssubjectsgrades as ssg, subjects as subj
            where g.id = s.GroupId and ssg.StudentId = s.id and ssg.SubjectId = subj.id"""
    df_students = pd.read_sql(query, mydb)
    df_students_avg_grades = df_students.groupby(by='Group').agg(["mean"]).copy()
    print(df_students)
    print(df_students_avg_grades)
    mydb.close()

    query_auto = """
        select cs.state, sr.Price from sellrequest as sr, carstate as cs
        where sr.CarStateId = cs.id
    """

    df_sellrequest = pd.read_sql(query_auto, auto)
    print(df_sellrequest)
    grouped_by_region = df_sellrequest.groupby(by='state')
    print(grouped_by_region.agg(["sum"]))

    """
    Задание:
    Показать пользователей у которых были заявки или
    покупки и продемонстрировать максимальную цену
    заявки которую они подавали и минимальную цену
    покупки, которую они совершали.
    """

    query_auto = """
            (select user.name, user.surname,
            sellrequest.Price as "Buy price",
            null as "Sell price"
            from
            user
            left join cart on
            user.id = cart.UserId
            left join sellrequest on
            sellrequest.id = cart.RequestId)
            union
            (select user.Name, user.Surname,
            null as "Buy price",
            sellrequest.Price as "Sell price"
            from
            user
            left join sellrequest on
            user.id = sellrequest.UserId)
        """

    df_sellrequest_2 = pd.read_sql(query_auto, auto).dropna(thresh=3)
    print(df_sellrequest_2)
    print(f"Минимальная цена покупки - {df_sellrequest_2['Buy price'].min()}")
    print(f"Максимальная цена заявки - {df_sellrequest_2['Sell price'].max()}")
    auto.close()
except Exception as e:
    if mydb:
        mydb.close()
    if auto:
        auto.close()
    print(e)
