
# Домашнее задание 2

Изучаем группировки, сложные запросы, CTE


# Задание 1

15 баллов

Возьмем датасет такси Нью-Йорка: 

https://disk.yandex.ru/d/ESzSFuc4UdK88Q

Поэтапно превратим его в SQL-таблицу.

Первым делом создадим таблицу с текстовыми полями. По полю на каждый столбец.

Прочитаем с помощью запроса COPY.

Создадим новые поля с адекватными типами, старые строковые удалим.

VendorID -> целочисленное

tpep_pickup_datetime, tpep_dropoff_datetime -> timestamp

passenger_count -> целочисленное

trip_distance -> вещественное

RatecodeID -> целочисленное

store_and_fwd_flag -> односимвольное или enum

PULocationID,DOLocationID,payment_type,fare_amount,extra - целочисленные

mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount - с фиксированной точкой

Оформим последовательность действий в виде процедуры на PL/pgSQL.

Процедура должна предполагать, что файл скачан и находится где то на сервере. И принимать имя файла в качестве параметра.

# Задание 2

15 баллов

Найдем для каждого перевозчика

* среднюю длину поездки

* дисперсию стоимости поездки

* количество поездок, начавшихся с 6 утра до 6 вечера

* сколько раз его водитель встречал Новый год в поездке

* максимальную стоимость поездки в среднем на пассажира

Добьемся того, чтобы ответы на данные вопросы были взяты с одного состояния базы.

Это можно сделать с помощью одного сложного запроса или несколькими с правильным,
но не избыточным уровнем изоляции.


# Задание 3

20 баллов

Найдем для каждой поездки

* долю чаевых в расходах, понесенных пассажиром

* разницу между долей чаевых этой поездки и средней долей по всем поездкам в датасете

* разницу между долей чаевых этой поездки и средней долей по всем поездкам данного перевозчика

* разницу между долей чаевых этой поездки и средней долей по всем более дальним поездкам, начавшимся в тот же день 

* ранг данной поездки по средней скорости у данного переводчика

Добейтесь того, чтобы ответы на данные вопросы были взяты с одного состояния базы.

Это можно сделать с помощью одного сложного запроса или несколькими с правильным,
но не избыточным уровнем изоляции.



## Задание 4

30 баллов

```
CREATE TABLE nums(a INT CHECK (a > 0), b INT CHECK(b > 0));
```

Напишем CTE-запрос, который реализует массовое применение алгоритма Евклида и вернет ответ с тремя полями:
a, b, gcd.

Если одно из чисел 0, то gcd должно содержать другое число.


## Задание 5

20 баллов

```
CREATE TABLE staff(emp_id INT PRIMARY KEY, reports_to INT REFERENCES staff(emp_id));
```

Дана таблица сотрудников.

* emp_id - id сотрудника

* reports_to - id менеджера 

У генерального директора reports_to - NULL.

Гарантируется, что в таблице один и только один генеральный директор.

Напишем запрос, который вернет две колонки: emp_id и level - количество начальников над данным сотрудником.

Для генерального директора нужно вернуть 0, для его непосредственных подчиненных - 1 и т.д.

Есть проблема в том, что в таблице не исключено наличие циклов.

Будем считать, что в оргазации вряд ли будет более двадцати уровней иерархии.

Если для каких-то пользователей не дошли до генерального директора за двадцать шагов, остановимся и в ответе породим -1
в колонке level.

Для сотрудников, которые не являются частью цикла, должно быть порождено количество начальников над данным сотрудником.

Ожидается решение через одно CTE.

Решения типа "процедура с циклом" не проверяются и получают 0 баллов. Только CTE, только хардкор.


## Бонусное задание

30 баллов

Решите задачу определения циклов в задании 5 без закладки на фиксированный лимит.

Помните о том, что цикл - это не только "круг". Это может быть что-то типа a -> b -> c -> b. 

