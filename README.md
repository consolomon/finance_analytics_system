# Итоговый проект

### Ссылка на схему и таблицу в Vertica, где расположена итоговая витрина:
dwh.KOSYAK1998YANDEXRU__DWH.global_metrics

### Ответы на вопросы ревьювера

 - **Вопрос**: Супер, добавь только код создания проекций пожалуйста
 - **Ответ**: До сих пор до конца не понимаю, что имеется в виду. Если нужно просто в явном виде написать DDL проекций, которые создаются автоматически из моего DDL таблицы с сегментацией по датам, то ок, это я сделал. Если нужно как-то автоматически создавать проекции исходя из новых данных, то насколько я понял, такого функционала у вертики нет. Нужно писать конкретнее, а то формулировка " b. Создайте проекции по датам." не совсем понятна.
___
 - **Вопрос**: Опиши пожалуйста свою логику в ридмишке, я не очень понял, что ты решил сделать со вставкой в кучу разных таблиц из двух целевых
- **Ответ**: Сделал dds по схеме data-vault как бы на перспективу, для упрощения расширения структуры данных и скомбинировал с CDC из 5го спринта.
___
 - **Вопрос**: В коде отсутствует механизм обработки и логирования ошибок. Это может усложнить выявление и исправление проблем в будущем
 - **Ответ**: Все транзакции в БД проходят через vertica_operator() который обрабатывает эксепшены, но согласен, стоит общий load_items() и get_last_date() тоже завернуть. Сделал.
___
 - **Вопрос**: Надо бы вынести отсюда и потом вызывать, чтобы не светить паролем.
 - **Ответ**: Да, до последнего оставлял на всякий случай и забыл убрать. Исправил.

    


### Структура репозитория
Файлы в репозитории будут использоваться для проверки и обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре: так будет проще соотнести задания с решениями.

Внутри `src` расположены папки:
- `/src/dags` - вложите в эту папку код DAG, который поставляет данные из источника в хранилище. Назовите DAG `1_data_import.py`. Также разместите здесь DAG, который обновляет витрины данных. Назовите DAG `2_datamart_update.py`.
- `/src/sql` - сюда вложите SQL-запрос формирования таблиц в `STAGING`- и `DWH`-слоях, а также скрипт подготовки данных для итоговой витрины.
- `/src/py` - если источником вы выберете Kafka, то в этой папке разместите код запуска генерации и чтения данных в топик.
- `/src/img` - здесь разместите скриншот реализованного над витриной дашборда.
