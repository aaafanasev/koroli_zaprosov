# koroli_zaprosov
# Репозиторий для проекта хакатона
# Данные забираются с s3 через даг на airflow и прогружаются в таблицу stg.events 
# После этого у нас имееются 2 вьюхи на основании которых построен дэшборды. Задача 2 подмножество событий payments задачи 1, поэтому дополнительно не стали создавать ничего.
# Ссылка на metabase http://158.160.62.179:3000/metabase/dashboard/1-hackathon