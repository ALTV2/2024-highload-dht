## Подготовим базу
* Создадим 260 SSTable скриптом put_request.lua
* 1. id: 1-1_000_000 
  2. value: 40-70 символов (символ -1 байт)
* Каждый SSTable весом в 0,5 МБ
* Пример запроса в wrk2:
wrk2-arm % ./wrk -d 5000 -t 1 -c 1 -R 25000 -L -s /Users/tveritinaleksandr/IdeaProjects/2024-highload-dht/src/main/java/ru/vk/itmo/test/tveritinalexandr/lua/put_rquest.lua http://localhost:8080

## PUT
* Для генерации запросов использовался wrk2 и скрипт put_request.lua. 
* Параметры запуска ```./wrk -d 20 -t 1 -c 1 -R 30500 -L -s``` - результаты представлены в файле step_1/wrk2/put_wrk2_output.
* Держит нагрузку в 30500 rps. При повшении числа запросов, они не успевают корректно обработаться.
* Гистограмма зависимости latency и запросов: step_1/histogram/step_1_put.png

## GET
Для генерации запросов использовался wrk2 и скрипт get_request.lua.
* Параметры запуска ```./wrk -d 20 -t 1 -c 1 -R 2300 -L -s``` - результаты представлены в файле step_1/wrk2/get_wrk2_output.
* Держит нагрузку в 30500 rps. При повшении числа запросов, они не успевают корректно обработаться.
* Гистограмма зависимости latency и запросов: step_1/histogram/step_1_get.png.

# Все флейм графы в дериктории profiling