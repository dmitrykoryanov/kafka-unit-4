# Захват изменений из PostgreSQL в Kafka с помощью Debezium

P.S. Пункт задания "**~~5. Получите данные из топиков Apache Kafka, используя код на выбранном вами языке программирования, и выведите их в терминал.~~**" не делал - проект полностью инфраструктурный, не хотелось его мешать со Spring-м, всё остальное сделано.  

Решение отслеживает изменения в таблицах `users` и `orders` базы PostgreSQL и передаёт их в Kafka. Коннектор Debezium читает журнал PostgreSQL (WAL) через логическую репликацию. Каждое изменение строки (вставка, обновление, удаление) он пишет отдельным сообщением в топики `customers.public.users` и `customers.public.orders`.

Сообщения сериализуются в Avro, схемы хранятся в Schema Registry. Весь стенд запускается одной командой `docker compose up`, таблицы и коннектор создаются автоматически.

### Что нужно

- Docker с Docker Compose v2.

## Запуск

`docker compose up -d` возвращает управление примерно через 1–1,5 минуты: Compose ждёт, пока Kafka Connect будет готов, и только потом запускает `connector-init`.

### Команды

```bash
docker compose up -d --build        # собрать образы и запустить стенд
docker compose logs connector-init  # результат создания коннектора: "connector pg-connector: HTTP 201"
docker compose down                 # остановить стенд, данные остаются в томах
docker compose down -v              # остановить стенд и удалить все данные Kafka, ZooKeeper и PostgreSQL
```

### Что происходит при запуске

1. ZooKeeper, три брокера Kafka и Schema Registry поднимают кластер.
2. PostgreSQL при первом запуске, пока том с данными пустой, выполняет `postgres/init.sql`: создаёт таблицы `users` и `orders` и заполняет их (4 пользователя, 5 заказов). После этого проверка `pg_isready` помечает контейнер как готовый (healthy).
3. Kafka Connect загружает плагины и создаёт свои служебные топики. Когда его REST API начинает отвечать, контейнер становится готовым.
4. `connector-init` отправляет `postgres/connector.json` в REST API Kafka Connect и завершается. Статус `Exited (0)` у него — это нормально.
5. Коннектор `pg-connector` создаёт в PostgreSQL слот репликации `debezium` и публикацию `dbz_publication`. Затем он читает текущее содержимое обеих таблиц (начальный снимок) и дальше передаёт в Kafka каждое изменение.

### Как применить изменения

| Что изменили | Как применить |
|---|---|
| `postgres/connector.json` | `docker compose up -d`: `connector-init` запустится снова и обновит коннектор |
| `postgres/init.sql` | `docker compose down -v`, затем `docker compose up -d`: скрипт выполняется только на пустой базе |
| `postgres/custom-config.conf` | `docker compose restart postgres` |
| плагины в `confluent-hub-components/` | `docker compose restart kafka-connect` |

Не удаляйте том PostgreSQL отдельно от томов Kafka. Позиция, до которой Debezium уже прочитал журнал PostgreSQL, хранится в Kafka. С новой базой она теряет смысл: коннектор не перечитает таблицы заново. Поэтому сбрасывайте всё вместе командой `docker compose down -v`.

## Компоненты

| Сервис | Образ | Назначение | Адрес с хоста |
|---|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.7.1` | Хранит метаданные кластера Kafka (кластер работает в режиме ZooKeeper) | `localhost:22181` |
| `kafka-0`, `kafka-1`, `kafka-2` | `confluentinc/cp-kafka:7.7.1` | Три брокера Kafka, хранят топики. Внутри сети Docker доступны как `kafka-N:9092` | `127.0.0.1:9094`, `:9095`, `:9096` |
| `schema-registry` | `confluentinc/cp-schema-registry:7.7.1` | Хранит Avro-схемы ключей и значений сообщений | http://localhost:8081 |
| `postgres` | `debezium/postgres:16` | База `customers` с таблицами `users` и `orders`. Настройки логической репликации в `postgres/custom-config.conf`, данные в томе `postgres_data` | `localhost:5432`, пользователь `postgres-user`, пароль `postgres-pw` |
| `kafka-connect` | собирается из `kafka-connect/Dockerfile`: `cp-kafka-connect:7.7.1` и JMX Exporter | Запускает коннектор `pg-connector`. Плагины берёт из `confluent-hub-components/`. Конфигурации, позиции чтения и статусы коннекторов хранит в топиках `connect-config-storage`, `connect-offset-storage` и `connect-status-storage` | REST API http://localhost:8083, метрики http://localhost:9876/metrics |
| `connector-init` | `confluentinc/cp-kafka-connect:7.7.1` | Одноразовый контейнер. Ждёт готовности `kafka-connect` и `postgres`, создаёт или обновляет коннектор по `postgres/connector.json` и завершается | — |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.0` | Веб-интерфейс: топики и сообщения, схемы в Schema Registry, статус коннекторов | http://localhost:8085 |
| `prometheus` | `prom/prometheus:v2.30.3` | Каждые 5 секунд собирает метрики Kafka Connect и Debezium (конфиг `prometheus/prometheus.yml`) | http://localhost:9090 |
| `grafana` | собирается из `grafana/` (Grafana 8.1.6) | Дашборды Debezium PostgreSQL CDC и Kafka Connect Overview. Логин `admin`, пароль `admin` | http://localhost:3000 |

### Как компоненты связаны

```text
postgres (users, orders)
   │  логическая репликация: слот debezium, публикация dbz_publication (pgoutput)
   ▼
kafka-connect: pg-connector (Debezium) ──► schema-registry (Avro-схемы)
   │      ▲
   │      └── connector-init: при запуске отправляет postgres/connector.json
   ▼
kafka-0, kafka-1, kafka-2 ◄── zookeeper
   топики customers.public.users и customers.public.orders
   ▲
   └── kafka-ui: читает топики, схемы и статус коннектора

kafka-connect :9876 (метрики JMX) ──► prometheus ──► grafana
```

- **PostgreSQL → Debezium.** Debezium подключается к PostgreSQL как клиент логической репликации. PostgreSQL отдаёт изменения из журнала через слот `debezium` и публикацию `dbz_publication`, в которую входят только `users` и `orders`.
- **Debezium → Schema Registry → Kafka.** Kafka Connect превращает каждое изменение в сообщение, регистрирует его Avro-схему в Schema Registry и пишет сообщение в брокеры. В самом сообщении хранится только номер схемы, поэтому тем, кто читает топики, например Kafka UI, нужен Schema Registry.
- **Состояние Kafka Connect.** Kafka Connect хранит своё состояние в самой Kafka: конфигурацию коннектора, его статус и позицию в журнале PostgreSQL (LSN), до которой изменения уже переданы. После перезапуска коннектор продолжает с этой позиции и ничего не теряет.
- **Метрики.** Метрики Kafka Connect идут по цепочке JMX → JMX Exporter (порт 9876) → Prometheus → Grafana.

## Настройки Debezium Connector

Настройки лежат в файле `postgres/connector.json`. Формат `{"name": ..., "config": {...}}` такой же, как для `POST /connectors`. `connector-init` сам вынимает из файла содержимое `config` и отправляет его запросом `PUT /connectors/pg-connector/config`. Такой запрос создаёт коннектор, если его нет, и обновляет, если он уже есть.

| Параметр | Значение | Что делает |
|---|---|---|
| `name` | `pg-connector` | Имя коннектора в Kafka Connect и в адресах REST API |
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | Коннектор Debezium, который читает изменения из PostgreSQL (source) |
| `database.hostname`, `database.port` | `postgres`, `5432` | Адрес базы внутри сети Docker |
| `database.user`, `database.password` | `postgres-user`, `postgres-pw` | Суперпользователь базы. Debezium нужны права на репликацию и на создание публикации |
| `database.dbname` | `customers` | База, из которой читаются изменения |
| `plugin.name` | `pgoutput` | Плагин логического декодирования, встроенный в PostgreSQL 10+. Без этой строки Debezium использует `decoderbufs`, с которым публикации не применяются и PostgreSQL отдаёт изменения всех таблиц |
| `table.include.list` | `public.users,public.orders` | Отслеживаемые таблицы в формате `схема.таблица` через запятую |
| `publication.autocreate.mode` | `filtered` | Debezium создаёт публикацию `dbz_publication` только для таблиц из `table.include.list`, поэтому PostgreSQL вообще не передаёт изменения других таблиц. По умолчанию (`all_tables`) в публикацию попадают все таблицы, а лишние отбрасывает сам коннектор |
| `topic.prefix` | `customers` | Префикс имён топиков по шаблону `<префикс>.<схема>.<таблица>`: получаются `customers.public.users` и `customers.public.orders`. Под этим же именем Kafka Connect хранит позицию чтения журнала. Если сменить префикс, коннектор заново выгрузит таблицы, уже в новые топики |
| `transforms`, `transforms.unwrap.type` | `unwrap`, `io.debezium.transforms.ExtractNewRecordState` | Преобразование, которое упрощает сообщение. Без него в сообщении есть служебные поля `before`, `after`, `source` и `op`. С ним остаётся только строка таблицы после изменения и поле `__deleted` |
| `transforms.unwrap.delete.tombstone.handling.mode` | `rewrite-with-tombstone` | Удаление строки передаётся двумя сообщениями. Первое — строка с `__deleted = "true"`. Второе — tombstone: сообщение с тем же ключом и пустым значением, по которому Kafka при сжатии топика (compaction) удаляет этот ключ |
| `topic.creation.default.replication.factor`, `topic.creation.default.partitions` | `-1`, `-1` | Kafka Connect создаёт топики сам, с настройками брокера по умолчанию. В этом кластере это 1 партиция и фактор репликации 1 |
| `skipped.operations` | `none` | Передавать все операции. По умолчанию (`t`) Debezium пропускает `TRUNCATE` |

Параметры, которые в файле не заданы, но влияют на работу:

| Параметр | Значение по умолчанию | Что делает |
|---|---|---|
| `snapshot.mode` | `initial` | При первом запуске коннектор читает текущее содержимое таблиц (снимок), затем переходит к потоку изменений. При последующих запусках он продолжает с сохранённой позиции |
| `slot.name`, `publication.name` | `debezium`, `dbz_publication` | Имена слота репликации и публикации в PostgreSQL |
| `slot.drop.on.stop` | `false` | Если удалить коннектор, слот репликации останется в базе. В `postgres/custom-config.conf` разрешено не больше 4 слотов. Лишний слот удаляется запросом `SELECT pg_drop_replication_slot('debezium');` |
| `key.converter`, `value.converter` | `AvroConverter`, из настроек Kafka Connect в `docker-compose.yaml` | Ключи и значения сериализуются в Avro. Схемы регистрируются в Schema Registry под именами `<топик>-key` и `<топик>-value` |

При удалении строки в сообщении заполнен только первичный ключ: остальные поля пустые (`null`), а поля с датой и временем равны 0. Так происходит потому, что PostgreSQL по умолчанию записывает в журнал только ключ удалённой строки. Если нужна вся строка, добавьте в `postgres/init.sql` команды `ALTER TABLE users REPLICA IDENTITY FULL;` и `ALTER TABLE orders REPLICA IDENTITY FULL;`.

## Мониторинг

### Сбор метрик в Prometheus

Kafka Connect и Debezium публикуют свои метрики через JMX (MBean внутри JVM). В JVM Kafka Connect запущен JMX Prometheus Exporter (параметр `-javaagent` в `KAFKA_OPTS` в `docker-compose.yaml`). Он превращает MBean в метрики Prometheus по правилам из `kafka-connect/kafka-connect.yml` и отдаёт их по адресу http://localhost:9876/metrics. Prometheus опрашивает этот адрес каждые 5 секунд: это задание `kafka-connect` в `prometheus/prometheus.yml`.

| Группа | Откуда берётся (JMX) | Примеры метрик в Prometheus |
|---|---|---|
| Воркер Kafka Connect | `kafka.connect:type=connect-worker-metrics`, `type=app-info` | `kafka_connect_worker_connector_count`, `kafka_connect_app_info` |
| Коннектор и его задачи | `kafka.connect:type=connector-metrics`, `connector-task-metrics`, `source-task-metrics`, `task-error-metrics` | `kafka_connect_connector_info{status="running"}`, `kafka_connect_worker_connector_running_task_count`, `kafka_connect_source_task_source_record_write_total`, `kafka_connect_task_error_total_record_failures` |
| Продюсер коннектора | `kafka.producer:client-id=connector-producer-pg-connector-0` | `kafka_producer_topic_record_send_total{topic="customers.public.users"}`, `kafka_producer_request_latency_avg` |
| Debezium | `debezium.postgres:type=connector-metrics,context=snapshot` и `context=streaming` | `debezium_metrics_connected`, `debezium_metrics_milli_seconds_behind_source`, `debezium_metrics_total_number_of_create_events_seen`, `debezium_metrics_rows_scanned{table="public.users"}` |

- Состояние сбора видно на странице http://localhost:9090/targets: задание `kafka-connect` должно быть в состоянии **UP**.
- Изменения в `prometheus/prometheus.yml` применяются без перезапуска: `curl -X POST http://localhost:9090/-/reload`.
- Правила `kafka-connect/kafka-connect.yml` копируются в образ Kafka Connect, поэтому после их изменения нужна пересборка: `docker compose up -d --build kafka-connect`.
- Счётчики событий по отдельным таблицам Debezium заполняет только при внутренней настройке `internal.advanced.metrics.enable`. Поэтому разбивка по таблицам на дашборде берётся из метрик продюсера по топикам: у каждой таблицы свой топик.
- У Prometheus нет тома для данных, поэтому история метрик пропадает при пересоздании его контейнера.

### Дашборды Grafana

Дашборды загружаются в Grafana из папки `grafana/dashboards/` при сборке образа. После их изменения нужна пересборка: `docker compose up -d --build grafana`. Оба дашборда обновляются каждые 10 секунд и по умолчанию показывают последние 15 минут.

- **Debezium PostgreSQL CDC** (http://localhost:3000/d/debezium-postgres-cdc) — основной дашборд этого решения. Вверху можно выбрать коннектор (показываются только коннекторы Debezium PostgreSQL) и его `topic.prefix`.
- **Kafka Connect Overview** (http://localhost:3000/d/kafka-connect-overview) — общий обзор воркера Kafka Connect и всех его коннекторов: состояние задач, таблицы воркера и коннекторов, скорость чтения и записи, метрики продюсера.

Блок «Работоспособность» дашборда Debezium PostgreSQL CDC:

| Панель | Что показывает | Нормальное значение |
|---|---|---|
| Коннектор | Состояние коннектора в Kafka Connect | RUNNING |
| Задачи в работе, Упавшие задачи | Сколько задач коннектора в состоянии RUNNING и FAILED | 1 и 0 |
| Связь с PostgreSQL | Подключён ли Debezium к журналу PostgreSQL | Подключён |
| Задержка от источника | Сколько прошло от изменения в базе до его обработки в Debezium, по последнему событию | Миллисекунды. Сразу после запуска — «Событий не было» |
| Ошибки | Сумма с момента запуска: ошибки обработки записей в Kafka Connect, ошибки отправки в Kafka и ошибочные события Debezium | 0 |
| Начальный снимок | Состояние начального снимка таблиц | «Завершён» после запуска с нуля, «Не требовался» после перезапуска |
| С последнего события | Сколько времени прошло с последнего изменения в таблицах | Растёт, пока таблицы не меняются, — это не ошибка |
| Изменений с запуска, Записано в Kafka с запуска | Сколько изменений Debezium прочитал из журнала и сколько записей попало в Kafka | Растут при изменениях в таблицах |
| Очередь Debezium | Заполненность внутренней очереди между чтением журнала и отправкой в Kafka | Около 0 %. Близко к 100 % — Kafka не успевает принимать изменения |
| Доля времени в работе | Доля времени, которую задача провела в состоянии RUNNING | 100 % |
| Состояние задач, Задержка от источника, Ошибки и повторы | Те же показатели во времени | Нет провалов и всплесков ошибок |

Блок «Передача данных»:

| Панель | Что показывает |
|---|---|
| Записи: прочитано и записано в Kafka | Сколько записей в секунду задача получает от Debezium и сколько записывает в Kafka |
| Изменения по типам операций | Сколько INSERT, UPDATE и DELETE Debezium прочитал из журнала за последнюю минуту |
| Сообщения по топикам, Объём по топикам | Скорость отправки в каждый топик (то есть по каждой таблице), в сообщениях и байтах в секунду |
| Время обработки | Среднее время получения пачки записей от Debezium и сохранения позиции чтения. Без изменений в таблицах получение пачки занимает около 500 мс: столько Debezium ждёт новые события |
| Отправка в Kafka: задержки | Время ответа брокера на запрос продюсера и время ожидания записи в буфере продюсера |
| Отправлено по топикам с запуска, Начальный снимок: строк по таблицам | Сколько сообщений отправлено в каждый топик и сколько строк каждой таблицы прочитано при начальном снимке |

## Ручная проверка

Откройте Kafka UI по адресу http://localhost:8085 и в меню слева выберите кластер `zookeeper-cluster`.

### 1. Раздел Kafka Connect: создан ли коннектор

В списке должен быть `pg-connector` со статусом **RUNNING**. В нём 1 задача, упавших задач 0, топики `customers.public.users` и `customers.public.orders`. Если нажать на коннектор, будут видны состояние задачи и его конфигурация, такая же, как в `postgres/connector.json`.

То же самое можно проверить из терминала (в PowerShell вместо `curl` пишите `curl.exe`):

```bash
docker compose logs connector-init                            # "connector pg-connector: HTTP 201"
curl -s http://localhost:8083/connectors/pg-connector/status  # connector и tasks в состоянии RUNNING
```

### 2. Раздел Topics: дошли ли данные

- В `customers.public.users` должно быть 4 сообщения, в `customers.public.orders` 5. Это строки из `init.sql`, которые Debezium прочитал при старте.
- Откройте топик и перейдите на вкладку **Messages**. Значение сообщения показывается как JSON, например `{"id":1,"name":{"string":"John Doe"},...,"__deleted":{"string":"false"}}`.
- Ключ сообщения поначалу выглядит как непонятные символы вроде `\u0000\u0000\u0000\u0000\u0001\u0002`. Это не ошибка: ключ тоже закодирован в Avro, а Kafka UI по умолчанию читает его как обычную строку. Выберите в поле **Key Serde** значение `SchemaRegistry`, и ключ отобразится как `{"id":1}`.

### 3. Раздел Schema Registry: зарегистрированы ли схемы

Там должно быть четыре схемы: `customers.public.users-key`, `customers.public.users-value`, `customers.public.orders-key` и `customers.public.orders-value`.

### 4. Отслеживание изменений

Пункты 1–3 показывают только, что коннектор прочитал таблицы при старте. Чтобы убедиться, что он ловит и новые изменения, измените данные в базе и посмотрите, появились ли сообщения:

```bash
docker exec -it postgres psql -U postgres-user -d customers -c "INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com');"
docker exec -it postgres psql -U postgres-user -d customers -c "UPDATE users SET email = 'test2@example.com' WHERE name = 'Test User';"
docker exec -it postgres psql -U postgres-user -d customers -c "DELETE FROM users WHERE name = 'Test User';"
```

После этого в `customers.public.users` появятся 4 новых сообщения:

- вставка с `"__deleted": "false"`;
- обновление с новым email;
- удаление: сообщение с `"__deleted": "true"` и сразу за ним сообщение с пустым значением (tombstone).

Удалять лучше именно тестового пользователя. Пользователей 1–4 удалить не получится: на них ссылаются заказы в `orders`, и PostgreSQL выдаст ошибку внешнего ключа.

### 5. Мониторинг: Prometheus и Grafana

1. Откройте http://localhost:9090/targets. Задание `kafka-connect` должно быть в состоянии **UP**.
2. Откройте дашборд http://localhost:3000/d/debezium-postgres-cdc (логин `admin`, пароль `admin`). После запуска с нуля на нём должно быть: «Коннектор» — RUNNING, «Связь с PostgreSQL» — Подключён, «Начальный снимок» — Завершён, на панели «Начальный снимок: строк по таблицам» — `public.users` 4 и `public.orders` 5.
3. Выполните команды из пункта 4. В течение 10–20 секунд «Изменений с запуска» вырастет на 3, «Записано в Kafka с запуска» — на 4 (удаление даёт два сообщения). На графике «Изменения по типам операций» появятся INSERT, UPDATE и DELETE, а «Задержка от источника» покажет время в миллисекундах.

Тестовые изменения останутся в базе и в топиках. Чтобы вернуться к чистому состоянию, выполните `docker compose down -v`, затем `docker compose up -d`.

## Оптимизация пропускной способности JDBC Source Connector

Цель: максимизировать Source Record Write rate коннектора `postgres-source` (PostgreSQL → Kafka), меняя параметры продюсера `batch.size`, `linger.ms`, `compression.type` и `buffer.memory`.

Это отчёт о предыдущем задании на этом же стенде. Коннектора `postgres-source` и плагина JDBC в текущей конфигурации нет, а таблица `users` в тех экспериментах была другой: `id`, `name`, `updated_at`, `private_info`.

### Результаты

Каждый эксперимент: `TRUNCATE users;` → новые `producer.override.*` в конфигурации коннектора → запись 9 000 000 строк в PostgreSQL.

| Эксперимент | batch.size | linger.ms | compression.type | buffer.memory | Source Record Write Rate (кops/sec) | Пик на графике Grafana (кops/sec) | Записей в запросе | Время передачи 9 млн строк, с |
|---|---|---|---|---|---|---|---|---|
| 1 | 100 | 0 | none | 33554432 | 3.3 ¹ | 4.1 | 1 | ≈ 2700–3300 ¹ |
| 2 | 11600 | 0 | none | 33554432 | 200.9 | 158.7 | 134 | 44.8 |
| 3 | 116000 | 0 | none | 33554432 | 201.1 | 163.6 | 133 | 44.8 |
| **4** | **116000** | **10** | **none** | **33554432** | **238.3** | 156.5 | 2429 | **37.8** |
| 5 | 116000 | 10 | lz4 | 33554432 | 191.3 | 153.5 | 2072 | 47.1 |
| 6 | 116000 | 10 | zstd | 33554432 | 164.5 | 156.1 | 1699 | 54.7 |
| 7 | 116000 | 10 | none | 67108864 | 214.1 | 156.5 | 2141 | 42.0 |

¹ Прогон остановлен через 5 минут: за это время передано 0.98 млн строк. Все 9 млн строк передавались бы около 45–55 минут.

**Лучшая конфигурация — эксперимент 4**:

```json
"producer.override.batch.size": "116000",
"producer.override.linger.ms": "10",
"producer.override.compression.type": "none",
"producer.override.buffer.memory": "33554432"
```

Ускорение относительно эксперимента 1 — примерно в 70 раз (с 3.3 до 238 тыс. записей/с).

### Выводы

1. **`batch.size` — главный параметр.** Kafka Connect создаёт продюсер с `acks=all` и `max.in.flight.requests.per.connection=1`. Продюсер не отправляет следующий запрос, пока не получит ответ на предыдущий. При `batch.size=100` батч меньше одной записи (Record Size Average ≈ 117 байт), поэтому в каждом запросе ровно 1 запись. Скорость ограничена задержкой запроса: около 0.35 мс, то есть примерно 3 тыс. записей/с. Продюсер не успевает отправлять, и буфер на 32 МБ заполняется полностью: свободными остаются 27 байт, а `bufferpool-wait-ratio` = 0.9. Записи ждут отправки в среднем 108 с.
2. **`batch.size` по формуле** = Record Size Average × `batch.max.rows` = 116 × 100 = **11600** байт. В одном запросе уже около 134 записей, скорость выросла в 61 раз.
3. **При `linger.ms=0` увеличивать `batch.size` дальше бесполезно** (эксперимент 3). Продюсер отправляет батч сразу, как только приходит ответ на предыдущий запрос. В батч попадает только то, что накопилось за один запрос: в среднем 5.4 КБ из доступных 116 КБ.
4. **`linger.ms=10` даёт +19 %** (эксперимент 4). Продюсер ждёт, пока батч наполнится: в среднем 98 КБ, около 2400 записей. Число запросов за прогон падает с 67 691 до 3 706, и накладных расходов на запрос в пересчёте на одну запись становится в 18 раз меньше.
5. **Сжатие на локальном стенде замедляет передачу:** `lz4` на 20 %, `zstd` на 31 %. Данные сжимаются в 3.3 раза (`lz4`) и в 11 раз (`zstd`). Но сжатие выполняется в том же потоке, который читает строки из базы и готовит записи, а сеть между контейнерами узким местом не является. Сжатие окупится при медленной сети или удалённом кластере.
6. **`buffer.memory` скорость не ограничивает.** Начиная с эксперимента 3, `bufferpool-wait-ratio` = 0, а занято всего 113–227 КБ буфера (1–2 батча) из 32 или 64 МБ. Разница между экспериментами 7 и 4 (214 и 238 тыс./с) — разброс между прогонами. В эксперименте 7 Kafka отвечала медленнее (задержка запроса 2.0 мс против 1.2 мс), и вставка в PostgreSQL тоже шла дольше (27.6 с против 18.8 с). Разброс между одинаковыми прогонами на локальном стенде доходит примерно до 10 %.

### Методика

- **Не менялись:** `mode=timestamp` (колонка `updated_at`), `batch.max.rows=100`, `poll.interval.ms=200`, преобразование `MaskField` для `private_info`, конвертер Avro через Schema Registry. Топик `postgresql-jdbc-bulk-users`: 1 партиция, фактор репликации 1.
- **Порядок эксперимента:** остановка коннектора (`PUT /connectors/postgres-source/stop`), чтобы его открытый курсор не блокировал `TRUNCATE` → `TRUNCATE users;` → новые `producer.override.*` → запуск коннектора (`PUT /connectors/postgres-source/resume`). Затем проверка по логам Connect (`ProducerConfig values`), что продюсер получил новые значения, и вставка 9 000 000 строк одной транзакцией (`INSERT ... SELECT FROM generate_series`). Строки во всех прогонах одинаковые, поэтому размер записи не меняется.
- **Source Record Write Rate** = 9 000 000 / время между первой и последней записанной записью. Счётчик `source-record-write-total` снимался с JMX exporter (`:9876/metrics`) каждую секунду.
- **Записей в запросе** = 9 000 000 / число запросов продюсера (`kafka_producer_node_request_total`).
