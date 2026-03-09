# Пример использования рецепта шардированной MongoDB в тесте

Для описания шардированного кластера используется файл в формате `.yaml`,
который лежит в репозитори Arcadia. Чтобы добавить файл конфигурации
в тест, в файле `ya.make` данного рецепта необходимо:
1. Добавить файл конфигурации с помощью директивы `DATA`
2. Прописать файл конфигурации в переменную окружения `MONGO_SHARDED_CLUSTER_CONFIG`
3. Включить с помощью директивы `INCLUDE` рецепт шардированной MongoDB

Получаются следующие строчки (они есть в `ya.make`):
```
DATA(arcadia/transfer_manager/go/recipe/mongo/example/configs/auth.yaml)
ENV(MONGO_SHARDED_CLUSTER_CONFIG="transfer_manager/go/recipe/mongo/example/configs/auth.yaml")
INCLUDE(${ARCADIA_ROOT}/transfer_manager/go/recipe/mongo/recipe.inc)
```

При запуске теста будут опубликованы следующие переменные
для подключения к инстансу `mongos`, к которому уже
будут подключены все шарды данных:
* `MONGO_SHARDED_CLUSTER_HOST` -- mongos хост (всегда localhost)
* `MONGO_SHARDED_CLUSTER_PORT` -- mongos порт
* `MONGO_SHARDED_CLUSTER_USERNAME` -- имя пользователя
* `MONGO_SHARDED_CLUSTER_PASSWORD` -- пароль
* `MONGO_SHARDED_CLUSTER_AUTH_SOURCE` -- источник аутентификации

**Примечание:** если вы не указали в конфигурации `postSteps->createAdminUser`, то
вам бессмысленно использовать имя пользователя, пароль и источник аутентификации -- вы можете
подключиться к кластеру только в режиме "напрямую". Пример такого
подключения на языке Go может быть найден, например,
[здесь](https://a.yandex-team.ru/arc_vcs/transfer_manager/go/recipe/mongo/pkg/mongo_shareded_cluster/mongod.go?rev=aebb2179ea#L265):
в драйвере `MongoDB` для Go можно указать Direct подключение для работы с хостами.

Если есть необходимость развернуть два и более шардированных кластера, то
можно передать несколько конфигураций кластеров в переменную окружения `MONGO_SHARDED_CLUSTER_CONFIG`,
разделённых символом двоеточия. Например, запуск двух кластеров с описанием
`db1.yaml` и `db2.yaml`:
```
DEPENDS(transfer_manager/go/recipe/mongo)
DATA(arcadia/transfer_manager/go/tests/e2e/mongo2mongo/rps/replication_source_sharded/db1.yaml)
DATA(arcadia/transfer_manager/go/tests/e2e/mongo2mongo/rps/replication_source_sharded/db2.yaml)
ENV(MONGO_SHARDED_CLUSTER_CONFIG="transfer_manager/go/tests/e2e/mongo2mongo/rps/replication_source_sharded/db1.yaml:transfer_manager/go/tests/e2e/mongo2mongo/rps/replication_source_sharded/db2.yaml")
INCLUDE(${ARCADIA_ROOT}/transfer_manager/go/recipe/mongo/recipe.inc)
```

