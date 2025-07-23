# `docker` tests of Transfer

This directory contains `docker`-based tests of Transfer.

## Run

In order to run these tests, the environment must be properly configured. In addition to a working `ya make`, the following are required:

1. `docker` installed. [Guide](https://docs.docker.com/engine/install/)
2. `docker-compose` installed. [Guide](https://docs.docker.com/compose/install/)
3. Docker must be logged into `registry.yandex.net`. Выполнить следующее:
   * Взять токен [отсюда](https://oauth.yandex-team.ru/authorize?response_type=token&client_id=12225edea41e4add87aaa4c4896431f1)
   * Запустить docker login registry.yandex.net, ввести:
       * login: `<yandex team login>`
       * password: `<token>`

4. Execute `sudo sysctl -w vm.max_map_count=262144` to set a system parameter to a proper value to run Elasticsearch **(!!!) ЭТА ШТУКА СБРАСЫВАЕТСЯ МЕЖДУ ПЕРЕЗАГРУЗКАМИ ЛИНУХА. ЕСЛИ КОНТЕЙНЕТ С ЭЛАСТИКОМ НЕ СТАРТУЕТ - ДЕЛО В ЭТОЙ ШНЯГЕ!!!**
5. Also should be given IDM role (Префиксы / data-transfer/ / contributor) for system (Docker-registry) - like that: https://idm.yandex-team.ru/roles/174720484?section=history (@ovandriyanov gave it to data-transfer team). How to check if permissions enough: "docker pull registry.yandex.net/data-transfer/tests/postgres-postgis-wal2json:13-3.3-2.5@sha256:5ab2b7b9f2392f0fa0e70726f94e0b44ce5cc370bfac56ac4b590f163a38e110"

After this, use `ya make -ttt .` to conduct tests or `ya make -AZ` to canonize.

---

траблшутинг:
* если постгрес не стартует и ругается на пароль - скорее всего на тачке поднят свой постгрес. Ошибка: `failed to connect to `host=localhost user=postgres database=postgres`: server error (FATAL: password authentication failed for user "postgres" (SQLSTATE 28P01))`
  * `sudo service postgresql stop` - остановит пг сервер
  * `netstat -a | grep post` или `pgrep postgres` - так можно проверить пг работает ли и слушает ли порт
* если контейнеры с эластиком фейлятся - `DockerComposeRecipeException: Has failed containers: elastic2elastic-dst, elastic2elastic-src, elastic2opensearch-src, elastic2pg-elastic-source-1, pg2elasticsearch-elastic-target-1` - это надо сделать `sudo sysctl -w vm.max_map_count=262144`. Ошибка:
      ```
      ERROR: [1] bootstrap checks failed. You must address the points described in the following [1] lines before starting Elasticsearch.
      bootstrap check failure [1] of [1]: max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]
      ERROR: Elasticsearch did not exit normally - check the logs at /usr/share/elasticsearch/logs/es-docker-cluster-1.log
      ```
