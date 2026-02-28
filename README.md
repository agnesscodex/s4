# s4

`s4` — CLI-утилита для S3/MinIO на Rust в стиле `mc`.

## Что реализовано

- Глобальные флаги: `-C/--config-dir`, `--json`, `--debug`, `--insecure`, `--resolve`, `--limit-upload`, `--limit-download`, `--custom-header/-H`.
- Управление alias: `alias set|ls|rm`.
- S3-команды: `ls`, `mb`, `rb`, `put`, `get`, `rm`, `stat`, `cat`, `cors`, `encrypt`, `event`, `legalhold`, `idp`, `ilm`, `replicate`, `sync`, `mirror` (alias к `sync`), `cp`, `mv`, `find`, `tree`, `head`, `pipe`, `ping`, `ready`.
- AWS SigV4 подпись запросов реализована через встроенный Python helper (`python3`) и HTTP-вызовы через `curl`.
- Для больших upload-ов (более 16 MiB) реализован multipart upload (`put`, `cp` local->s3, `sync/mirror`, `pipe`).
- Формат конфига: `~/.s4/config.toml`.

> Текущая сборка поддерживает только alias с `--path-style`.

## Быстрый старт

```bash
s4 alias set local http://127.0.0.1:9000 minio minio123 --path-style
s4 mb local/test-bucket
echo hello > hello.txt
s4 put hello.txt local/test-bucket/hello.txt
s4 cat local/test-bucket/hello.txt
s4 get local/test-bucket/hello.txt ./downloaded.txt
s4 stat local/test-bucket/hello.txt

# cors
s4 cors set local/test-bucket ./cors.xml
s4 cors get local/test-bucket
s4 cors remove local/test-bucket

# encryption
s4 encrypt set local/test-bucket ./encryption.xml
s4 encrypt info local/test-bucket
s4 encrypt clear local/test-bucket

# events
s4 event add local/test-bucket ./notification.xml
s4 event ls local/test-bucket
s4 event rm local/test-bucket --force

# legal hold (object-lock bucket required)
s4 mb --with-lock local/lock-bucket
s4 legalhold set local/lock-bucket/hello.txt
s4 legalhold info local/lock-bucket/hello.txt
s4 legalhold clear local/lock-bucket/hello.txt

# idp (placeholder in current build)
s4 idp openid
s4 idp ldap

# ilm (placeholder in current build)
s4 ilm rule
s4 ilm tier
s4 ilm restore

# replicate (placeholder in current build)
s4 replicate ls local/test-bucket
s4 replicate status local/test-bucket

s4 rm local/test-bucket/hello.txt
s4 rb local/test-bucket

# синхронизация (аналог mc mirror)
s4 sync local/source-bucket local/destination-bucket
# или в стиле mc
s4 mirror local/source-bucket local/destination-bucket

# копирование / перемещение
s4 cp ./local.txt local/test-bucket/local.txt
s4 cp local/test-bucket/local.txt ./local-copy.txt
s4 mv local/test-bucket/local.txt local/test-bucket/local-moved.txt

# поиск / дерево / head
s4 find local/test-bucket photos
s4 tree local/test-bucket
s4 head local/test-bucket/local-moved.txt 5

# загрузка из stdin
echo "stream data" | s4 pipe local/test-bucket/stdin.txt

# checks
s4 ping local
s4 ready local
```




> Примечание по лимитам: `--limit-upload` и `--limit-download` в текущей реализации передаются в `curl` как `--limit-rate` для upload/download-запросов соответственно.

## Mirror/sync флаги (совместимость с `mc mirror`)

Поддержано в `s4 mirror`/`s4 sync`:
- `--dry-run`
- `--remove`
- `--watch/-w` (polling-режим; интервал по умолчанию 2с, можно задать `S4_SYNC_WATCH_INTERVAL_SEC`)
- `--exclude <glob>` (можно указывать несколько раз; поддерживаются `*` и `?`)
- `--newer-than <duration>`
- `--older-than <duration>`
- `--overwrite` (принимается для совместимости; текущее поведение и так перезаписывает целевые объекты)

Пока **не реализовано** и возвращает явную ошибку `not implemented yet`:
- `--preserve/-a`, `--active-active`, `--disable-multipart`, `--exclude-bucket`,
  `--exclude-storageclass`, `--storage-class/--sc`, `--attr`,
  `--monitoring-address`, `--retry`, `--summary`, `--skip-errors`, `--max-workers`, `--checksum`,
  `--enc-c`, `--enc-kms`, `--enc-s3`, `--region` и другие специальные флаги `mc mirror`.

## Автоматизированный e2e

Для полного smoke/e2e прогона добавлен скрипт `scripts/e2e.sh`.

Пример запуска с вашим endpoint:

```bash
S4_E2E_ENDPOINT=http://63.141.251.44:10117 \
S4_E2E_ACCESS_KEY=my-secret-key_id \
S4_E2E_SECRET_KEY=my-secret-access-key \
S4_E2E_REGION=us-east-1 \
S4_E2E_PATH_STYLE=1 \
./scripts/e2e.sh
```

Скрипт прогоняет: `alias set/ls/rm`, `ls`, `mb`, `put`, `stat`, `cat`, `get`, `rm`, `rb` и проверяет целостность загруженного/скачанного содержимого через `cmp`.


## CI

В репозитории добавлен workflow `.github/workflows/ci.yml`, который запускается:

- на каждом `push`
- на `pull_request` в `main`

Пайплайн выполняет:

1. `cargo fmt --all --check`
2. `cargo test --all-targets`
3. Интеграционные S3-кейсы против локального MinIO (`scripts/ci_s3_cases.sh`), включая `sync` и `mirror` (mc-совместимый alias).
4. На `push` в `main` — интеграционные S3-кейсы против удалённого endpoint из GitHub Secrets.

### Секреты для remote S3 job

Добавьте в `Settings -> Secrets and variables -> Actions`:

- `S3_ENDPOINT` (например `63.141.251.44`)
- `S3_ENDPOINT_PORT` (например `10117`)
- `S3_ACCESS_KEY_ID` (например `my-secret-key_id`)
- `S3_SECRET_ACCESS_KEY` (например `my-secret-access-key`)


## Мониторинг CI без ручного кликанья

Добавлен скрипт `scripts/monitor_ci.sh`, который через GitHub API показывает статус и джобы workflow `CI` для текущего коммита/ветки.

Примеры:

```bash
# одноразовая проверка последнего CI для текущего коммита
GITHUB_TOKEN=... ./scripts/monitor_ci.sh --sha "$(git rev-parse HEAD)"

# ждать до завершения и вернуть exit code (0=success)
GITHUB_TOKEN=... ./scripts/monitor_ci.sh --wait --sha "$(git rev-parse HEAD)"

# ждать и автоматически rerun failed jobs
GITHUB_TOKEN=... ./scripts/monitor_ci.sh --wait --rerun-failed --sha "$(git rev-parse HEAD)"
```

Если `GITHUB_REPOSITORY` не задан, скрипт сам определит `owner/repo` из `git remote origin`.


## Покрытие команд mc vs s4

На текущем этапе в `s4` реализованы: `alias`, `ls`, `mb`, `rb`, `put`, `get`, `rm`, `stat`, `cat`, `cors`, `encrypt`, `event`, `legalhold`, `idp` (placeholder), `ilm` (placeholder), `replicate` (placeholder), `sync`, `mirror`, `cp`, `mv`, `find`, `tree`, `head`, `pipe`, `ping`, `ready`.

Остальные команды из полного списка `mc` (например `admin`, `anonymous`, `watch`, `sql`, `tag`, и т.д.) пока **не реализованы** и требуют отдельных итераций.


## Флаги: что есть и чего пока нет

Сейчас поддерживаются глобальные флаги: `-C/--config-dir`, `--json`, `--debug`, `--insecure`, `--resolve`, `--limit-upload`, `--limit-download`, `--custom-header/-H`, `-h/--help`, `-v/--version`.

Флаги из `mc`, которые пока не реализованы: `--quiet`, `--disable-pager`, `--no-color`, `--autocompletion` и другие.


> `idp openid|ldap` сейчас добавлены как placeholder-команды (возвращают `not implemented`) для совместимости CLI, полноценная интеграция с MinIO admin API будет отдельным этапом.


> `ilm rule|tier|restore` сейчас добавлены как placeholder-команды (возвращают `not implemented`) для совместимости CLI; полная реализация lifecycle/tier/restore будет отдельным этапом.


> `legalhold set|clear|info` поддерживаются для объектов в бакетах с object-lock (используйте `mb --with-lock`).


> `replicate add|update|list|status|resync|export|import|remove|backlog` сейчас добавлены как placeholder-команды (возвращают `not implemented`) для совместимости CLI; полноценная server-side replication конфигурация будет отдельным этапом.
