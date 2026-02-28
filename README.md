# s4

`s4` — CLI-утилита для S3/MinIO на Rust в стиле `mc`.

## Что реализовано

- Глобальные флаги: `-C/--config-dir`, `--json`, `--debug`, `--insecure`.
- Управление alias: `alias set|ls|rm`.
- S3-команды: `ls`, `mb`, `rb`, `put`, `get`, `rm`, `stat`, `cat`, `sync`, `mirror` (alias к `sync`), `cp`, `mv`, `find`, `tree`, `head`, `pipe`, `ping`, `ready`.
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

На текущем этапе в `s4` реализованы: `alias`, `ls`, `mb`, `rb`, `put`, `get`, `rm`, `stat`, `cat`, `sync`, `mirror`, `cp`, `mv`, `find`, `tree`, `head`, `pipe`, `ping`, `ready`.

Остальные команды из полного списка `mc` (например `admin`, `anonymous`, `watch`, `replicate`, `sql`, `tag`, и т.д.) пока **не реализованы** и требуют отдельных итераций.


## Флаги: что есть и чего пока нет

Сейчас поддерживаются глобальные флаги: `-C/--config-dir`, `--json`, `--debug`, `--insecure`, `-h/--help`, `-v/--version`.

Флаги из `mc`, которые пока не реализованы: `--resolve`, `--limit-upload`, `--limit-download`, `--custom-header/-H`, `--quiet`, `--disable-pager`, `--no-color`, `--autocompletion` и другие.
