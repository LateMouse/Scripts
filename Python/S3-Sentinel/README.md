# S3-Sentimel

Cкрипт для активного мониторинга S3-совместимого хранилища через S3 API.
Предназначен для запуска из командной строки, систем мониторинга (в частности, Zabbix) или ручной диагностики.  

## Что делает
Последовательно выполняет несколько проверок:
1. Проверяет доступность S3 API.
2. Проверяет, что endpoint действительно отвечает как S3 API.
3. Проверяет доступность указанного бакета.
4. Измеряет задержку bucket-level операции.
5. Опционально измеряет скорость скачивания тестового объекта.
6. Опционально измеряет скорость загрузки тестового файла.
7. Опционально удаляет загруженные тестовые объекты.
8. Возвращает результат в JSON и выставляет exit code, удобный для интеграции с Zabbix.

## Для каких задач
- Мониторинг S3-compatible storage из Zabbix через external check или UserParameter.
- Проверка работоспособности MinIO, Ceph RGW и других S3-совместимых решений.
- Контроль не только доступности API, но и реального выполнения операций чтения/записи.
- Оценка деградации производительности upload/download.

## Логика проверок
### 1. Connectivity check
Цель проверки:
- endpoint доступен по сети;
- endpoint отвечает именно как S3 API;
- базовая аутентификация работает.

Дополнительно проверяется наличие заголовка `x-amz-request-id`.  
Если заголовка нет, скрипт считает, что вместо S3 API мог ответить прокси, nginx или HTML-страница ошибки.

Если connectivity check не проходит, дальнейшие тесты не запускаются.

### 2. Bucket check
Для бакета выполняется:
```python
list_objects_v2(Bucket=<bucket>, MaxKeys=1)
```

Это позволяет быстро проверить:

- существует ли бакет;
- есть ли права на доступ;
- сколько занимает по времени простая bucket-операция.

### 3. Download speed test
Пытается скачать заранее существующий объект:

- ключ: `s3monitor.tmp`
- ожидаемый размер: `100 MiB`
- количество повторов: `3`

Для каждой попытки измеряется:

- время скачивания;
- расчетная скорость в Mbps;
- фактически скачанный размер локального файла.

После каждой попытки временный локальный файл удаляется.

### 4. Upload speed test
- создает локальный временный файл;
- выполняет multipart upload в бакет;
- повторяет операцию `3` раза;
- измеряет время и скорость upload;
- по завершении удаляет загруженные объекты из бакета;
- затем удаляет локальный временный файл.

## Формат запуска

```bash
./S3-Sentinel.py <endpoint> <access_key> <secret_key> <bucket>
```

Пример:

```bash
./S3-Sentinel.py s3.example.local:9000 ACCESS_KEY SECRET_KEY backup-bucket
```

## Аргументы

- `endpoint` — адрес S3 endpoint в формате `host` или `host:port`
- `access_key` — S3 access key
- `secret_key` — S3 secret key
- `bucket` — имя бакета для проверки

## Пример JSON-ответа

```json
{
  "timestamp": "2026-03-27T09:00:00+00:00",
  "endpoint": "https://s3.example.local:9000",
  "bucket": "backup-bucket",
  "status": "OK",
  "checks": {
    "connectivity": {
      "success": true
    },
    "bucket_check": {
      "operation": "list_objects_v2",
      "latency_ms": 18.42,
      "success": true,
      "object_count_sample": 1
    },
    "download": {
      "operation": "download_file",
      "object_key": "s3monitor.tmp",
      "size_bytes": 104857600,
      "repeats": 3,
      "success": true,
      "download_time_ms_min": 920.11,
      "download_time_ms_avg": 945.33,
      "download_time_ms_max": 970.58,
      "download_mbps_min": 864.23,
      "download_mbps_avg": 887.21,
      "download_mbps_max": 911.45,
      "ok_runs": 3
    },
    "upload": {
      "operation": "multipart_upload",
      "size_bytes": 10485760,
      "chunk_size": 5242880,
      "threads": 1,
      "repeats": 3,
      "delete_remote": true,
      "success": true,
      "upload_time_ms_min": 140.52,
      "upload_time_ms_avg": 152.41,
      "upload_time_ms_max": 166.32,
      "upload_mbps_min": 504.21,
      "upload_mbps_avg": 550.34,
      "upload_mbps_max": 596.77,
      "ok_upload_runs": 3,
      "delete_time_ms_min": 12.31,
      "delete_time_ms_avg": 15.44,
      "delete_time_ms_max": 19.07,
      "ok_delete_runs": 3
    }
  }
}
```

## Что требуется подготовить заранее

Для корректного download-теста желательно заранее положить в бакет объект:

- ключ: `s3monitor.tmp`
- рекомендуемый размер: `100 MiB`

Если размер будет другим, сам тест скачивания отработает, но расчёт скорости будет основан на ожидаемом размере, указанном в коде.

### Типовой сценарий

1. Zabbix запускает скрипт.
2. Скрипт возвращает JSON.
3. Основной item получает JSON как текст.
4. Dependent items извлекают:
   - общий статус;
   - latency bucket check;
   - download/upload speed;
   - список ошибок.