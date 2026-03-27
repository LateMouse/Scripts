#!/usr/bin/env python3
### Summary:
### Простой мониторинг S3-совместимого хранилища:
# 1) Подключается к S3 API по endpoint + access_key + secret_key.
# 2) Проверяет доступность S3 API.
# 3) Проверяет доступность конкретного бакета.
# 4) Опционально измеряет скорость:
#    - скачивания (download_file) заранее подготовленного объекта,
#    - загрузки (upload_file) тестового файла (включая возможность удаления загруженных объектов).
# 5) Возвращает JSON с метриками (latency, скорость, ошибки) и завершает работу с кодом возврата для Zabbix.

import sys
import boto3
from boto3.s3.transfer import TransferConfig
import os
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
import time
import json
import logging
from datetime import datetime, timezone
import urllib3

urllib3.disable_warnings()
# При желании можно включить подробный лог boto3/botocore для отладки
#boto3.set_stream_logger("botocore", logging.DEBUG)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("S3Monitor")

#  Класс <S3Monitor> инкапсулирует:
#    - клиент S3 (boto3.client("s3"))
#    - набор тестов ("checks")
#    - итоговый статус и метрики в self.metrics
#  Посредством данного класса вызываются проверки по очереди и собираются один общий результат.
class S3Monitor:

# Конструктор:
#   - собираем URL endpoint (добавляем https://):
#   - сохраняем ключи доступа,
#   - создаем botocore Config (таймауты, retries, стиль адресации),
#   - создаем boto3 S3 client,
#   - готовим структуру self.metrics (будущий JSON-результат).
        
    def __init__(self, endpoint, access, secret, bucket):
        # В параметр endpoint ожидается хост:порт или доменное имя. Скрипт сам добавляет "https://
        # Например: "s3.example.local:9000"
        self.endpoint_url = "https://" + endpoint

        # Пара ключей для S3 (аналог логина/пароля).
        self.access_key = access
        self.secret_key = secret

        # Название бакета - объекта хранения в S3.
        self.target_bucket = bucket

        # region_name boto3 часто требует даже для S3-compatible решений.
        # Для многих S3-compatible API можно поставить "us-east-1".
        self.region_name = "us-east-1"

        # Конфигурация botocore клиента:
        # - addressing_style="path" означает URL вида: https://endpoint/bucket/key
        #   (в отличие от virtual-hosted style: https://bucket.endpoint/key).
        # - use_expect_header=False иногда помогает с совместимостью и прокси.
        # - retries: ограничиваем число повторов.
        # - connect_timeout/read_timeout: таймауты соединения/чтения.
        self.config = Config(
            s3={"addressing_style": "path", "use_expect_header": False},
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=10,
            read_timeout=300
        )

        # Создаем S3 client.
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
            config=self.config,
            verify=True
        )

        # Итоговая структура, которую скрипт выведет в JSON.
        # status по умолчанию OK, но может стать WARNING/CRITICAL/UNKNOWN по итогам проверок.
        self.metrics = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "endpoint": self.endpoint_url,
            "bucket": self.target_bucket,
            "status": "OK",
            "checks": {}
        }

    def _measure_latency(self, func, *args, **kwargs):
        # Вспомогательная функция: измеряет время выполнения операции (в миллисекундах)
        # и возвращает ошибку строкой, не “роняя” весь скрипт.
        # Возвращает кортеж:
        # - result: результат func(...) или None при ошибке,
        # - latency_ms: время выполнения в миллисекундах,
        # - error: None если успех, иначе строка с текстом ошибки.
        start = time.time()
        try:
            result = func(*args, **kwargs)
            return result, (time.time() - start) * 1000, None
        except Exception as e:
            return None, (time.time() - start) * 1000, str(e)

    def check_connectivity(self):
        # Проверка базовой доступности S3 API.
        # - Вызываем list_buckets() — очень легкая операция, которая показывает, что:
        #       1) endpoint вообще доступен,
        #       2) это именно S3 API, а не nginx/html заглушка,
        #       3) ключи доступа корректны (или хотя бы endpoint отвечает S3-образно).
        # - дополнительно проверяем наличие заголовка x-amz-request-id:
        #  если его нет, вероятно мы попали не в S3 API (веб-страница/прокси).
        try:
            r = self.client.list_buckets()
            headers = r["ResponseMetadata"]["HTTPHeaders"]
            
            # Для S3 API типично наличие "x-amz-request-id" в HTTP заголовках ответа.
            # Если его нет - подозрение, что endpoint не S3 (например, отдал HTML).
            if "x-amz-request-id" not in headers:
                raise Exception("Not S3 API (nginx/html detected)")

            # Успех записываем в метрики.
            self.metrics["checks"]["connectivity"] = {"success": True}
            return True

        except Exception as e:
            # Если базовая связность не прошла - критическая проблема мониторинга.
            self.metrics["status"] = "CRITICAL"
            self.metrics["checks"]["connectivity"] = {"success": False, "error": str(e)}
            return False

    def check_bucket(self):
        # Проверка бакета:
        # - Пробуем выполнить list_objects_v2 с MaxKeys=1 (получить максимум 1 объект).
        #  Быстрый способ проверить:
        #   1) существует ли бакет,
        #   2) есть ли права доступа,
        #   3) какова примерная задержка операции листинга.
        #
        # В зависимости от ошибки выставляем статус:
        #   - NoSuchBucket / 404 -> CRITICAL (бакет отсутствует),
        #   - AccessDenied / 403 -> WARNING (S3 жив, но прав нет),
        #   - иное -> UNKNOWN (непонятная ситуация).
        result, latency, error = self._measure_latency(
            self.client.list_objects_v2,
            Bucket=self.target_bucket,
            MaxKeys=1
        )

        check = {
            "operation": "list_objects_v2",
            "latency_ms": round(latency, 2),
            "success": error is None
        }

        if error:
            check["error"] = error
            if "NoSuchBucket" in error or "404" in error:
                self.metrics["status"] = "CRITICAL"
            elif "AccessDenied" in error or "403" in error:
                self.metrics["status"] = "WARNING"
            else:
                self.metrics["status"] = "UNKNOWN"

        else:
            check["object_count_sample"] = result.get("KeyCount", 0)

        self.metrics["checks"]["bucket_check"] = check

    def check_upload(self, size_mb=100, chunk_size=5*1024*1024, n_threads=1, repeats=3, delete_remote=True):
        # Проверка загрузки (upload) и опционального удаления объекта:
        #   - создаем локальный временный файл заданного размера (size_mb),
        #   - несколько раз загружаем его в бакет разными ключами,
        #   - считаем время и скорость,
        #   - опционально удаляем загруженный объект (delete_remote=True),
        #   - после тестов удаляем локальный временный файл.
        # 
        # Параметры:
        #   - size_mb: размер тестового файла в мегабайтах,
        #   - chunk_size: размер части для multipart загрузки,
        #   - n_threads: желаемое количество потоков (параллельность),
        #   - repeats: количество повторений теста,
        #   - delete_remote: удалять ли загруженные объекты (чтобы не засорять бакет).
        # 
        # Важно про TransferConfig:
        #   - multipart_threshold/chunksize управляют multipart загрузкой,
        #   - max_concurrency задаёт число потоков, но use_threads=False отключает потоки.

        size_bytes = int(size_mb * 1024 * 1024)

        # Базовый ключ и локальное имя создаваемого файла, который будет загружатся в бакет.
        base_key = "s3monitor-speedtest.tmp"
        tmp_path = os.path.join(os.getcwd(), base_key)

        check = {
            "operation": "multipart_upload",
            "size_bytes": size_bytes,
            "chunk_size": chunk_size,
            "threads": n_threads,
            "repeats": repeats,
            "delete_remote": delete_remote,
            "success": False
        }

        upload_times_s = []
        upload_speeds_mbps = []
        delete_times_s = []
        errors = []
        keys = []

        try:
            # Создаем локальный файл нужного размера, если его нет или размер не совпадает.
            # Пишем нулями по 1MB, чтобы не держать весь файл в памяти.
            if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) != size_bytes:
                with open(tmp_path, "wb") as f:
                    chunk = b"\x00" * (1024 * 1024)
                    remaining = size_bytes
                    while remaining > 0:
                        to_write = chunk if remaining >= len(chunk) else chunk[:remaining]
                        f.write(to_write)
                        remaining -= len(to_write)

            # Конфигурация для upload_file: как именно делать multipart.
            cfg = TransferConfig(
                multipart_threshold=chunk_size,
                multipart_chunksize=chunk_size,
                max_concurrency=n_threads,
                use_threads=True
            )

            # Повторяем тест загрузки несколько раз, чтобы получить min/avg/max.
            for i in range(1, repeats + 1):
                key = f"{base_key}-{int(time.time())}-{os.getpid()}-try{i}"
                keys.append(key)

                try:
                    start = time.time()
                    self.client.upload_file(tmp_path, self.target_bucket, key, Config=cfg)
                    t = time.time() - start
                    upload_times_s.append(t)
                    # Приблизительная скорость в мегабитах/сек в процессе загрузки
                    upload_speeds_mbps.append((size_bytes * 8) / t / 1_000_000 if t > 0 else 0)

                except Exception as e:
                    # Если одна попытка upload упала - ставим WARNING и продолжаем следующие попытки.
                    errors.append(f"upload_try_{i}: {e}")
                    self.metrics["status"] = "WARNING"
                    continue

                # Если включено delete_remote — пробуем удалить загруженный объект.
                if delete_remote:
                    try:
                        start = time.time()
                        self.client.delete_object(Bucket=self.target_bucket, Key=key)
                        dt = time.time() - start
                        delete_times_s.append(dt)
                    except Exception as e:
                        errors.append(f"delete_try_{i}: {e}")
                        self.metrics["status"] = "WARNING"

            check["object_keys"] = keys

            # Если есть хотя бы одна успешная загрузка, считаем статистику времени/скорости.
            if upload_times_s:
                upload_times_ms = [t * 1000 for t in upload_times_s]
                check["upload_time_ms_min"] = round(min(upload_times_ms), 2)
                check["upload_time_ms_avg"] = round(sum(upload_times_ms) / len(upload_times_ms), 2)
                check["upload_time_ms_max"] = round(max(upload_times_ms), 2)

                check["upload_mbps_min"] = round(min(upload_speeds_mbps), 2)
                check["upload_mbps_avg"] = round(sum(upload_speeds_mbps) / len(upload_speeds_mbps), 2)
                check["upload_mbps_max"] = round(max(upload_speeds_mbps), 2)

                check["ok_upload_runs"] = len(upload_times_s)

            # Аналогично считаем статистику по delete, если он включен и были успешные удаления.
            if delete_remote and delete_times_s:
                delete_times_ms = [t * 1000 for t in delete_times_s]
                check["delete_time_ms_min"] = round(min(delete_times_ms), 2)
                check["delete_time_ms_avg"] = round(sum(delete_times_ms) / len(delete_times_ms), 2)
                check["delete_time_ms_max"] = round(max(delete_times_ms), 2)
                check["ok_delete_runs"] = len(delete_times_s)

            # Если были ошибки, добавляем их в результат (чтобы оператор видел причины).
            if errors:
                check["errors"] = errors

            # success=True только если:
            #   - все upload попытки успешны,
            #   - все delete попытки тоже успешны.
            if len(upload_times_s) == repeats and (not delete_remote or len(delete_times_s) == repeats):
                check["success"] = True

        except Exception as e:
            check["error"] = f"upload_failed: {e}"
            self.metrics["status"] = "WARNING"

        finally:
            # Удаляем локальный временный файл, чтобы не копить мусор на диске.
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception as e:
                # Даже если локальная очистка не удалась - фиксируем это в метриках.
                check["local_cleanup_error"] = str(e)
            # Сохраняем результат проверки upload в общие метрики
            self.metrics["checks"]["upload"] = check

    def check_download_speed(self, key="s3monitor.tmp", size_bytes=100 * 1024 * 1024, repeats=3):
        # Проверка скорости скачивания (download).
        # Предполагается, что в бакете уже лежит объект с ключом key (по умолчанию "s3monitor.tmp"),
        # и его размер примерно соответствует size_bytes:
        #
        # Что делаем:
        #   - repeats раз скачиваем объект во временный файл,
        #   - замеряем время,
        #   - считаем скорость (Mbps),
        #   - затем удаляем локальный скачанный файл.
        # 
        # Важно:
        #   - Если объекта нет или нет прав, будут ошибки и статус станет WARNING.
        #   - size_bytes используется для расчета скорости; если реальный размер другой,
        #   скорость получится “условной”. Скрипт дополнительно пытается измерить реальный
        #   скачанный размер (downloaded_size_bytes) для диагностики.
        times_s = []
        speeds_mbps = []
        errors = []

        check = {
            "operation": "download_file",
            "object_key": key,
            "size_bytes": size_bytes,
            "repeats": repeats,
            "success": False
        }

        for i in range(1, repeats + 1):
            # Имя временного файла для конкретной попытки.
            tmp_path = os.path.join(os.getcwd(), f"download-{i}-{key}")

            try:
                start = time.time()
                self.client.download_file(self.target_bucket, key, tmp_path)
                t = time.time() - start
                times_s.append(t)

                speeds_mbps.append((size_bytes * 8) / t / 1_000_000 if t > 0 else 0)

                # Пытаемся узнать, сколько реально байт скачалось на диск.
                try:
                    check.setdefault("downloaded_size_bytes", []).append(os.path.getsize(tmp_path))
                except Exception as e:
                    check.setdefault("downloaded_size_error", []).append(str(e))

            except Exception as e:
                errors.append(f"try_{i}: {e}")
                self.metrics["status"] = "WARNING"

            finally:
                # Удаляем локальный скачанный файл (не трогаем объект в бакете).
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception as e:
                    check.setdefault("local_cleanup_error", []).append(f"try_{i}: {e}")

        # Если хотя бы одно скачивание было успешным - считаем статистику.
        if times_s:
            times_ms = [t * 1000 for t in times_s]
            check["download_time_ms_min"] = round(min(times_ms), 2)
            check["download_time_ms_avg"] = round(sum(times_ms) / len(times_ms), 2)
            check["download_time_ms_max"] = round(max(times_ms), 2)

            check["download_mbps_min"] = round(min(speeds_mbps), 2)
            check["download_mbps_avg"] = round(sum(speeds_mbps) / len(speeds_mbps), 2)
            check["download_mbps_max"] = round(max(speeds_mbps), 2)

            check["ok_runs"] = len(times_s)
            # success=True только если успешны ВСЕ повторы.
            check["success"] = (len(times_s) == repeats)

        if errors:
            check["errors"] = errors

        self.metrics["checks"]["download"] = check

    def run(self):
        # Основной сценарий сбора данных
        # Порядок:
        #   1) check_connectivity - если упало, сразу возвращаем metrics (дальше бессмысленно).
        #   2) Если указан bucket:
        #      - check_bucket (проверка прав/существования/latency),
        #      - check_download_speed (проверка download),
        #      - check_upload (проверка upload с удалением).
        if not self.check_connectivity():
            return self.metrics

        # В текущем коде bucket всегда передается из argv, но оставлена проверка на всякий случай.
        if self.target_bucket:
            self.check_bucket()
            
            # Скачивание: ожидается, что объект "s3monitor.tmp" существует.
            # Если его нет — увидите ошибку в download.checks.
            self.check_download_speed(key="s3monitor.tmp", size_bytes=100 * 1024 * 1024, repeats=3)
            self.check_upload(10, repeats=3, delete_remote=True)

        return self.metrics

if __name__ == "__main__":
# Точка входа при запуске скрипта как программы.
# Ожидаемые аргументы командной строки:
#   s3monitor.py <endpoint> <access_key> <secret_key> <bucket>
#   Пример:
#   ./s3monitor.py s3.example.local <access_key> <secret_key> mybucket
#
#   По результату:
#      - печатаем JSON,
#           OK -> 0
#           WARNING -> 1
#           CRITICAL -> 2
#           UNKNOWN -> 3

    # Простая проверка количества аргументов.
    if len(sys.argv) != 5:
        print("Usage: s3monitor.py <endpoint> <access_key> <secret_key> <bucket>")
        sys.exit(3)

    endpoint = sys.argv[1]
    access = sys.argv[2]
    secret = sys.argv[3]
    bucket = sys.argv[4]

    # Создаем переменную класса и запускаем проверки
    monitor = S3Monitor(endpoint, access, secret, bucket)
    result = monitor.run()
    # Печатаем JSON-результат.
    print(json.dumps(result, indent=2))

    # Коды возврата для Zabbix
    status_map = {"OK": 0, "WARNING": 1, "CRITICAL": 2, "UNKNOWN": 3}
    sys.exit(status_map.get(result["status"], 3))
