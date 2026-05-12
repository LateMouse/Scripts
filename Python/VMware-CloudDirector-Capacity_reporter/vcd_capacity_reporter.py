#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
from collections import defaultdict
import requests
import xml.etree.ElementTree as ET

# Пространство имен XML, которые используются при поиске XML-элементов в ответах vCloud Director.
# Без них методы find/findall/findtext не смогут корректно найти узлы в XML.
NS = {
    "vcloud": "http://www.vmware.com/vcloud/v1.5",
    "vmext": "http://www.vmware.com/vcloud/extension/v1.5",
}

# class CollectorError:
#   Тип исключения для контролируемых ошибок коллектора.
#   позволяет отделить ожидаемые ошибки бизнес-логики и интеграции от непредвиденных системных ошибок;
#   упрощает централизованную обработку ошибок в main().
#   Используется почти во всех функциях, где возможна контролируемая ошибка. Перехватывается в main() для вывода сообщения.
class CollectorError(Exception):
    pass

# str_to_bool:
#   Преобразует строковое значение в булево:
#   - принимает строку, число или иной объект;
#   - приводит значение к нижнему регистру и убирает пробелы;
#   - распознает типовые варианты true/false.
#
#   Если значение не распознано - вызывает argparse.ArgumentTypeError, чтобы пользователь сразу увидел, что аргумент командной строки передан неверно.
#   Используется в parse_args() для параметра --verify-ssl.
def str_to_bool(value):
    value = str(value).strip().lower()
    if value in ("1", "true", "yes", "y", "on"):
        return True
    if value in ("0", "false", "no", "n", "off"):
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean value: {value}")

# safe_int
#   Преобразует значение в int.
#   - если значение отсутствует, возвращает default;
#   - если значение нельзя преобразовать в целое число, тоже возвращает default.
#
#   В API-ответах VCD числовые значения часто приходят как строки:
#   - некоторые поля могут отсутствовать или быть пустыми;
#   - функция позволяет не останавливать выполнение из-за единичного некорректного поля.
#
#   используется в parse_capacity_block(), parse_query_result_records(),
#   aggregate_admin_org_vdc_storage_profiles(), aggregate_datastores()
def safe_int(value, default=None):
    if value is None:
        return default
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default

# safe_float_div
#   Защита от деления на ноль;
#   Единый формат расчета коэффициентов использования ресурсов.
#
#   Делит одно число на другое и округляет результат:
#   - возвращает None, если числитель отсутствует;
#   - возвращает None, если знаменатель отсутствует или равен 0;
#   - в остальных случаях выполняет деление и округляет результат.
#
#   Используется для расчета utilisation/ratio в агрегатах и итоговом отчете.
def safe_float_div(numerator, denominator, precision=6):
    if numerator is None or denominator in (None, 0):
        return None
    return round(numerator / denominator, precision)

# mb_to_gb
#   Переводит мегабайты в гигабайты:
#   Используется в агрегатах storage и при формировании метрик.
def mb_to_gb(value_mb):
    if value_mb is None:
        return None
    return round(value_mb / 1024, 2)

# mb_to_gb
#   Переводит мегабайты в терабайты:
#   Используется в агрегатах в build_memory_converted(), aggregate_admin_org_vdc_storage_profiles(), aggregate_datastores().
def mb_to_tb(value_mb):
    if value_mb is None:
        return None
    return round(value_mb / 1024 / 1024, 4)

# mhz_to_ghz
#   Переводит MHz в GHz.
#   Используется в build_cpu_converted() для представления CPU-метрик.
def mhz_to_ghz(value_mhz):
    if value_mhz is None:
        return None
    return round(value_mhz / 1000, 6)

# mhz_to_thz
#   Переводит MHz в THz.
#   Используется в build_cpu_converted() для представления CPU-метрик.
def mhz_to_thz(value_mhz):
    if value_mhz is None:
        return None
    return round(value_mhz / 1000000, 9)

# build_cpu_converted
#   Строит расширенный словарь CPU-метрик.
#   Возвращает словарь, где каждая метрика представлена сразу в MHz, GHz и THz.
#
#   Вызывается из build_pvdc_report() после получения и разбора CPU capacity.
def build_cpu_converted(cpu):
    return {
        "allocation_mhz": cpu.get("allocation"),
        "allocation_ghz": mhz_to_ghz(cpu.get("allocation")),
        "allocation_thz": mhz_to_thz(cpu.get("allocation")),
        "reserved_mhz": cpu.get("reserved"),
        "reserved_ghz": mhz_to_ghz(cpu.get("reserved")),
        "reserved_thz": mhz_to_thz(cpu.get("reserved")),
        "total_mhz": cpu.get("total"),
        "total_ghz": mhz_to_ghz(cpu.get("total")),
        "total_thz": mhz_to_thz(cpu.get("total")),
        "used_mhz": cpu.get("used"),
        "used_ghz": mhz_to_ghz(cpu.get("used")),
        "used_thz": mhz_to_thz(cpu.get("used")),
        "overhead_mhz": cpu.get("overhead"),
        "overhead_ghz": mhz_to_ghz(cpu.get("overhead")),
        "overhead_thz": mhz_to_thz(cpu.get("overhead")),
        "total_reservation_mhz": cpu.get("total_reservation"),
        "total_reservation_ghz": mhz_to_ghz(cpu.get("total_reservation")),
        "total_reservation_thz": mhz_to_thz(cpu.get("total_reservation")),
    }


# build_memory_converted
#   Строит расширенный словарь memory-метрик.
#   Возвращает словарь, где каждая метрика представлена в MB, GB, TB.
#
#   Вызывается из build_pvdc_report() после разбора memory capacity.
def build_memory_converted(mem):
    return {
        "allocation_mb": mem.get("allocation"),
        "allocation_gb": mb_to_gb(mem.get("allocation")),
        "allocation_tb": mb_to_tb(mem.get("allocation")),
        "reserved_mb": mem.get("reserved"),
        "reserved_gb": mb_to_gb(mem.get("reserved")),
        "reserved_tb": mb_to_tb(mem.get("reserved")),
        "total_mb": mem.get("total"),
        "total_gb": mb_to_gb(mem.get("total")),
        "total_tb": mb_to_tb(mem.get("total")),
        "used_mb": mem.get("used"),
        "used_gb": mb_to_gb(mem.get("used")),
        "used_tb": mb_to_tb(mem.get("used")),
        "overhead_mb": mem.get("overhead"),
        "overhead_gb": mb_to_gb(mem.get("overhead")),
        "overhead_tb": mb_to_tb(mem.get("overhead")),
        "total_reservation_mb": mem.get("total_reservation"),
        "total_reservation_gb": mb_to_gb(mem.get("total_reservation")),
        "total_reservation_tb": mb_to_tb(mem.get("total_reservation")),
    }

# first_non_empty
#   Возвращает первое непустое значение из списка кандидатов.
#   Важная склейка между разными форматами XML:
#   один и тот же атрибут может лежать:
#   - в тексте XML-элемента,
#   - в атрибуте XML-узла,
#   - в namespace vcloud,
#   - в namespace vmext.
#
#   parse_provider_vdc_storage_profile_xml() активно использует эту функцию,
#   чтобы не быть жестко привязанным к одному варианту структуры.
def first_non_empty(*values):
    for value in values:
        if value is None:
            continue
        s = str(value).strip()
        if s != "":
            return s
    return None

# node_bool
#   Преобразует текстовое значение из XML в bool.
#
#   Отличие от str_to_bool():
#       - str_to_bool() предназначен для CLI и бросает ошибку на плохом вводе;
#       - node_bool() предназначен для данных API и в сомнительном случае возвращает None.
#
#   Это отражает разную семантику:
#       CLI должен быть валидным,
#       а XML-ответ просто может быть неполным или неожиданным.
def node_bool(value):
    if value is None:
        return None
    value = str(value).strip().lower()
    if value in ("true", "1", "yes", "y", "on"):
        return True
    if value in ("false", "0", "no", "n", "off"):
        return False
    return None

# sanitize_filename
#   Связь с dump_provider_storage_profile_xml():
#   значения profile_name и profile_id приходят из API,
#   поэтому перед использованием в имени файла их нужно очистить.
def sanitize_filename(value):
    if value is None:
        return "unnamed"
    value = str(value).strip()
    if not value:
        return "unnamed"
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    return value[:200]

# VCDClient:
#   Клиент для работы с API vCloud Director:
#   - хранит параметры подключения;
#   - управляет HTTP-сессией requests;
#   - аутентификация;
#   - GET-запросы к JSON и XML API VCD.
class VCDClient:

    # __init__:    
    # Инициализия клиента:
    #    Параметры:
    #    - base_url: URL vCloud Director, например https://vcd.example.com
    #    - api_token: refresh token для получения access token
    #    - verify_ssl: проверять ли SSL-сертификат
    #    - api_version: версия API VCD
    #    - timeout: таймаут HTTP-запросов в секундах
    def __init__(self, base_url, api_token, verify_ssl=True, api_version="39.1", timeout=60):
        self.base_url = base_url.rstrip("/")
        self.api_token = api_token
        self.verify_ssl = verify_ssl
        self.api_version = api_version
        self.timeout = timeout
        self.session = requests.Session()
        self.access_token = None

    # _headers
    # Формирование HTTP-заголовков для запросов к VCD API:
    #   - добавляет Bearer access token;
    #   - задает Accept-заголовок с нужным форматом ответа.
    # Нужен чтобы не дублировать одинаковую логику в get_json() и get_xml().
    def _headers(self, accept):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": accept,
        }

    # authenticate:
    # Выполняет аутентификацию в VCD и получает access token:
    #   - отправляет POST-запрос на /oauth/provider/token;
    #   - использует refresh_token grant;
    #   - извлекает access_token из JSON-ответа;
    #   - сохраняет access_token внутри объекта клиента.
    def authenticate(self):
        url = f"{self.base_url}/oauth/provider/token"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.api_token,
        }

        try:
            response = self.session.post(
                url,
                headers=headers,
                data=data,
                verify=self.verify_ssl,
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.RequestException as exc:
            raise CollectorError(f"Authentication request failed: {exc}") from exc
        except ValueError as exc:
            raise CollectorError("Authentication response is not valid JSON") from exc

        access_token = payload.get("access_token")
        if not access_token:
            raise CollectorError("Authentication succeeded but access_token is missing")

        self.access_token = access_token

    # get_json:
    # Выполняет GET-запрос к JSON API VCD и возвращает уже разобранный JSON:
    #   - проверяет, что клиент уже аутентифицирован;
    #   - выполняет GET-запрос;
    #   - добавляет нужную версию API в Accept-заголовок;
    #   - возвращает response.json().
    # Используется для запросов к cloudapi для списка Provider VDC.
    def get_json(self, path, params=None):
        if not self.access_token:
            raise CollectorError("Client is not authenticated")

        url = f"{self.base_url}{path}"
        try:
            response = self.session.get(
                url,
                headers=self._headers(f"application/json;version={self.api_version}"),
                params=params,
                verify=self.verify_ssl,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            raise CollectorError(f"JSON request failed for {path}: {exc}") from exc
        except ValueError as exc:
            raise CollectorError(f"Response is not valid JSON for {path}") from exc

    # get_xml
    # Выполняет GET-запрос к XML API VCD и возвращает XML как строку:
    #   - проверяет наличие access token;
    #   - выполняет GET-запрос;
    #   - при необходимости принимает кастомный Accept;
    #   - возвращает текст XML-ответа.
    def get_xml(self, path, params=None, accept=None):
        if not self.access_token:
            raise CollectorError("Client is not authenticated")

        url = f"{self.base_url}{path}"
        try:
            response = self.session.get(
                url,
                headers=self._headers(accept or f"application/*+xml;version={self.api_version}"),
                params=params,
                verify=self.verify_ssl,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as exc:
            raise CollectorError(f"XML request failed for {path}: {exc}") from exc

# urn_to_uuid
# Извлекает UUID из URN-строки.
#   Пример:
#   - вход: urn:vcloud:providervdc:12345678
#   - выход: 12345678
#
#   - часть API возвращает идентификаторы в формате URN;
#   - admin API для Provider VDC ожидает UUID в URL.
# Используется в build_pvdc_report() перед запросом admin XML.
def urn_to_uuid(urn):
    if not urn or ":" not in urn:
        raise CollectorError(f"Invalid URN value: {urn}")
    return urn.split(":")[-1]

# parse_xml_root
# Разбирает XML-строку и возвращает корневой XML-элемент.
#   - вызывает ET.fromstring();
#   - при ошибке разбора выбрасывает CollectorError с указанием контекста.
# Единая точка обработки ошибок XML.
# Используется в parse_provider_vdc_admin_xml() и parse_query_result_records().
def parse_xml_root(xml_text, context):
    try:
        return ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise CollectorError(f"Failed to parse XML for {context}: {exc}") from exc

# xml_text_any
#   Ищет первый непустой атрибут по списку имен.
#
#   В связке с xml_text_any() позволяет парсеру быть "двухрежимным":
#   сначала искать значения как XML-элементы, потом как XML-атрибуты
def xml_text_any(node, paths):
    for path in paths:
        value = node.findtext(path, default=None, namespaces=NS)
        if value is not None and str(value).strip() != "":
            return value
    return None

# xml_attr_any
#   Унифицированный парсер блока capacity (CPU или Memory).
#
#   один из ключевых примеров переиспользования:
#   структура CPU и Memory в admin XML одинакова,
#   поэтому одна функция обслуживает оба случая.
#
#   Благодаря этому parse_provider_vdc_admin_xml() не дублирует логику.
def xml_attr_any(node, attr_names):
    for attr in attr_names:
        value = node.attrib.get(attr)
        if value is not None and str(value).strip() != "":
            return value
    return None

# parse_capacity_block:
# Разбирает один блок capacity из XML CPU или Memory из секции ComputeCapacity. Извлекаем:
#   - units
#   - allocation
#   - reserved
#   - total
#   - used
#   - overhead
#   - total_reservation
# Cтруктура CPU и Memory в XML очень похожа, что позволяет не дублировать одинаковый код.
# Используется в parse_provider_vdc_admin_xml() для CPU и Memory.
def parse_capacity_block(node):
    if node is None:
        return {}

    return {
        "units": node.findtext("vcloud:Units", default="", namespaces=NS),
        "allocation": safe_int(node.findtext("vcloud:Allocation", default=None, namespaces=NS)),
        "reserved": safe_int(node.findtext("vcloud:Reserved", default=None, namespaces=NS)),
        "total": safe_int(node.findtext("vcloud:Total", default=None, namespaces=NS)),
        "used": safe_int(node.findtext("vcloud:Used", default=None, namespaces=NS)),
        "overhead": safe_int(node.findtext("vcloud:Overhead", default=None, namespaces=NS)),
        "total_reservation": safe_int(node.findtext("vcloud:TotalReservation", default=None, namespaces=NS)),
    }

# parse_provider_vdc_storage_profile_xml
#   Разбирает XML отдельного Provider VDC storage profile.
#
#   - parse_provider_vdc_admin_xml() дает список storage profile "ссылок" (имя, href, id);
#   - дальше по каждому href делается отдельный GET XML;
#   - уже этот XML парсится здесь для получения емкости, usage, IOPS.
#
#   То есть admin XML даёт "каркас" профилей,
#   а этот парсер извлекает их детальные метрики.
def parse_provider_vdc_storage_profile_xml(xml_text):
    root = parse_xml_root(xml_text, "Provider VDC storage profile")

    enabled_raw = first_non_empty(
        xml_text_any(root, [
            ".//vcloud:Enabled",
            ".//vmext:Enabled",
        ]),
        xml_attr_any(root, [
            "enabled",
        ])
    )

    units_raw = first_non_empty(
        xml_text_any(root, [
            ".//vcloud:Units",
            ".//vmext:Units",
        ]),
        xml_attr_any(root, [
            "units",
        ])
    )

    capacity_total_raw = first_non_empty(
        xml_text_any(root, [
            ".//vcloud:CapacityTotal",
            ".//vmext:CapacityTotal",
        ]),
        xml_attr_any(root, [
            "capacityTotal",
        ])
    )

    capacity_used_raw = first_non_empty(
        xml_text_any(root, [
            ".//vcloud:CapacityUsed",
            ".//vmext:CapacityUsed",
        ]),
        xml_attr_any(root, [
            "capacityUsed",
        ])
    )

    iops_capacity_raw = first_non_empty(
        xml_text_any(root, [
            ".//vcloud:IopsCapacity",
            ".//vmext:IopsCapacity",
        ]),
        xml_attr_any(root, [
            "iopsCapacity",
        ])
    )

    iops_allocated_raw = first_non_empty(
        xml_text_any(root, [
            ".//vcloud:IopsAllocated",
            ".//vmext:IopsAllocated",
        ]),
        xml_attr_any(root, [
            "iopsAllocated",
        ])
    )

    return {
        "id": first_non_empty(root.attrib.get("id")),
        "name": first_non_empty(root.attrib.get("name")),
        "href": first_non_empty(root.attrib.get("href")),
        "enabled": node_bool(enabled_raw),
        "default": None,
        "units": units_raw,
        "capacity_total_mb": safe_int(capacity_total_raw),
        "capacity_used_mb": safe_int(capacity_used_raw),
        "iops_capacity": safe_int(iops_capacity_raw),
        "iops_allocated": safe_int(iops_allocated_raw),
    }


# parse_provider_vdc_admin_xml
# Разбирает XML admin-представления Provider VDC:
#   - получаем корень XML;
#   - извлекаем идентификатор и имя Provider VDC;
#   - извлекаем ComputeCapacity для CPU и Memory;
#   - извлекаем список storage profiles, опубликованных в Provider VDC.
# Возвращает Python-словарь с основными данными Provider VDC.
# Вызывается из build_pvdc_report() после получения XML через get_xml().
def parse_provider_vdc_admin_xml(xml_text):
    root = parse_xml_root(xml_text, "Provider VDC admin view")

    result = {
        "id": root.attrib.get("id"),
        "name": root.attrib.get("name"),
        "compute": {
            "cpu": {},
            "memory": {},
        },
        "storage_profiles": [],
    }

    cpu = root.find("./vcloud:ComputeCapacity/vcloud:Cpu", NS)
    mem = root.find("./vcloud:ComputeCapacity/vcloud:Memory", NS)

    result["compute"]["cpu"] = parse_capacity_block(cpu)
    result["compute"]["memory"] = parse_capacity_block(mem)

    for sp in root.findall("./vcloud:StorageProfiles/vcloud:ProviderVdcStorageProfile", NS):
        result["storage_profiles"].append({
            "name": sp.attrib.get("name"),
            "id": sp.attrib.get("id"),
            "href": sp.attrib.get("href"),
            "type": sp.attrib.get("type"),
        })

    return result

# parse_query_result_records
# Разбирает XML-ответ query API в формате records:
#   - читает служебные параметры страницы: page, pageSize, total, pageCount;
#   - проходит по дочерним элементам XML;
#   - сохраняет только элементы, имена которых оканчиваются на Record.
# Возвращает словарь с параметрами и списком records.
# query API VCD работает постранично. Данные из records удобно затем агрегировать в Python.
# Используется в query_all_records().
def parse_query_result_records(xml_text):
    root = parse_xml_root(xml_text, "query records")

    records = []
    for child in root:
        tag = child.tag.split("}")[-1]
        if tag.endswith("Record"):
            records.append({
                "tag": tag,
                "attrib": dict(child.attrib),
            })

    return {
        "page": safe_int(root.attrib.get("page")),
        "page_size": safe_int(root.attrib.get("pageSize")),
        "total": safe_int(root.attrib.get("total")),
        "page_count": safe_int(root.attrib.get("pageCount")),
        "records": records,
    }

# query_all_records
# Параметры:
#   - client: экземпляр VCDClient
#   - query_type: тип query, например datastore или adminOrgVdcStorageProfile
#   - page_size: размер страницы
# Получает все записи query API:
#   - отправляет запросы к /api/query постранично;
#   - запрашивает формат records;
#   - продолжает запрашивать страницы, пока не будут собраны все записи.
# VCD query API редко возвращает все данные одной страницей. Функция скрывает от остального кода всю логику постраничного обхода.
# Используется в build_pvdc_report() для получения записей по storage profiles и datastores.
def query_all_records(client, query_type, page_size=128, filter_expr=None):
    all_records = []
    page = 1

    while True:
        params = {
            "type": query_type,
            "format": "records",
            "page": page,
            "pageSize": page_size,
        }
        if filter_expr:
            params["filter"] = filter_expr

        parsed = parse_query_result_records(client.get_xml("/api/query", params=params))
        batch = parsed["records"]
        all_records.extend(batch)

        total = parsed["total"]
        page_count = parsed["page_count"]

        if page_count is not None and page >= page_count:
            break
        if not batch:
            break
        if total is not None and len(all_records) >= total:
            break

        page += 1

    return all_records

# aggregate_admin_org_vdc_storage_profiles
# Агрегирует usage по storage policy на основе записей adminOrgVdcStorageProfile:
#   - группирует записи по имени storage policy;
#   - суммирует количество Org VDC profile, used storage и limit storage;
#   - рассчитывает значения в MB, GB, TB;
#   - рассчитывает коэффициент использования usage_ratio.
# Возвращает список политик с агрегированными значениями и summary по всем политикам вместе.
# Позволяет увидеть, как распределено потребление storage по storage policy, а не только в разрезе отдельных записей API.
def aggregate_admin_org_vdc_storage_profiles(records):
    by_policy = defaultdict(lambda: {
        "org_vdc_profile_count": 0,
        "storage_used_mb": 0,
        "storage_limit_mb": 0,
    })

    for rec in records:
        a = rec.get("attrib", {})
        policy_name = a.get("name") or "UNKNOWN"
        used_mb = safe_int(a.get("storageUsedMB"), 0)
        limit_mb = safe_int(a.get("storageLimitMB"), 0)

        by_policy[policy_name]["org_vdc_profile_count"] += 1
        by_policy[policy_name]["storage_used_mb"] += used_mb
        by_policy[policy_name]["storage_limit_mb"] += limit_mb

    policies = []
    for policy_name, data in by_policy.items():
        used_mb = data["storage_used_mb"]
        limit_mb = data["storage_limit_mb"]

        policies.append({
            "storage_policy": policy_name,
            "org_vdc_profile_count": data["org_vdc_profile_count"],
            "tenant_storage_used_mb": used_mb,
            "tenant_storage_used_gb": mb_to_gb(used_mb),
            "tenant_storage_used_tb": mb_to_tb(used_mb),
            "tenant_storage_limit_mb": limit_mb,
            "tenant_storage_limit_gb": mb_to_gb(limit_mb),
            "tenant_storage_limit_tb": mb_to_tb(limit_mb),
            "tenant_usage_ratio": safe_float_div(used_mb, limit_mb),
        })

    policies.sort(key=lambda x: x["tenant_storage_used_mb"], reverse=True)

    total_used_mb = sum(x["tenant_storage_used_mb"] for x in policies)
    total_limit_mb = sum(x["tenant_storage_limit_mb"] for x in policies)

    return {
        "policies": policies,
        "summary": {
            "policy_count": len(policies),
            "org_vdc_profile_count": sum(x["org_vdc_profile_count"] for x in policies),
            "tenant_storage_used_mb_total": total_used_mb,
            "tenant_storage_used_gb_total": mb_to_gb(total_used_mb),
            "tenant_storage_used_tb_total": mb_to_tb(total_used_mb),
            "tenant_storage_limit_mb_total": total_limit_mb,
            "tenant_storage_limit_gb_total": mb_to_gb(total_limit_mb),
            "tenant_storage_limit_tb_total": mb_to_tb(total_limit_mb),
            "tenant_usage_ratio_total": safe_float_div(total_used_mb, total_limit_mb),
        },
    }

# aggregate_provider_storage_profiles
#   Агрегирует provider-side view по storage policy.
#
#    Здесь происходит сшивание двух источников:
#   1) admin_storage_profiles -> список профилей из PVDC admin XML;
#   2) detailed_profiles -> подробности, полученные отдельными GET по href.
#
#   Таким образом, admin XML дает перечень сущностей,
#   а detailed XML дает метрики этих сущностей.
def aggregate_provider_storage_profiles(admin_storage_profiles, detailed_profiles):
    detailed_by_name = {}
    for item in detailed_profiles:
        name = item.get("name")
        if name:
            detailed_by_name[name] = item

    policies = []
    for admin_sp in admin_storage_profiles:
        name = admin_sp.get("name") or "UNKNOWN"
        detailed = detailed_by_name.get(name, {})

        capacity_total_mb = detailed.get("capacity_total_mb")
        capacity_used_mb = detailed.get("capacity_used_mb")
        iops_capacity = detailed.get("iops_capacity")
        iops_allocated = detailed.get("iops_allocated")

        policies.append({
            "storage_policy": name,
            "provider_storage_capacity_mb": capacity_total_mb,
            "provider_storage_capacity_gb": mb_to_gb(capacity_total_mb),
            "provider_storage_capacity_tb": mb_to_tb(capacity_total_mb),
            "provider_storage_used_mb": capacity_used_mb,
            "provider_storage_used_gb": mb_to_gb(capacity_used_mb),
            "provider_storage_used_tb": mb_to_tb(capacity_used_mb),
            "provider_usage_ratio": safe_float_div(capacity_used_mb, capacity_total_mb),
            "iops_capacity": iops_capacity,
            "iops_allocated": iops_allocated,
        })

     # Сначала политики с известной capacity, затем по убыванию capacity.
    policies.sort(key=lambda x: (x["provider_storage_capacity_mb"] is None, -(x["provider_storage_capacity_mb"] or 0)))

    total_capacity_mb = sum((x["provider_storage_capacity_mb"] or 0) for x in policies)
    total_used_mb = sum((x["provider_storage_used_mb"] or 0) for x in policies)

    return {
        "policies": policies,
        "summary": {
            "policy_count": len(policies),
            "provider_storage_capacity_mb_total": total_capacity_mb,
            "provider_storage_capacity_gb_total": mb_to_gb(total_capacity_mb),
            "provider_storage_capacity_tb_total": mb_to_tb(total_capacity_mb),
            "provider_storage_used_mb_total": total_used_mb,
            "provider_storage_used_gb_total": mb_to_gb(total_used_mb),
            "provider_storage_used_tb_total": mb_to_tb(total_used_mb),
            "provider_usage_ratio_total": safe_float_div(total_used_mb, total_capacity_mb),
        },
    }

# merge_storage_views
#   Объединяет tenant_view и provider_view в единую merged_view.
#
#   Соединяет два разных взгляда на одну и ту же storage policy:
#   - сколько заняли и какой лимит виден арендаторам;
#   - какая реальная provider capacity есть под этой политикой.
#
#   Именно здесь появляются cross-layer коэффициенты,
#   которых нельзя было получить ни из tenant_agg, ни из provider_agg по отдельности.
def merge_storage_views(tenant_agg, provider_agg):
    provider_by_name = {
        x["storage_policy"]: x for x in provider_agg.get("policies", [])
    }
    tenant_by_name = {
        x["storage_policy"]: x for x in tenant_agg.get("policies", [])
    }

    all_names = sorted(set(provider_by_name.keys()) | set(tenant_by_name.keys()))

    merged = []
    for name in all_names:
        tenant = tenant_by_name.get(name, {})
        provider = provider_by_name.get(name, {})

        tenant_used_mb = tenant.get("tenant_storage_used_mb")
        tenant_limit_mb = tenant.get("tenant_storage_limit_mb")
        provider_capacity_mb = provider.get("provider_storage_capacity_mb")

        merged.append({
            "storage_policy": name,
            "org_vdc_profile_count": tenant.get("org_vdc_profile_count"),
            "tenant_storage_used_mb": tenant_used_mb,
            "tenant_storage_used_gb": mb_to_gb(tenant_used_mb),
            "tenant_storage_used_tb": mb_to_tb(tenant_used_mb),
            "tenant_storage_limit_mb": tenant_limit_mb,
            "tenant_storage_limit_gb": mb_to_gb(tenant_limit_mb),
            "tenant_storage_limit_tb": mb_to_tb(tenant_limit_mb),
            "provider_storage_capacity_mb": provider_capacity_mb,
            "provider_storage_capacity_gb": mb_to_gb(provider_capacity_mb),
            "provider_storage_capacity_tb": mb_to_tb(provider_capacity_mb),
            "tenant_usage_ratio": safe_float_div(tenant_used_mb, tenant_limit_mb),
            "provider_consumption_ratio": safe_float_div(tenant_used_mb, provider_capacity_mb),
            "tenant_limit_vs_provider_ratio": safe_float_div(tenant_limit_mb, provider_capacity_mb),
        })

    total_tenant_used_mb = sum((x.get("tenant_storage_used_mb") or 0) for x in merged)
    total_tenant_limit_mb = sum((x.get("tenant_storage_limit_mb") or 0) for x in merged)
    total_provider_capacity_mb = sum((x.get("provider_storage_capacity_mb") or 0) for x in merged)

    return {
        "policies": merged,
        "summary": {
            "policy_count": len(merged),
            "tenant_storage_used_mb_total": total_tenant_used_mb,
            "tenant_storage_used_gb_total": mb_to_gb(total_tenant_used_mb),
            "tenant_storage_used_tb_total": mb_to_tb(total_tenant_used_mb),
            "tenant_storage_limit_mb_total": total_tenant_limit_mb,
            "tenant_storage_limit_gb_total": mb_to_gb(total_tenant_limit_mb),
            "tenant_storage_limit_tb_total": mb_to_tb(total_tenant_limit_mb),
            "provider_storage_capacity_mb_total": total_provider_capacity_mb,
            "provider_storage_capacity_gb_total": mb_to_gb(total_provider_capacity_mb),
            "provider_storage_capacity_tb_total": mb_to_tb(total_provider_capacity_mb),
            "tenant_used_vs_provider_capacity_ratio_total": safe_float_div(
                total_tenant_used_mb, total_provider_capacity_mb
            ),
            "tenant_limit_vs_provider_capacity_ratio_total": safe_float_div(
                total_tenant_limit_mb, total_provider_capacity_mb
            ),
        },
    }

# aggregate_datastores
# Агрегирует суммарные показатели по datastores:
#   - считает количество datastore;
#   - суммирует полную емкость и используемую емкость, provisioned и requested объемы;
#   - конвертирует значения в GB и TB;
#   - рассчитывает коэффициенты заполнения и переподписки.
# Возвращает словарь с суммарной статистикой по datastore.
def aggregate_datastores(records):
    totals = {
        "datastore_count": 0,
        "storage_mb_total": 0,
        "storage_used_mb_total": 0,
        "provisioned_storage_mb_total": 0,
        "requested_storage_mb_total": 0,
    }

    for rec in records:
        a = rec.get("attrib", {})
        totals["datastore_count"] += 1
        totals["storage_mb_total"] += safe_int(a.get("storageMB"), 0)
        totals["storage_used_mb_total"] += safe_int(a.get("storageUsedMB"), 0)
        totals["provisioned_storage_mb_total"] += safe_int(a.get("provisionedStorageMB"), 0)
        totals["requested_storage_mb_total"] += safe_int(a.get("requestedStorageMB"), 0)

    total_storage_mb = totals["storage_mb_total"]
    total_used_mb = totals["storage_used_mb_total"]
    total_provisioned_mb = totals["provisioned_storage_mb_total"]
    total_requested_mb = totals["requested_storage_mb_total"]

    totals.update({
        "storage_gb_total": mb_to_gb(total_storage_mb),
        "storage_tb_total": mb_to_tb(total_storage_mb),
        "storage_used_gb_total": mb_to_gb(total_used_mb),
        "storage_used_tb_total": mb_to_tb(total_used_mb),
        "provisioned_storage_gb_total": mb_to_gb(total_provisioned_mb),
        "provisioned_storage_tb_total": mb_to_tb(total_provisioned_mb),
        "requested_storage_gb_total": mb_to_gb(total_requested_mb),
        "requested_storage_tb_total": mb_to_tb(total_requested_mb),
        "used_ratio": safe_float_div(total_used_mb, total_storage_mb),
        "provisioned_ratio": safe_float_div(total_provisioned_mb, total_storage_mb),
        "requested_ratio": safe_float_div(total_requested_mb, total_storage_mb),
    })

    return totals

# dump_provider_storage_profile_xml
#   При необходимости сохраняет сырой XML storage profile на диск.
#
#   Это вспомогательная отладочная ветка.
#   Она не влияет на вычисление итогового отчета, но помогает анализировать, почему 
#   parse_provider_vdc_storage_profile_xml() не смог извлечь нужные поля.
def dump_provider_storage_profile_xml(dump_dir, profile_name, profile_id, xml_text):
    if not dump_dir:
        return None

    os.makedirs(dump_dir, exist_ok=True)
    safe_name = sanitize_filename(profile_name)
    safe_id = sanitize_filename(profile_id)
    filename = f"{safe_name}__{safe_id}.xml"
    path = os.path.join(dump_dir, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(xml_text)

    return path

# build_pvdc_report
# Основная функция - собирает итоговый отчет по Provider VDC:
#   1. Получает список Provider VDC через cloudapi.
#   2. Выбирает нужный Provider VDC:
#       - по имени, если передан pvdc_name;
#       - первый из списка, если имя не указано.
#   3. Извлекает UUID Provider VDC из URN.
#   4. Получает admin XML-представление выбранного Provider VDC.
#   5. Извлекает блоки CPU и Memory capacity.
#   6. Формирует конвертированные метрики CPU и RAM.
#   7. Получает и агрегирует usage по storage policy.
#   8. Получает и агрегирует емкость datastore.
#   9. Рассчитывает итоговые ratio по CPU и memory.
#   10. Возвращает единый JSON-совместимый словарь отчета.
# Возвращает полностью подготовленный отчет, готовый к записи в JSON.
def build_pvdc_report(client, pvdc_name=None, dump_provider_storage_profile_xml_dir=None):
    provider_vdcs = client.get_json("/cloudapi/1.0.0/providerVdcs")
    values = provider_vdcs.get("values", [])

    if not values:
        raise CollectorError("No Provider VDCs returned by /cloudapi/1.0.0/providerVdcs")

    if pvdc_name:
        matches = [x for x in values if x.get("name") == pvdc_name]
        if not matches:
            raise CollectorError(f"Provider VDC named '{pvdc_name}' not found")
        pvdc = matches[0]
    else:
        pvdc = values[0]

    pvdc_uuid = urn_to_uuid(pvdc.get("id"))

    admin = parse_provider_vdc_admin_xml(client.get_xml(f"/api/admin/providervdc/{pvdc_uuid}"))

    cpu = admin["compute"]["cpu"]
    mem = admin["compute"]["memory"]

    if not cpu:
        raise CollectorError("CPU block was not parsed from Provider VDC admin XML")
    if not mem:
        raise CollectorError("Memory block was not parsed from Provider VDC admin XML")

    cpu_converted = build_cpu_converted(cpu)
    memory_converted = build_memory_converted(mem)

    tenant_storage_agg = aggregate_admin_org_vdc_storage_profiles(
        query_all_records(client, "adminOrgVdcStorageProfile")
    )

    provider_storage_profile_details = []
    provider_storage_profile_debug = []

    for sp in admin.get("storage_profiles", []):
        href = sp.get("href")
        if not href:
            continue

        if href.startswith(client.base_url):
            path = href[len(client.base_url):]
        else:
            path = href

        xml_text = None
        debug_entry = {
            "storage_policy": sp.get("name"),
            "provider_profile_id": sp.get("id"),
            "provider_profile_href": href,
            "request_path": path,
            "xml_dump_file": None,
            "parse_status": "unknown",
            "error": None,
        }

        try:
            xml_text = client.get_xml(path)
            debug_entry["xml_dump_file"] = dump_provider_storage_profile_xml(
                dump_provider_storage_profile_xml_dir,
                sp.get("name"),
                sp.get("id"),
                xml_text,
            )

            parsed = parse_provider_vdc_storage_profile_xml(xml_text)
            if not parsed.get("name"):
                parsed["name"] = sp.get("name")
            if not parsed.get("id"):
                parsed["id"] = sp.get("id")
            if not parsed.get("href"):
                parsed["href"] = sp.get("href")

            provider_storage_profile_details.append(parsed)
            debug_entry["parse_status"] = "ok"
        except Exception as exc:
            provider_storage_profile_details.append({
                "id": sp.get("id"),
                "name": sp.get("name"),
                "href": sp.get("href"),
                "enabled": None,
                "default": None,
                "units": None,
                "limit_mb": None,
            })
            debug_entry["parse_status"] = "error"
            debug_entry["error"] = str(exc)

        provider_storage_profile_debug.append(debug_entry)

    provider_storage_agg = aggregate_provider_storage_profiles(
        admin.get("storage_profiles", []),
        provider_storage_profile_details
    )

    merged_storage_agg = merge_storage_views(tenant_storage_agg, provider_storage_agg)

    published_datastore_capacity_agg = aggregate_datastores(
        query_all_records(client, "datastore")
    )

    return {
        "meta": {
            "status": "ok",
            "api_version": client.api_version,
            "verify_ssl": client.verify_ssl,
        },
        "provider_vdc": {
            "name": admin.get("name"),
            "id": admin.get("id"),
            "vim_server": pvdc.get("vimServer"),
            "nsxt_manager": pvdc.get("nsxTManager"),
        },
        "capacity": {
            "cpu": cpu,
            "cpu_converted": cpu_converted,
            "memory": mem,
            "memory_converted": memory_converted,
        },
        "ratios": {
            "cpu_allocation_ratio": safe_float_div(cpu.get("allocation"), cpu.get("total")),
            "cpu_reservation_pressure": safe_float_div(cpu.get("reserved"), cpu.get("total")),
            "cpu_usage_ratio": safe_float_div(cpu.get("used"), cpu.get("total")),
            "memory_allocation_ratio": safe_float_div(mem.get("allocation"), mem.get("total")),
            "memory_reservation_pressure": safe_float_div(mem.get("reserved"), mem.get("total")),
            "memory_usage_ratio": safe_float_div(mem.get("used"), mem.get("total")),
        },
        "storage_by_policy": {
            "tenant_view": tenant_storage_agg,
            "provider_view": provider_storage_agg,
            "merged_view": merged_storage_agg,
        },
#        "debug": {
#            "provider_storage_profile_xml_dump_dir": dump_provider_storage_profile_xml_dir,
#            "provider_storage_profiles": provider_storage_profile_debug,
#        },
        "published_datastore_capacity": published_datastore_capacity_agg,
    }

# parse_args
# Поддерживаемые параметры:
#    --vcd-url: URL сервера VMware Cloud Director
#    --vcd-api-token: refresh token для аутентификации
#    --verify-ssl: проверять ли SSL-сертификат
#    --api-version: версия API VCD
#    --pvdc-name: имя конкретного Provider VDC
#    --output-file: путь к файлу для сохранения JSON-результата (опционально)
#    ----dump-provider-storage-profile-xml-dir: путь к файлу для сохранения XML-результата (опционально)
def parse_args():
    parser = argparse.ArgumentParser(
        description="VCD Provider VDC collector with tenant/provider storage policy aggregation and XML debug dump"
    )
    parser.add_argument("--vcd-url", required=True)
    parser.add_argument("--vcd-api-token", required=True)
    parser.add_argument("--verify-ssl", required=False, default="true", type=str_to_bool)
    parser.add_argument("--api-version", required=False, default="39.1")
    parser.add_argument("--pvdc-name", required=False, default=None)
    parser.add_argument("--output-file", required=False, default=None)
    parser.add_argument(
        "--dump-provider-storage-profile-xml-dir",
        required=False,
        default=None,
        help="Directory where raw Provider VDC storage profile XML responses will be dumped",
    )
    return parser.parse_args()


def main():
    try:
        args = parse_args()

        client = VCDClient(
            base_url=args.vcd_url,
            api_token=args.vcd_api_token,
            verify_ssl=args.verify_ssl,
            api_version=args.api_version,
        )
        client.authenticate()

        report = build_pvdc_report(
            client,
            pvdc_name=args.pvdc_name,
            dump_provider_storage_profile_xml_dir=args.dump_provider_storage_profile_xml_dir,
        )

        if args.output_file:
            try:
                with open(args.output_file, "w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2, ensure_ascii=False)
            except OSError as exc:
                raise CollectorError(f"Failed to write output file '{args.output_file}': {exc}") from exc

        print(json.dumps(report, ensure_ascii=False))

    except CollectorError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"UNEXPECTED ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()