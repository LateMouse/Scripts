# VCD Capacity Reporter

Скрипт для сбора и агрегации метрик из **VMware Cloud Director** по **Provider VDC**.  
Подключается к API vCloud Director, получает данные по вычислительным ресурсам и storage, рассчитывает коэффициенты использования и формирует итоговый JSON-отчет.

---
## Сценарии применения

Скрипт может быть полезен в следующих случаях:

- подготовка capacity report для облачной платформы;
- анализ использования ресурсов Provider VDC;
- проверка storage pressure перед расширением площадки;
- регулярная выгрузка метрик для аудита;
- подготовка данных для внешней системы мониторинга или BI;
- сравнение аллокации и фактического использования ресурсов.

---

## Особенности и ограничения

### 1. Если `--pvdc-name` не задан
Скрипт выберет **первый Provider VDC** из ответа API.  
Это удобно для тестирования, но в production-сценарии лучше всегда явно указывать имя нужного Provider VDC.

### 2. Часть данных собирается глобально через query API
Агрегация по `adminOrgVdcStorageProfile` и `datastore` берется через query API.  
Если в инфраструктуре несколько Provider VDC, нужно отдельно проверить, соответствует ли такая агрегация логике отчетности.

### 3. Скрипт рассчитан на доступность конкретных API VCD
Если в вашей версии VMware Cloud Director изменены API-поля или поведение endpoint, может потребоваться доработка парсинга.
---

## Параметры запуска

### Обязательные параметры

#### `--vcd-url`
URL VMware Cloud Director.

Пример:

```bash
--vcd-url https://vcd.example.com
```

---

#### `--vcd-api-token`
API token (refresh token), который используется для получения access token.

Пример:

```bash
--vcd-api-token eyJhbGciOi...
```

---

### Необязательные параметры

#### `--verify-ssl`
Включает или отключает проверку SSL-сертификата сервера.

Поддерживаемые значения:

- `true`
- `false`
- `yes`
- `no`
- `1`
- `0`
- `on`
- `off`

Пример:

```bash
--verify-ssl false
```

---

#### `--api-version`
Версия API VMware Cloud Director.

Значение по умолчанию:

```text
39.1
```

Пример:

```bash
--api-version 39.1
```

---

#### `--pvdc-name`
Имя Provider VDC, по которому нужно строить отчёт.

Если параметр не указан, будет использован **первый Provider VDC**, возвращённый API.

Пример:

```bash
--pvdc-name ProviderVDC-01
```

---

#### `--output-file`
Путь к файлу, в который нужно сохранить JSON-отчёт.

Пример:

```bash
--output-file report.json
```

---

## Примеры запуска

### Простейший запуск

```bash
python3 vcd_pvdc_capacity_reporter.py \
  --vcd-url https://vcd.example.com \
  --vcd-api-token YOUR_REFRESH_TOKEN
```

---

### Запуск с выбором конкретного Provider VDC

```bash
python3 vcd_pvdc_capacity_reporter.py \
  --vcd-url https://vcd.example.com \
  --vcd-api-token YOUR_REFRESH_TOKEN \
  --pvdc-name ProviderVDC-01
```

---

### Запуск без проверки SSL

```bash
python3 vcd_pvdc_capacity_reporter.py \
  --vcd-url https://vcd.example.com \
  --vcd-api-token YOUR_REFRESH_TOKEN \
  --verify-ssl false
```

---

### Запуск с сохранением результата в файл

```bash
python3 vcd_pvdc_capacity_reporter.py \
  --vcd-url https://vcd.example.com \
  --vcd-api-token YOUR_REFRESH_TOKEN \
  --pvdc-name ProviderVDC-01 \
  --output-file report.json
```

---

## Общая схема работы

Логика скрипта выглядит так:

1. Пользователь запускает скрипт с параметрами командной строки.
2. Скрипт создает API-клиент для VMware Cloud Director.
3. Выполняется аутентификация по refresh token.
4. Скрипт получает список Provider VDC.
5. Выбирается конкретный Provider VDC:
   - по имени, если указан `--pvdc-name`;
   - первый в списке, если имя не задано.
6. По выбранному Provider VDC извлекается UUID.
7. Скрипт получает административное XML-представление Provider VDC.
8. Из XML извлекаются compute capacity и storage profiles.
9. Дополнительно через query API собираются:
   - `adminOrgVdcStorageProfile`;
   - `datastore`.
10. Все данные агрегируются и приводятся к удобному формату.
11. Итоговый отчёт печатается в stdout.
12. Если указан `--output-file`, отчет сохраняется в файл.

---

### Примеры ситуаций, при которых возможна ошибка

- недоступен VMware Cloud Director;
- неверный URL;
- неверный API token;
- access token не вернулся в ответе;
- XML или JSON от API не удалось разобрать;
- указанный Provider VDC не найден;
- нет прав на чтение данных;
- невозможно записать результат в файл.

---

## Troubleshooting

### Ошибка: `Authentication request failed`
Проверьте:

- доступность URL VMware Cloud Director;
- корректность токена;
- сетевую связность;
- SSL-сертификат.

---

### Ошибка: `Authentication succeeded but access_token is missing`
Проверьте:

- действительно ли передан корректный refresh token;
- не изменился ли формат ответа API;
- не ограничены ли права текущего пользователя.

---

### Ошибка: `Provider VDC named '...' not found`
Проверьте:

- правильность имени Provider VDC;
- регистр символов;
- права на просмотр данного объекта.

---

### Ошибка парсинга XML
Проверьте:

- что endpoint действительно возвращает XML;
- что версия API совместима;
- что ответ API не содержит HTML-страницу ошибки вместо XML.

---

### Пустые или `null` значения в ratios
Это возможно, если:

- API не вернул соответствующие поля;
- total равно 0;
- часть полей недоступна в вашей среде.

---
