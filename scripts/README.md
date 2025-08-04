# Scripts Directory

Каталог содержит скрипты для миграции данных из Kaiten в Bitrix24 и управления созданными объектами.

## 📁 Структура каталога

```
scripts/
├── README.md                      # Документация (этот файл)
├── user_migration.py             # Основной мигратор пользователей
├── space_migration.py            # Основной мигратор пространств
├── card_migration.py             # Основной мигратор карточек  
├── custom_fields_migration.py    # Основной мигратор пользовательских полей
├── utils/                        # Утилиты и вспомогательные скрипты
│   ├── get_all_cards.py          # Анализ карточек Kaiten
│   ├── list_kaiten_users.py      # Просмотр пользователей
│   ├── get_existing_groups.py    # Анализ групп Bitrix24
│   ├── delete_all_groups.py      # Массовое удаление групп
│   └── bulk_card_migration.py    # Массовая миграция карточек
└── vps/                          # VPS скрипты и развертывание
    ├── deploy_vps_scripts.ps1    # Единый deploy скрипт
    ├── create_custom_fields_on_vps.py
    ├── update_comment_dates.py
    └── update_group_features.py
```

## 🔄 Основные миграторы

### `user_migration.py`
**Миграция пользователей Kaiten → Bitrix24**

Переносит всех пользователей с email-адресами из Kaiten в Bitrix24. Создает новых пользователей или обновляет существующих.

```bash
python scripts/user_migration.py
```

**Функции:**
- Получает всех пользователей из Kaiten
- Фильтрует только пользователей с email
- Создает новых или обновляет существующих пользователей в Bitrix24
- Сохраняет маппинг ID в `mappings/user_mapping.json`
- Показывает детальную статистику миграции

---

### `custom_fields_migration.py`
**Миграция пользовательских полей Kaiten → Bitrix24**

Переносит пользовательские поля из Kaiten в Bitrix24 через двухэтапный процесс: локально получает данные, на VPS создает поля через SQL.

```bash
python scripts/custom_fields_migration.py --dry-run    # Анализ полей (тестовый режим)
python scripts/custom_fields_migration.py             # Полная миграция
```

**Двухэтапный процесс:**
1. **Локально:** Получает все пользовательские поля из Kaiten API
2. **На VPS:** Создает поля в БД Bitrix24 через прямые SQL запросы

**Функции:**
- Анализ полей в режиме `--dry-run` (показывает типы, количество значений)
- Создание полей со всеми значениями и языковыми версиями
- Автоматическое маппинг Kaiten ID → Bitrix ID полей и значений
- Сохранение маппинга в `mappings/custom_fields_mapping.json`

**Требования:**
- SSH настройки для VPS: `SSH_HOST`, `SSH_USER`, `SSH_KEY_PATH` в `env.txt`
- Доступ к MySQL на VPS сервере

---

### `space_migration.py`
**Миграция пространств Kaiten → Группы Bitrix24**

Переносит пространства (НЕ доски!) из Kaiten в группы Bitrix24 по новой логике.

```bash
python scripts/space_migration.py --list-spaces          # Просмотр доступных пространств
python scripts/space_migration.py                        # Миграция всех подходящих пространств
python scripts/space_migration.py --limit 10             # Первые 10 пространств (тест)
python scripts/space_migration.py --space-id 123         # Конкретное пространство
```

**Логика миграции:**
- Конечные пространства (без дочерних) → группы Bitrix24
- Пространства 2-го уровня (если родитель имеет дочерние) → группы Bitrix24
- Исключение пространств из `config/space_exclusions.py`
- Участники: из пространства Kaiten

**Создает:** `mappings/space_mapping.json`

---

### `card_migration.py`
**Миграция карточек Kaiten → Задачи Bitrix24**

Переносит карточки из указанного пространства Kaiten в задачи соответствующей группы Bitrix24.

```bash
python scripts/card_migration.py --space-id 426722 --list-only    # Просмотр карточек
python scripts/card_migration.py --space-id 426722                # Полная миграция
python scripts/card_migration.py --space-id 426722 --limit 5      # Первые 5 карточек
python scripts/card_migration.py --space-id 426722 --card-id 123  # Конкретная карточка
python scripts/card_migration.py --space-id 426722 --include-archived  # Включая архивные (type: 3)
```

**Функции:**
- Автоматически определяет группу Bitrix24 из `space_mapping.json`
- Переносит карточки с правильными стадиями:
  - `type: 1` (начальные колонки) → стадия "Новые"
  - `type: 2` и другие → стадия "Выполняются" 
  - `type: 3` (финальные) → пропускаются (по умолчанию) или стадия "Сделаны" + STATUS=5 (с `--include-archived`)
- Переносит комментарии, файлы, пользовательские поля
- Поддержка режима просмотра без создания задач

**Параметр `--include-archived`:**
По умолчанию карточки из финальных колонок (type: 3) пропускаются при миграции. 
Используйте этот параметр для конкретных пространств, где необходимо перенести 
и архивные карточки в стадию "Сделаны" со статусом "Завершена" (STATUS=5).

**Техническая информация:**
- **STAGE_ID** - стадия канбана (колонка): "Новые", "Выполняются", "Сделаны"
- **STATUS** - статус задачи в Bitrix24: 1=новая, 2=в работе, 3=ожидает контроля, 5=завершена
- Архивные карточки (type: 3) получают STATUS=5 для корректного отображения как завершенные

---

## 🔧 Утилиты

### `utils/bulk_card_migration.py`
**Массовая миграция карточек для нескольких пространств**

Запускает `card_migration.py` в цикле для указанных пространств. Используется для автоматизации миграции больших объемов данных.

```bash
python scripts/utils/bulk_card_migration.py
```

**Особенности:**
- Содержит список пространств для миграции
- Последовательно обрабатывает каждое пространство
- Показывает прогресс и ошибки для каждого пространства
- Прерывается при критических ошибках

---

### `utils/get_all_cards.py`
**Получение и анализ карточек из Kaiten**

Показывает карточки из Kaiten с возможностью фильтрации и анализа для планирования миграции.

```bash
python scripts/utils/get_all_cards.py                                    # Все карточки системы
python scripts/utils/get_all_cards.py --space-id 426722                  # Карточки пространства
python scripts/utils/get_all_cards.py --space-id 426722 --migration-only # Только для миграции
python scripts/utils/get_all_cards.py --space-id 426722 --limit 500      # Увеличить лимит
```

**Функции:**
- Получает карточки из пространства и его дочерних пространств
- Фильтр карточек для миграции (исключает архивные и `type: 3` - по умолчанию)
- Показывает статистику по типам колонок
- Группировка по целевым стадиям Bitrix24

---

### `utils/list_kaiten_users.py`
**Просмотр пользователей Kaiten**

Выводит список всех пользователей из Kaiten API с основной информацией.

```bash
python scripts/utils/list_kaiten_users.py
```

**Показывает:**
- ID, email, полное имя каждого пользователя
- Статистику: всего, активированных, с именами, с email
- Ограничение вывода до 147 строк для удобства чтения

---

### `utils/get_existing_groups.py`
**Анализ групп в Bitrix24**

Показывает все существующие группы в Bitrix24 с классификацией по источнику создания.

```bash
python scripts/utils/get_existing_groups.py
```

**Анализирует:**
- Системные группы (ID 1-3)
- Группы от space-миграции (ID 4-69) - старая логика
- Группы от board-миграции (ID 70+) - новая логика
- Выдает рекомендации по дальнейшим действиям

---

### `utils/delete_all_groups.py`
**Массовое удаление групп из Bitrix24**

Удаляет ВСЕ группы в Bitrix24 кроме указанных в исключениях.

```bash
python scripts/utils/delete_all_groups.py                     # Удалить все кроме ID 1,2
python scripts/utils/delete_all_groups.py --exclude 1 2 5     # Удалить все кроме ID 1,2,5
python scripts/utils/delete_all_groups.py --dry-run           # Показать план без удаления
```

**Безопасность:**
- По умолчанию сохраняет группы с ID 1 и 2
- Режим dry-run для предварительного просмотра
- Требует строгое подтверждение (ввод 'DELETE')
- Показывает детальную статистику

---

## 📁 Создаваемые файлы

Скрипты сохраняют результаты в каталоге `mappings/`:
- `user_mapping.json` - соответствие ID пользователей Kaiten ↔ Bitrix24
- `space_mapping.json` - соответствие ID пространств Kaiten ↔ групп Bitrix24
- `card_mapping.json` - соответствие ID карточек Kaiten ↔ задач Bitrix24
- `custom_fields_mapping.json` - соответствие ID пользовательских полей Kaiten ↔ Bitrix24
- `custom_properties_cache.json` - кеш пользовательских полей Kaiten (автоматический)

## 🔄 Рекомендуемый порядок выполнения

1. **Анализ данных:**
   ```bash
   python scripts/utils/list_kaiten_users.py
   python scripts/utils/get_existing_groups.py
   python scripts/space_migration.py --list-spaces
   python scripts/custom_fields_migration.py --dry-run
   ```

2. **Полная миграция:**
   ```bash
   python scripts/user_migration.py                    # 1. Пользователи
   python scripts/custom_fields_migration.py           # 2. Пользовательские поля
   python scripts/space_migration.py                   # 3. Пространства → группы
   python scripts/card_migration.py --space-id <ID>    # 4. Карточки → задачи
   ```

3. **Тестирование (рекомендуется):**
   ```bash
   python scripts/custom_fields_migration.py --dry-run
   python scripts/space_migration.py --limit 5
   python scripts/card_migration.py --space-id <ID> --list-only
   python scripts/card_migration.py --space-id <ID> --limit 5
   python scripts/card_migration.py --space-id <ID> --list-only --include-archived  # Проверка архивных
   ```

## 🛠️ VPS Скрипты

### Новая архитектура

Все VPS скрипты теперь размещаются в единой папке на сервере: `/root/kaiten-vps-scripts/`

**Развертывание:**
```powershell
.\scripts\vps\deploy_vps_scripts.ps1
```

**VPS скрипты:**
- `create_custom_fields_on_vps.py` - создание пользовательских полей через SQL
- `update_comment_dates.py` - обновление дат комментариев в БД
- `update_group_features.py` - управление возможностями рабочих групп

**Использование на VPS:**
```bash
# Пользовательские поля
python3 /root/kaiten-vps-scripts/create_custom_fields_on_vps.py

# Даты комментариев  
python3 /root/kaiten-vps-scripts/update_comment_dates.py '{"601": "2025-07-08 14:22:00"}'

# Возможности групп
python3 /root/kaiten-vps-scripts/update_group_features.py --view-group 38
python3 /root/kaiten-vps-scripts/update_group_features.py --update-all
``` 