# Scripts Directory

Каталог содержит скрипты для миграции данных из Kaiten в Bitrix24 и управления созданными объектами.

## 🔄 Скрипты миграции

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
```

**Функции:**
- Автоматически определяет группу Bitrix24 из `space_mapping.json`
- Переносит карточки с правильными стадиями:
  - `type: 1` (начальные колонки) → стадия "Новые"
  - `type: 2` и другие → стадия "Выполняются" 
  - `type: 3` (финальные) → пропускаются
- Переносит комментарии, файлы, пользовательские поля
- Поддержка режима просмотра без создания задач

---

## 🔍 Скрипты анализа и просмотра

### `get_all_cards.py`
**Получение и анализ карточек из Kaiten**

Показывает карточки из Kaiten с возможностью фильтрации и анализа для планирования миграции.

```bash
python scripts/get_all_cards.py                                    # Все карточки системы
python scripts/get_all_cards.py --space-id 426722                  # Карточки пространства
python scripts/get_all_cards.py --space-id 426722 --migration-only # Только для миграции
python scripts/get_all_cards.py --space-id 426722 --limit 500      # Увеличить лимит
```

**Функции:**
- Получает карточки из пространства и его дочерних пространств
- Фильтр карточек для миграции (исключает архивные и `type: 3`)
- Показывает статистику по типам колонок
- Группировка по целевым стадиям Bitrix24

---

### `list_kaiten_users.py`
**Просмотр пользователей Kaiten**

Выводит список всех пользователей из Kaiten API с основной информацией.

```bash
python scripts/list_kaiten_users.py
```

**Показывает:**
- ID, email, полное имя каждого пользователя
- Статистику: всего, активированных, с именами, с email
- Ограничение вывода до 147 строк для удобства чтения

---

### `get_existing_groups.py`
**Анализ групп в Bitrix24**

Показывает все существующие группы в Bitrix24 с классификацией по источнику создания.

```bash
python scripts/get_existing_groups.py
```

**Анализирует:**
- Системные группы (ID 1-3)
- Группы от space-миграции (ID 4-69) - старая логика
- Группы от board-миграции (ID 70+) - новая логика
- Выдает рекомендации по дальнейшим действиям

---

## 🗑️ Скрипты удаления

### `delete_all_groups.py`
**Массовое удаление групп из Bitrix24**

Удаляет ВСЕ группы в Bitrix24 кроме указанных в исключениях.

```bash
python scripts/delete_all_groups.py                     # Удалить все кроме ID 1,2
python scripts/delete_all_groups.py --exclude 1 2 5     # Удалить все кроме ID 1,2,5
python scripts/delete_all_groups.py --dry-run           # Показать план без удаления
```

**Безопасность:**
- По умолчанию сохраняет группы с ID 1 и 2
- Режим dry-run для предварительного просмотра
- Требует строгое подтверждение (ввод 'DELETE')
- Показывает детальную статистику

---

## 🛠️ Служебные скрипты

### `update_comment_dates.py`
**Обновление дат комментариев в БД Bitrix24**

Исправляет даты комментариев через прямой SQL-запрос к MySQL БД Bitrix24. Выполняется на VPS сервере.

```bash
python3 update_comment_dates.py '{"comment_id": "2025-07-08 14:22:00"}'
```

**Особенности:**
- Прямое подключение к MySQL БД Bitrix24
- Использует конфигурацию из `/root/.my.cnf`
- Обновляет поле `POST_DATE` в таблице `b_forum_message`
- Поддержка ISO и MySQL форматов дат
- Транзакционное обновление с откатом при ошибках

---

### `deploy_simple.ps1`
**Развертывание скрипта на VPS (PowerShell)**

Копирует `update_comment_dates.py` на VPS сервер через SSH для работы с БД Bitrix24.

```powershell
.\scripts\deploy_simple.ps1
```

**Требования:**
- SSH настройки в файле `env.txt`: `SSH_HOST`, `SSH_USER`, `SSH_KEY_PATH_PUTTY`, `VPS_SCRIPT_PATH`
- Наличие pscp.exe и plink.exe (PuTTY tools)
- SSH ключ в формате PuTTY (.ppk)

**Функции:**
- Чтение SSH настроек из переменных окружения
- Копирование файла через pscp
- Установка прав на выполнение через plink
- Проверка всех зависимостей перед развертыванием

---

## 📁 Создаваемые файлы

Скрипты сохраняют результаты в каталоге `mappings/`:
- `user_mapping.json` - соответствие ID пользователей Kaiten ↔ Bitrix24
- `space_mapping.json` - соответствие ID пространств Kaiten ↔ групп Bitrix24
- `card_mapping.json` - соответствие ID карточек Kaiten ↔ задач Bitrix24
- `custom_properties_cache.json` - кеш пользовательских полей Kaiten

## 🔄 Рекомендуемый порядок выполнения

1. **Анализ данных:**
   ```bash
   python scripts/list_kaiten_users.py
   python scripts/get_existing_groups.py
   python scripts/space_migration.py --list-spaces
   ```

2. **Полная миграция:**
   ```bash
   python scripts/user_migration.py
   python scripts/space_migration.py
   python scripts/card_migration.py --space-id <ID>
   ```

3. **Тестирование (рекомендуется):**
   ```bash
   python scripts/space_migration.py --limit 5
   python scripts/card_migration.py --space-id <ID> --list-only
   python scripts/card_migration.py --space-id <ID> --limit 5
   ``` 