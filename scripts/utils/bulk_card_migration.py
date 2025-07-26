#!/usr/bin/env python3
"""
Простой скрипт для миграции карточек всех указанных пространств.
Запускает card_migration.py в цикле для каждого пространства.
"""

import subprocess
import sys

# Список пространств для миграции
SPACES = {
    "482281": "34",
    "426721": "43", 
    "457440": "44",
    "426720": "45",
    "434323": "46",
    "474215": "47",
    "457420": "48",
    "426722": "49",
    "478331": "50"
}

if __name__ == "__main__":
    for space_id, group_id in SPACES.items():
        print(f"\n🚀 Миграция пространства {space_id} -> группа {group_id}")
        result = subprocess.run([
            sys.executable, "scripts/card_migration.py", 
            "--space-id", space_id
        ])
        if result.returncode != 0:
            print(f"❌ Ошибка при миграции пространства {space_id}") 