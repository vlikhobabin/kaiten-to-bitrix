import asyncio
import os
import sys
from typing import List

from loguru import logger

# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from connectors.bitrix_client import BitrixClient
from connectors.kaiten_client import KaitenClient
from models.bitrix_models import BitrixUser
from models.kaiten_models import KaitenUser
from transformers.user_transformer import UserTransformer


async def main():
    """
    Тестовый сценарий для миграции пользователей:
    1. Получает пользователей из Kaiten и Bitrix24.
    2. Берет первых 10 пользователей из Kaiten.
    3. Для каждого: если найден в Bitrix24 - обновляет, если не найден - создает.
    4. Обновляет список пользователей Bitrix24.
    5. Находит или создает тестовую рабочую группу в Bitrix24.
    6. Добавляет всех обработанных пользователей в группу.
    """
    logger.info("🚀 Запуск улучшенной миграции пользователей...")

    kaiten_client = KaitenClient()
    bitrix_client = BitrixClient()
    
    # --- 1. Получение данных ---
    logger.info("Получение пользователей из Kaiten...")
    kaiten_users: List[KaitenUser] = await kaiten_client.get_users()
    logger.success(f"Получено {len(kaiten_users)} пользователей из Kaiten.")

    logger.info("Получение пользователей из Bitrix24...")
    bitrix_users: List[BitrixUser] = await bitrix_client.get_users({"ACTIVE": "Y"})
    logger.success(f"Получено {len(bitrix_users)} пользователей из Bitrix24.")

    # --- 2. Работа с первыми 10 пользователями ---
    users_to_process = kaiten_users[:10]  # Берем только первых 10
    logger.info(f"Обрабатываем первых {len(users_to_process)} пользователей из Kaiten...")
    
    user_transformer = UserTransformer(bitrix_users)
    processed_users_count = 0
    created_count = 0
    updated_count = 0

    for kaiten_user in users_to_process:
        if not kaiten_user.email:
            logger.warning(f"Пользователь {kaiten_user.full_name} не имеет email, пропускаем")
            continue
        
        # Подготавливаем данные для Bitrix24
        user_data = user_transformer.kaiten_to_bitrix_data(kaiten_user)
        if not user_data:
            continue
        
        # Проверяем, существует ли пользователь в Bitrix24
        existing_user = user_transformer.transform(kaiten_user)
        
        if existing_user:
            # Пользователь найден - обновляем его данные
            logger.info(f"Обновление данных пользователя {kaiten_user.full_name} (ID: {existing_user.ID})...")
            success = await bitrix_client.update_user(existing_user.ID, user_data)
            if success:
                updated_count += 1
        else:
            # Пользователь не найден - создаем нового
            logger.info(f"Создание нового пользователя {kaiten_user.full_name}...")
            new_user_id = await bitrix_client.create_user(user_data)
            if new_user_id:
                created_count += 1
        
        processed_users_count += 1

    logger.success(f"✅ Обработано {processed_users_count} пользователей: создано {created_count}, обновлено {updated_count}")

    # --- 3. Обновление данных после создания/обновления ---
    logger.info("Повторное получение пользователей из Bitrix24 для обновления карты...")
    bitrix_users = await bitrix_client.get_users({"ACTIVE": "Y"})
    user_transformer = UserTransformer(bitrix_users)
    logger.success(f"Карта пользователей обновлена. Всего в Bitrix24: {len(bitrix_users)}.")

    # --- 4. Сопоставление обработанных пользователей для добавления в группу ---
    processed_user_ids = []
    for kaiten_user in users_to_process:
        user_id = user_transformer.get_user_id(kaiten_user)
        if user_id:
            processed_user_ids.append(user_id)

    logger.success(f"Найдено {len(processed_user_ids)} обработанных пользователей для добавления в группу.")

    # --- 5. Работа с рабочей группой ---
    group_name = "Проверка миграции Kaiten"
    test_group = None
    logger.info(f"Попытка создать/найти тестовую группу '{group_name}'...")
    
    workgroups = await bitrix_client.get_workgroup_list()
    for group in workgroups:
        if group["NAME"] == group_name:
            test_group = group
            break

    if test_group:
        logger.info(f"Тестовая группа '{group_name}' уже существует с ID: {test_group['ID']}.")
        group_id = str(test_group["ID"])
    else:
        logger.info(f"Группа '{group_name}' не найдена, создаем новую...")
        group_data = {
            "NAME": group_name,
            "OWNER_ID": "1",  # ID администратора по умолчанию как строка
            "USER_IDS": ["1"],
        }
        created_group = await bitrix_client.create_workgroup(group_data)
        if created_group and "ID" in created_group:
            group_id = str(created_group["ID"])
            logger.success(f"Группа '{group_name}' успешно создана с ID: {group_id}.")
        else:
            logger.error("Не удалось создать тестовую группу.")
            return

    # --- 6. Синхронизация состава группы ---
    existing_group_users = await bitrix_client.get_workgroup_users(group_id)
    existing_user_ids = {str(user["USER_ID"]) for user in existing_group_users}
    logger.info(f"В группе уже состоят пользователи (ID): {existing_user_ids}")

    user_ids_to_add_in_group = [
        uid for uid in processed_user_ids if str(uid) not in existing_user_ids
    ]

    if not user_ids_to_add_in_group:
        logger.info("Все обработанные пользователи уже состоят в группе. Добавление не требуется.")
    else:
        logger.info(f"Необходимо добавить пользователей (ID): {user_ids_to_add_in_group}")
        for user_id in user_ids_to_add_in_group:
            await bitrix_client.add_user_to_workgroup(group_id, user_id)

    # --- 7. Финальная проверка и отчет ---
    final_group_users = await bitrix_client.get_workgroup_users(group_id)
    final_user_ids = sorted([str(user["USER_ID"]) for user in final_group_users])
    
    logger.info("=" * 50)
    logger.success("🎉 Улучшенная миграция пользователей успешно завершена!")
    logger.info(f"Обработано пользователей: {processed_users_count}")
    logger.info(f"Создано новых: {created_count}")
    logger.info(f"Обновлено существующих: {updated_count}")
    logger.info(f"Проверьте группу '{group_name}' в вашем Bitrix24.")
    logger.info(f"ID группы: {group_id}")
    logger.info(f"Итоговый состав группы (ID Bitrix24): {final_user_ids}")
    logger.info("=" * 50)


if __name__ == "__main__":
    asyncio.run(main()) 