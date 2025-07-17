#!/usr/bin/env python3
"""
Улучшенный скрипт для получения примеров данных из Kaiten API.
Учитывает контекстно-зависимые ресурсы и создает заглушки для недоступных данных.
"""

import requests
import json
import os
from typing import Dict, List, Any, Optional
import time


class KaitenEnhancedFetcher:
    """Улучшенный класс для получения данных из Kaiten API"""
    
    def __init__(self, base_url: str, api_token: str):
        self.base_url = base_url.rstrip('/')
        self.api_token = api_token
        self.headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Кеш для ID-ов, полученных из предыдущих запросов
        self.space_ids = []
        self.board_ids = []
        self.card_ids = []
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None, method: str = 'GET') -> Optional[Dict]:
        """Выполняет запрос к API"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            print(f"  {method} -> {url}")
            
            if method.upper() == 'GET':
                response = self.session.get(url, params=params, timeout=30)
            elif method.upper() == 'POST':
                response = self.session.post(url, json=params, timeout=30)
            else:
                print(f"  Неподдерживаемый метод: {method}")
                return None
            
            print(f"  Статус: {response.status_code}")
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code in [401, 403]:
                print(f"  Нет доступа к ресурсу")
                return None
            elif response.status_code == 404:
                print(f"  Ресурс не найден")
                return None
            elif response.status_code == 405:
                print(f"  Метод не разрешен")
                return None
            else:
                print(f"  Ошибка API: {response.status_code}")
                print(f"  Ответ: {response.text[:200]}...")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"  Ошибка сети: {e}")
            return None

    def get_boards_from_spaces(self) -> List[Dict]:
        """Получает доски через контекст пространств"""
        print("\n=== Получение досок через пространства ===")
        
        if not self.space_ids:
            print("  Нет доступных ID пространств")
            return []
        
        all_boards = []
        for space_id in self.space_ids[:3]:  # Берем первые 3 пространства
            endpoint = f'api/v1/spaces/{space_id}/boards'
            data = self._make_request(endpoint)
            if data and isinstance(data, list):
                all_boards.extend(data)
                # Сохраняем board_ids для дальнейшего использования
                for board in data:
                    if board.get('id'):
                        self.board_ids.append(board['id'])
        
        return all_boards[:10]  # Возвращаем максимум 10 записей

    def get_context_dependent_resource(self, resource_name: str, base_endpoint: str, context_ids: List[str], context_param: str) -> List[Dict]:
        """Получает ресурсы, которые зависят от контекста (доска, пространство и т.д.)"""
        print(f"\n=== Получение {resource_name} ===")
        
        if not context_ids:
            print(f"  Нет доступных {context_param} для получения {resource_name}")
            return []
        
        all_items = []
        
        # Пробуем несколько context_id
        for context_id in context_ids[:3]:
            endpoint = f'{base_endpoint.format(context_id=context_id)}'
            data = self._make_request(endpoint)
            
            if data:
                items = data if isinstance(data, list) else data.get('data', [])
                if items:
                    all_items.extend(items)
                    break  # Если нашли данные, прекращаем поиск
        
        return all_items[:10]



    def save_sample_data(self, data: List[Dict], filename: str, max_records: int = 10):
        """Сохраняет примеры данных в JSON файл"""
        if not data:
            print(f"  Нет данных для сохранения в {filename}")
            return
        
        # Берем только первые max_records записей
        sample_data = data[:max_records] if isinstance(data, list) else [data]
        
        # Получаем директорию скрипта и создаем путь к каталогу kaiten
        script_dir = os.path.dirname(os.path.abspath(__file__))
        kaiten_dir = os.path.join(script_dir, 'kaiten')
        os.makedirs(kaiten_dir, exist_ok=True)
        
        filepath = os.path.join(kaiten_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(sample_data, f, ensure_ascii=False, indent=2)
            
            print(f"  ✓ Сохранено {len(sample_data)} записей в {filepath}")
            
        except Exception as e:
            print(f"  ✗ Ошибка сохранения {filepath}: {e}")

    def fetch_main_resources(self):
        """Получает основные ресурсы"""
        resources = {
            'spaces': 'api/v1/spaces',
            'cards': 'api/v1/cards?limit=10',
            'users': 'api/v1/users',
            'tags': 'api/v1/tags',
            'card_types': 'api/v1/card-types'
        }
        
        collected_data = {}
        
        for resource_name, endpoint in resources.items():
            print(f"\n=== Получение {resource_name} ===")
            data = self._make_request(endpoint)
            
            if data:
                items = data if isinstance(data, list) else data.get('data', data.get(resource_name, []))
                if items:
                    collected_data[resource_name] = items
                    self.save_sample_data(items, f'{resource_name}.json')
                    
                    # Сохраняем ID для контекстных запросов
                    if resource_name == 'spaces':
                        self.space_ids = [item.get('id') for item in items if item.get('id')]
                    elif resource_name == 'cards':
                        self.card_ids = [item.get('id') for item in items if item.get('id')]
            
            time.sleep(1)
        
        return collected_data

    def fetch_context_dependent_resources(self):
        """Получает ресурсы, зависящие от контекста"""
        collected_data = {}
        
        # Доски через пространства
        boards = self.get_boards_from_spaces()
        if boards:
            collected_data['boards'] = boards
            self.save_sample_data(boards, 'boards.json')
        
        # Ресурсы, связанные с досками
        board_resources = [
            ('columns', 'api/v1/boards/{context_id}/columns'),
            ('lanes', 'api/v1/boards/{context_id}/lanes'),
            ('subcolumns', 'api/v1/boards/{context_id}/subcolumns')
        ]
        
        for resource_name, endpoint_pattern in board_resources:
            items = self.get_context_dependent_resource(
                resource_name, endpoint_pattern, self.board_ids, 'board_ids'
            )
            if items:
                collected_data[resource_name] = items
                self.save_sample_data(items, f'{resource_name}.json')
            time.sleep(1)
        
        # Ресурсы, связанные с карточками
        if self.card_ids:
            card_resources = [
                ('card_children', 'api/v1/cards/{context_id}/children'),
                ('card_checklists', 'api/v1/cards/{context_id}/checklists'),
                ('card_files', 'api/v1/cards/{context_id}/files')
            ]
            
            for resource_name, endpoint_pattern in card_resources:
                items = self.get_context_dependent_resource(
                    resource_name, endpoint_pattern, self.card_ids, 'card_ids'
                )
                if items:
                    collected_data[resource_name] = items
                    self.save_sample_data(items, f'{resource_name}.json')
                time.sleep(1)
        
        return collected_data




def main():
    """Основная функция"""
    
    # Данные доступа из файла Access.md
    BASE_URL = "https://imena.kaiten.ru"
    API_TOKEN = "f520225c-f92a-4499-96ab-87c1ce431ee1"
    
    print("🚀 Запуск улучшенного скрипта получения данных из Kaiten API")
    print(f"URL: {BASE_URL}")
    print(f"Токен: {API_TOKEN[:10]}...")
    
    fetcher = KaitenEnhancedFetcher(BASE_URL, API_TOKEN)
    
    # 1. Получаем основные ресурсы
    print("\n" + "="*60)
    print("ЭТАП 1: Получение основных ресурсов")
    print("="*60)
    
    main_data = fetcher.fetch_main_resources()
    
    # 2. Получаем контекстно-зависимые ресурсы
    print("\n" + "="*60)
    print("ЭТАП 2: Получение контекстно-зависимых ресурсов")
    print("="*60)
    
    context_data = fetcher.fetch_context_dependent_resources()
    

    
    # Итоговая статистика
    print("\n" + "="*60)
    print("📊 ИТОГОВАЯ СТАТИСТИКА")
    print("="*60)
    
    all_data = {**main_data, **context_data}
    
    print(f"✅ Получено из API: {len(all_data)} типов ресурсов")
    for resource_name, data in all_data.items():
        count = len(data) if isinstance(data, list) else 1
        print(f"   • {resource_name}: {count} записей")
    
    print(f"\n📁 Все файлы сохранены в project-doc/data_samples/kaiten/")
    
    # Проверяем созданные файлы
    created_files = []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    kaiten_dir = os.path.join(script_dir, 'kaiten')
    if os.path.exists(kaiten_dir):
        created_files = [f for f in os.listdir(kaiten_dir) if f.endswith('.json')]
    
    print(f"\n📋 Созданные файлы с реальными данными:")
    for filename in sorted(created_files):
        print(f"   ✓ {filename}")
    
    print(f"\n🎉 Успешно получены реальные данные для {len(created_files)} типов ресурсов!")


if __name__ == "__main__":
    main() 