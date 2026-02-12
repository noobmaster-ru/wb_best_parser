# wb_best_parser

Парсер Telegram-каналов на `Telethon`: читает новые посты в исходных каналах, оценивает "интересность" и публикует отобранные предложения в `@best_wb_hits`.

## 1) Подготовка

1. Создайте Telegram API credentials на [my.telegram.org](https://my.telegram.org): `api_id`, `api_hash`.
2. Скопируйте конфиг:

```bash
cp .env.example .env
```

3. Заполните `.env`.
4. Заполните `targets.txt` (по одному каналу/ID в строке), например:

```text
@channel_one
@channel_two
-1001234567890
```

## 2) Первичная авторизация аккаунта (создание сессии)

Нужно сделать один раз, чтобы Telethon сохранил пользовательскую сессию.

### Локально

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
python scripts/auth_session.py --api-id <TG_API_ID> --api-hash <TG_API_HASH> --session sessions/user
```

После этого появится файл `sessions/user.session`.

## 3) Запуск

### Локально

```bash
python -m wb_best_parser
```

### Через Docker Compose

```bash
docker compose up -d --build
```

Логи:

```bash
docker compose logs -f parser
```

## Логика фильтрации

Каждое сообщение получает `score`:

- `+N` за совпадения `INCLUDE_KEYWORDS`
- `+2`, если есть цена `<= 990`
- `+1`, если есть цена `<= 1490`
- `+2`, если скидка `>= 40%`
- `+1`, если скидка `>= 25%`
- если найдено слово из `EXCLUDE_KEYWORDS`, сообщение отбрасывается

Публикация происходит, если `score >= MIN_SCORE`.

## Важные замечания

- Источники читаются из `TARGETS_FILE` (по умолчанию `targets.txt`). Если файл пустой/не найден, используется `SOURCE_CHATS` из `.env`.
- Для чтения приватных/закрытых каналов ваш аккаунт должен быть подписан на них.
- Для публикации в `TARGET_CHAT` аккаунт должен иметь права писать в канал.
- Если пост содержит медиа, парсер отправит текст с пояснением и отдельно форварднет медиа-сообщение.
- Для безопасной отладки включите `DRY_RUN=true`.
# wb_best_parser
