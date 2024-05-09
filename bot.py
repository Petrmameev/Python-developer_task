import json

from aiogram import Bot, Dispatcher, executor, types

from config import *
from main import aggregate_payments, payments

bot = Bot(token=YOUR_BOT_TOKEN)
dp = Dispatcher(bot)


@dp.message_handler(commands=["start"])
async def send_welcome(message: types.Message):
    user_name = f'<a href="tg://user?id={message.from_user.id}">{message.from_user.full_name}</a>'
    await message.answer(f"Hi {user_name}!", parse_mode="HTML")


@dp.message_handler()
async def aggregate_and_respond(message: types.Message):
    try:
        data_json = json.loads(message.text)
        if (
            "dt_from" in data_json
            and "dt_upto" in data_json
            and "group_type" in data_json
        ):
            result = aggregate_payments(
                payments,
                data_json["dt_from"],
                data_json["dt_upto"],
                data_json["group_type"],
            )
            await message.answer(result)
        else:
            await message.answer(
                """Допустимо отправлять только следующие запросы:
{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}
{"dt_from": "2022-10-01T00:00:00", "dt_upto": "2022-11-30T23:59:00", "group_type": "day"}
{"dt_from": "2022-02-01T00:00:00", "dt_upto": "2022-02-02T00:00:00", "group_type": "hour"}"""
            )
    except json.JSONDecodeError:
        await message.answer(
            """Невалидный запос. Пример запроса:
{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}"""
        )


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
