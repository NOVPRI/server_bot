from service.tgSend import message_to as msg
from subprocess import Popen
filename = 'bot.py'


while True:
    print("Запуск бота или попытка восстановить сессию")
    msg("Запуск бота или попытка восстановить сессию")
    p = Popen("python " + filename, shell=True)
    p.wait()
