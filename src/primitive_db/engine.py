import prompt


def welcome():
    print('Первая попытка запустить проект!\n')
    print('***')
    print('<command> exit - выйти из программы')
    print('<command> help - справочная информация')
    while True:
        command = prompt.string('Введите команду: ')
        if command == 'exit':
            print('Программа завершена.')
            break
        elif command == 'help':
            print('Справочная информация:')
            print('- Команда "help": показывает это сообщение.')
            print('- Команда "exit": завершает программу.\n')
        else:
            print('Неизвестная команда. Введите "help", '
                  'чтобы посмотреть доступные команды.\n')

