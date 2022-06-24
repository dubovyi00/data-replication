import psycopg2, sys, time
from SWI import system_work_imitation
from RD import data_replication
from multiprocessing import Process, Queue


def main(login, password):

    try:
        connection = psycopg2.connect(dbname='students', 
                                      user=login, 
                                      password=password, 
                                      host='students.ami.nstu.ru', 
                                      options='-c search_path=pmib8508,public')
    except Exception:
        sys.exit("Не удалось соединиться с базой данных")
    
    try:
        count = int(input("Укажите, через какое количество транзакций выполнять репликацию данных: "))
        time_between = float(input("Укажите промежуток времени между транзакциями при работе системы (в секундах, не менее 0.5): "))
        if time_between < 0.5:
            raise TypeError
    except (TypeError, ValueError):
        sys.exit("Введены ошибочные данные")
    
    
    queue = Queue()
    queue.put(True)
    print("Запускается программа имитации работы системы")
    swi = Process(target=system_work_imitation, args=(connection, count, time_between, queue))
    print("Запускается программа репликации данных")
    rd = Process(target=data_replication, args=(connection, count, queue))
    print("Для выхода нажмите в процессе работы Ctrl+C\n----------------------------------------------------")
    
    swi.start()
    rd.start()
    
    swi.join()
    rd.join()
  
    

if __name__ == '__main__':
    login = input("login: ")
    password = input("password: ")
    main(login, password)
