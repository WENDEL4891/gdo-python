import datetime
import pytz

def log_inicio_fim_metodo(msg):
    data_hora_atual = datetime.datetime.now()
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    data_hora_atual_sao_paulo = data_hora_atual.astimezone(fuso_horario)
    data_hora_atual_sao_paulo_formatada = data_hora_atual_sao_paulo.strftime('%d/%m/%Y - %H:%M:%S')
    print('[{}]\t{}\n'.format(data_hora_atual_sao_paulo_formatada, msg))

def log(msg):
    data_hora_atual = datetime.datetime.now()
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    data_hora_atual_sao_paulo = data_hora_atual.astimezone(fuso_horario)
    data_hora_atual_sao_paulo_formatada = data_hora_atual_sao_paulo.strftime('%d/%m/%Y - %H:%M:%S')
    print('\t[{}]\t{}'.format(data_hora_atual_sao_paulo_formatada, msg))