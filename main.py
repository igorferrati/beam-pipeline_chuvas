import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pepiline_options import PipelineOptions
import re
from apache_beam.io.textio import WriteToText


pipeline_options = PipelineOptions(arg = None)
pipeline = beam.Pipeline(options = pipeline_options)

colunas_denguetxt = [
                    'id',
                    'data_iniSE',
                    'casos',
                    'ibge_code',
                    'cidade',
                    'uf',
                    'cep',
                    'latitude',
                    'longitude']


def txt_list(elemento, delimitador='|'):
    """
    Recebe texto e um delimitador
    Retorna uma lista de elementos de acordo com o delimitador
    """
    return elemento.split(delimitador)


def list_dicionario(elemento, colunas):
    """
    Recebe cada linha do arquivo
    Referencia com a coluna(key) do dict
    Recebe 2 lista, retorna um dicionário
    """
    return dict(zip(colunas, elemento))

def trata_data(elemento):
    """
    Recebe um dicionário e altera a key ANO-MÊS
    Retorna o mesmo dict com alterações ANO-MÊS
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def uf_key(elemento):
    """
    Receber dicionário
    Retorna uma tupla com estado (uf) e elemento (UF, dicionário)
    """
    key = elemento['uf']
    return (key, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('UF', [{}, {}, {},...])
    Retorna um tupla ('UF-ANO-MES', qtd) = ('RS-2014-12', 8.0)
    """
    uf, registros = elemento #uf = estado, regostros = lista de dicts
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])): #Lógica para tratar espaços vazios do arquivo
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))          
            #retorna todos valores dentro do for
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

def casos_chuva(elemento):
    """
    Recebe uma lista de elementos
    Retorna uma tupla ('UF-ANO-MES', chuva_mm) = ('RS-2014-12', 3.0)
    """
    data, mm_chuva, uf = elemento   #Descompactação do chuvas.csv ['2016-01-24', '4.2', 'TO']
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}' #['TO-2016-01']
    if float(mm_chuva) < 0:   # tratando dados negativos
        mm_chuva = 0.0
    else:
        mm_chuva = float(mm_chuva)
    return chave, mm_chuva


def trata_mm(elemento):
    """
    Recebe uma tupla
    Retorna uma tupla com round no valor
    """
    chave, mm_chuva = elemento
    return (chave, round(mm_chuva, 1))

def filtro_campos_vazio(elemento):
    """
    Remove elementos que tenham chaves vazias
    Verifica se existem elementos dentro das listas
    Recebe uma tupla 
    Retorna uam tupla 
    """
    chave, dados = elemento    #descompactando
    if all([
        dados['chuvas'],
        dados['dengue']
        ]):
        return True
    return False

def descompactar_elem(elemento):
    """
    Recebe uma tupla: ('CE-2015-11', {'chuvas': [0.4], 'dengue': [21.0]})
    Retorna descompactação: ('CE', '2015', '11', '0.4', '21.0')
    """
    chave, dados = elemento
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, int, int, str(chuva), str(dengue) 


def preparar_csv(elemento, delimitador = ';'):
    """
    Recebe uma tupla ('CE', '2015', '11', '0.4', '21,0')
    Retorna uma stringo delimitada -> "CE;2015;11;0.4;21.0
    """
    return f'{delimitador}'.join(elemento)


#Criando pipeline para tratar casos de dengue
dengue = (
    pipeline
    | 'Leitura do dataset de dengue' >> 
        ReadFromText('sample_casos_dengue.txt', skip_header_lines=1)
    | 'Texto para lista (dengue)' >> beam.Map(txt_list)
    | 'Lista para dicionário' >> beam.Map(list_dicionario, colunas_denguetxt)
    | 'Criar key ANO-MES' >> beam.Map(trata_data)
    | 'Criar chave pelo estado' >> beam.Map(uf_key)
    | 'Agrupar por estado' >> beam.GruopByKey()
    | 'Descompactando dados' >> beam.FlatMap(casos_dengue) #flatmap permite que retorne um yield
    | 'Soma dos casos dengue por key' >> beam.CombinePerKey(sum)  #agrupa as key e soma key iguais
    #| 'Mostra resultados de dengue' >> beam.Map(print)
)

#Criando pipeline para tratar quantidade de chuvas
chuvas = (
    pipeline
    | 'Leitura do dataset de dengue' >> 
        ReadFromText('sample_chuvas.csv', skip_header_lines=1)
    | 'Texto para lista (chuvas)' >> beam.Map(txt_list, delimitador = ',')
    | 'Cria chave UF-ANO-MES' >> beam.Map(casos_chuva)
    | 'Soma total mm chuvas por key' >> beam.CombinePerKey(sum)
    | 'Tratando resultados de mm de chuvas' >> beam.Map(trata_mm)
    #| 'Mostra resultados de chuvas' >> beam.Map(print)
    
)

#Unindo as pcollection chuvas x dengue
resultado = (
    ({'mm_chuvas': chuvas, 'casos_dengue': dengue}) #colocando as pcolls em dicts
    | 'Mesclando pcolls' >> beam.CoGroupByKey()     #junta e nomea as pcolls por key do dict
    | 'Filtro para dados vazios' >> beam.Filter(filtro_campos_vazio) #true/false p/ quem fica
    | 'Descompactando elementos' >> beam.Map(descompactar_elem)
    | 'Preparando para CSV' >> beam.Map(preparar_csv)
    #| 'Mostra resultado da união' >> beam.Map(print)
)

header = 'UF;ANO;MES;CHUVA;DENGUE'

resultado | 'Criar arquivo CSV' >> WriteToText('resultado', file_name_suffix = '.csv', header = header )

pipeline.run()
