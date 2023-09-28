import pandas as pd
import re

file_resultado_objetivo = "ResultadoSerpro2023Objetiva.txt"
file_resultado_pratica = "ResultadoSerpro2023PraticaDefinitivo.txt"

def is_number_regex(s):
    if re.match("^\d+?\.\d+?$", s) is None:
        return s.isdigit()
    return True

def process_file(file_name, n_cols ):
    file = open(file_name, "r")
    lines = file.readlines()
    raw_data = []
    for line in lines:
        records = line.split("/")
        for record in records:
            raw_data.append(record.split(","))

    page_number = 1
    data_to_add = []
    result = []
    nl = 0
    for d in raw_data:
        new_line_data = [x.strip() for x in d if x!='\n']
        if len(new_line_data)==0:
            continue
        d0 = new_line_data[0]
        n0 = len(d0)
        n = len(new_line_data)
        if n==n_cols:
            result.append(new_line_data)
            nl = 0
        elif d0.isnumeric() and n0==8:
            data_to_add = new_line_data
            nl += n
        elif n==1 and '\n' in d[0] and d0.isnumeric() \
             and int(d0)==(page_number+1):
            page_number += 1
        else:
            d_1 = data_to_add[-1] if len(data_to_add)>0 else ''
            nd_0 = new_line_data[0]
            nx = 0
            if len(data_to_add)>0 and not is_number_regex(d_1) \
               and not is_number_regex(nd_0):
                data_to_add[-1] += " " + nd_0
                nx = 1
            data_to_add += new_line_data[nx:]
            nl += n - nx
            if nl>=n_cols:
                result.append(data_to_add)
                data_to_add = []
                nl = 0
    return result


def cast_float(x):
    xdots = x.split('.')
    if len(xdots)==1:
        return float(xdots[0])
    else:
        return float(xdots[0]+"."+xdots[1])

def process_data(data,cols,index=None):
    df = pd.DataFrame(data, columns=cols)
    for c in cols[2:]:
        try:
            df[c] = df[c].astype('f8')
        except:
            df[c] = df[c].apply(cast_float)
    if index is not None:
        df.set_index(index,inplace=True)
    return df


def resultado():
    data_objetiva = process_file(file_resultado_objetivo, 17)
    data_pratica = process_file(file_resultado_pratica, 3)
    cols_objetiva = ['Inscrição','Nome','NotaPort','AcertosPort','NotaIngl',
                 'AcertosIngl','NotaEstat','AcertosEstat','NotaRacLog',
                 'AcertosRacLog','NotaLegis','AcertosLegis','NotaBasicos',
                 'AcertosBasicos','NotaEspec','AcertosEspec','NotaFinal']
    cols_pratica = ['Inscrição','Nome','NotaFinal']
    
    df_objetiva = process_data(data_objetiva, cols_objetiva)
    df_pratica = process_data(data_pratica, cols_pratica)

    df_prova = pd.merge(df_objetiva[['Inscrição','Nome','NotaBasicos','NotaEspec']],
                    df_pratica[['Inscrição','NotaFinal']], on="Inscrição")

    df_prova['NotaPratica']=df_prova['NotaFinal']
    df_prova['NotaFinal'] = df_prova['NotaBasicos'] + \
                            df_prova['NotaEspec']*2.0 + \
                            df_prova['NotaPratica']

    df_resultado = df_prova.groupby("Inscrição").agg(
        {"Nome":'first',"NotaBasicos":'max',"NotaEspec":'max',
         "NotaPratica":'max',"NotaFinal":'max'})

    df_resultado.sort_values(by='NotaFinal',ascending=False,
                             inplace=True)

    df_resultado.reset_index(inplace=True)

    df_resultado['Posição'] = df_resultado.index + 1

    cols_resultado = ['Inscrição','Nome','NotaBasicos','NotaEspec',
                      'NotaPratica','NotaFinal','Posição']

    return df_resultado[cols_resultado]

df_resultado = resultado()
df_resultado.to_csv('Resultado_Serpro_2023_Definitivo.csv',encoding='utf-8-sig')
