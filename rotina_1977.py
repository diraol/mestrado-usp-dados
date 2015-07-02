# coding: utf-8

from IPython.display import display #from IPython.core.display import HTML
import math
import logging
import sys, os
import pandas as pd
pd.set_option('display.mpl_style', 'default') #Make the graphs a bit prettier

#Definindo loggers
# logging.basicConfig(filename='rotina_1977.log', level=logging.DEBUG)
# ref: http://stackoverflow.com/questions/17035077/python-logging-to-multiple-log-files-from-different-classes
log_formatter = logging.Formatter('%(levelname)s: %(message)s')

log_verificador = logging.getLogger('log_verificador')
FH_verificador = logging.FileHandler('1977_verificador.log')
FH_verificador.setFormatter(log_formatter)
log_verificador.setLevel(logging.INFO)
log_verificador.addHandler(FH_verificador)

log_acompanhamento = logging.getLogger('log_acompanhamento')
FH_acompanhamento = logging.FileHandler('1977_acompanhamento.log')
FH_acompanhamento.setFormatter(log_formatter)
log_acompanhamento.setLevel(logging.INFO)
log_acompanhamento.addHandler(FH_acompanhamento)

#Variable to avoid log prints when generating pdf file
impressao = False  # True = to not print logs | False = to print logs

# --------
# ##Funções gerais
log_acompanhamento.info('Defining general functions')


def consulta_refext(row, ext_data_frame, name_col_ref, name_col_filt, name_col_search):
    """
    Traz valor de referência externa (em arquivo csv) baseado em valor de referência do arquivo de origem.
    O primeiro argumento passado é a "linha".
    O segundo argumento é uma variável do tipo dataframe referente ao arquivo csv que será consultado
    O terceiro argumento é o nome da coluna no dataframe (.csv) consultado que servirá de refência para a localização
    O quarto argumento é o nome da coluna de filtro do dataframe atual
    O quinto argumento é o nome da coluna no dataframe (.csv) consultado que contém o valor a ser retornado.
        Uso:
        od1977_ex['coluna a receber o valor'] = od1977_ex.apply(lambda row: consulta_refext(row, data_frame_externo, 'reference column', 'filter column', 'searched column'), axis=1)
    """
    if row[name_col_filt] == 0:
        return row[name_col_filt]
    res = ext_data_frame[ext_data_frame[name_col_ref] == row[name_col_filt]][name_col_search]
    #Verificando se algum valor foi encontrado
    if not res.empty:
        return int(res)
    else:
        return 999


def verifica_DUMMY(data_frame, nome_variavel):
    """
    Verifica se uma variável, dummy, contém algum valor diferente de 0 ou de 1.
        Uso:
        verifica_DUMMY(nome_do_dataframe, 'coluna a ser verificada')
    """
    contador_de_erros = 0
    log_verificador.info(nome_variavel + ': \n')
    log_verificador.info(nome_variavel + ":Verificando a variável Dummy: " + nome_variavel)
    # TODO: MELHORAR/OTIMIZAR
    for index, value in data_frame.iterrows():
        try:
            if value[nome_variavel] is None or (int(value[nome_variavel]) != 1 and int(value[nome_variavel]) != 0):
                log_verificador.warn(nome_variavel + ": Erro encontrado no registro " + str(index+1) + ".")
                log_verificador.warn(nome_variavel + ":     Valor encontrado: " + str(value[nome_variavel]))
                contador_de_erros += 1
        except Exception as e:
            log_acompanhamento.warn('\n-------------------------------------------------------\nERRO\n')
            log_acompanhamento.warn(e)
            log_acompanhamento.warn(nome_variavel)
            log_acompanhamento.warn(value)
            log_acompanhamento.warn(value[nome_variavel])
    log_verificador.info(nome_variavel + ": Total de erros encontrados: " + str(contador_de_erros) + '\n')


def verifica_RANGE(df, variavel, valor_menor, valor_maior):
    """
    Verifica se uma variável, do tipo número inteiro,
    contém algum valor menor que "valor_menor" ou maior que "valor_maior"
        Uso:
        verifica_RANGE(nome_do_dataframe, 'coluna a ser verificada', 'valor_menor', 'valor_maior')
    """
    log_acompanhamento.info(variavel + ': Verificando range da variável.')
    log_verificador.info('\n')
    log_verificador.info(variavel + ': Verificando o Range da variável.')
    log_verificador.info(variavel + ': Mínimo esperado - ' + str(valor_menor) +
                         ' | Máximo esperado - ' + str(valor_maior))
    log_verificador.info(variavel + ": Total de registros: " + str(len(df[variavel])))
    #Registros inválidos: None (equivalente a NA)
    log_verificador.info(variavel + ": Registros inválidos: " + str(df[variavel].isnull().sum()))
    log_verificador.info(variavel + ': Descição da variável: \n' + str(df[variavel].describe()))

    result = df[variavel].value_counts().sort_index()
    if result.first_valid_index() < valor_menor:
        log_verificador.warn(variavel + ": Valor inteiro mínimo: " + str(result.first_valid_index()))
    else:
        log_verificador.info(variavel + ": Valor inteiro mínimo: " +
                             str(result.first_valid_index()) + " - abaixo do esperado!")
    if result.last_valid_index() > valor_maior:
        log_verificador.warn(variavel + ": Valor inteiro máximo: " +
                             str(result.last_valid_index()) + " - acima do esperado!")
    else:
        log_verificador.info(variavel + ": Valor inteiro máximo: " + str(result.last_valid_index()))
    log_verificador.info(variavel + ": Tabela de valores: \n" + str(result) + '\n')

    df_filtrado = df[(df[variavel] < valor_menor) | (df[variavel] > valor_maior)]
    valores_incorretos = df_filtrado[variavel].value_counts()
    if len(valores_incorretos) > 0:
        log_acompanhamento.warn(variavel + ': ' + str(len(valores_incorretos)) +
                                ' Valor(es) incorreto(s) encontrado(s) nesta variável:')
        log_acompanhamento.warn('\n' + str(valores_incorretos))

# -----
# ## Funções das Variáveis Independentes Gerais


def passo_ano(passo, df):
    """
    Preenche a coluna "ANO" com  valor 1 em todas células
    Categorias:
    |valor|ano_correspondente|
    |-------|-----|
    |1|1977|
    |2|1987|
    |3|1997|
    |4|2007|

    :param passo: número do passo para o log
    :param df: dataframe a ser modificado
    :return: retorna o dataframe modificado
    """

    log_acompanhamento.info("### PASSO " + str(passo) + " - ANO")
    # Assigning value '1' to all cels of the "ANO" column
    df["ANO"] = 1

    # Describing data ("ANO" column) - count, mean, std, min and max
    log_verificador.info('ANO:')
    log_verificador.info(str(df['ANO'].describe()))

    return df


def passo_dia_sem(passo, df):
    """
    # Não existe essa informação no banco de dados de 1977, logo, este campo será preenchido com 'None'.
    #
    # ####Categorias:
    # Valor|Descrição
    # -----|-----
    # 0|Não disponível
    # 2|Segunda-Feira
    # 3|Terça-Feira
    # 4|Quarta-Feira
    # 5|Quinta-Feira
    # 6|Sexta-Feira
    :param passo: número do passo para o log
    :param df: dataframe a ser modificado
    :return: retorna o dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - DIA_SEM")
    # Assigning value '0' to all cels of the "DIA_SEM" column
    df['DIA_SEM']= None

    # Counting "DIA_SEM" in order to compare the values before and after the replacement
    log_verificador.info("DIA_SEM: Dados após modificação")
    log_verificador.info("DIA_SEM: \n" + str(df['DIA_SEM'].value_counts()))

    return df


# -----
# ## Funções das Variáveis Independentes dos Domicílios


def passo_zona_dom(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    :param passo: passo
    :param df: dataframe
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ZONA_DOM")

    # Verifying value interval for check - conditions: "ZONA_DOM < 1" and "ZONA_DOM > 243"
    verifica_RANGE(df, 'ZONA_DOM', 1, 243)


def passo_subzona_dom(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - SUBZONA_DOM")

    # Verifying value interval for check - conditions: "SUBZONA_DOM < 1" and "SUBZONA_DOM > 633"
    verifica_RANGE(df, 'SUBZONA_DOM', 1, 633)


def passo_mun_dom(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MUN_DOM")

    # Verifying value interval for check - conditions: "MUN_DOM < 1" and "MUN_DOM > 27"
    verifica_RANGE(df, 'MUN_DOM', 1, 27)


def passo_f_dom(passo, df):
    """
    # Checar se existe algum erro na coluna "F_DOM"
    #
    # ####Categorias
    # Valor|Descrição
    # ----|----
    # 0|Demais registros
    # 1|Primeiro Registro do Domicílio
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - F_DOM")

    # Verifying if there was left some value other than 0 or 1
    verifica_DUMMY(df, 'F_DOM')


def passo_fe_dom(passo, df):
    """
    # Nada há que se fazer em relação aos dados da coluna "FE_DOM"

    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - FE_DOM")
    pass


def passo_tipo_dom(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias anteriores / novas
    # Valor | Descrição
    # ----|----
    # 0|Não respondeu
    # 1|Particular
    # 2|Coletivo
    #
    # [Teste: Checar se existe algum número < 0 ou > 2. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return:
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - TIPO_DOM")

    # Verifying value interval for check - conditions: "TIPO_DOM < 0" and "TIPO_DOM > 2"
    verifica_RANGE(df, 'TIPO_DOM', 0, 2)


# -----
# ## Funções das Variáveis Independentes das Famílias


def passo_tot_fam(passo, df):
    """
    # Nada há que se fazer em relação aos dados da coluna "TOT_FAM"
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - TOT_FAM")
    pass


def passo_f_fam(passo, df):
    """
    # Checar se existe algum erro na coluna "F_FAM"
    #
    # ####Categorias
    # Valor|Descrição
    # ----|----
    # 0|Demais registros
    # 1|Primeiro Registro da Família
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return:
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - F_FAM")
    #Verifying if there was left some value other than 0 or 1
    verifica_DUMMY(df, 'F_FAM')


def passo_fe_fam(passo, df):
    """
    # ##"FE_FAM"
    # Nada há que se fazer em relação aos dados da coluna "FE_FAM"
    :param passo:
    :param df:
    :return:
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - FE_FAM")
    pass


def passo_cond_mora(passo, df):
    """
    # Substituir valores da coluna "COND_MORA"
    #
    # * Substituir todos valores **1** por **2**
    # * Substituir todos valores **3** por **1**
    # * Substituir todos valores **4** por **3**
    # * Substituir todos valores **5** por **3**
    # * Substituir todos valores **0** por **4**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Própria paga
    # 2|Própria em pagamento
    # 3|Alugada
    # 4|Cedida
    # 5|Outro
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 1|Alugada
    # 2|Própria
    # 3|Outros
    # 4|Não respondeu
    #
    # [Teste: Checar se existe algum número < 1 ou > 4. Se encontrar, retornar erro indicando em qual linha.]
    :param passo: passo
    :param df: dataframe
    :return: dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - F_FAM")
    log_verificador.info("COND_MORA: Situação inicial dos dados: \n" + str(df['COND_MORA'].value_counts()))

    # Replacing the values 1 for 2
    df.loc[df['COND_MORA']==1,'COND_MORA'] = 2
    # Replacing the values 3 for 1
    df.loc[df['COND_MORA']==3,'COND_MORA'] = 1
    # Replacing the values 4 for 3
    df.loc[df['COND_MORA']==4,'COND_MORA'] = 3
    # Replacing the values 5 for 3
    df.loc[df['COND_MORA']==5,'COND_MORA'] = 3
    # Replacing the values 0 for 4
    df.loc[df['COND_MORA']==0,'COND_MORA'] = 4

    # Verifying value interval for check - conditions: "COND_MORA < 1" and "COND_MORA > 4"
    verifica_RANGE(df, 'COND_MORA', 1, 4)

    return df


def passo_qt_auto(passo, df):
    """
    # ##"QT_AUTO"
    # Nada há que se fazer em relação aos dados da coluna "QT_AUTO"
    :param passo:
    :param df:
    :return:
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - QT_AUTO")
    pass


def passo_qt_bici(passo, df):
    """
    # Não existe essa informação no banco de dados de 1977, logo, este campo será preenchido com 'None'.
    :param passo:
    :param df:
    :return: dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - QT_BICI")
    df['QT_BICI'] = None

    # Counting "QT_BICI" in order to check the values after the procedure
    log_verificador.info("QT_BICI: Total de registros: " + str(len(df['QT_BICI'])))
    log_verificador.info("QT_BICI: Soma de bicicletas 'nulas': " + str(df['QT_BICI'].isnull().sum()))

    return df


def passo_qt_moto(passo, df):
    """
    # Não existe essa informação no banco de dados de 1977, logo, este campo será preenchido com 'None'.
    :param passo:
    :param df:
    :return: dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - QT_MOTO")
    df['QT_MOTO'] = None

    # Counting "QT_MOTO" in order to check the values after the procedure
    log_verificador.info("QT_MOTO: Total de registros: " + str(len(df['QT_MOTO'])))
    log_verificador.info("QT_MOTO: Soma de motos 'nulas': " + str(df['QT_MOTO'].isnull().sum()))

    return df


def passo_ren_fam(passo, df):
    """
    # Nada há que se fazer em relação aos dados da colunas "REN_FAM"
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - REN_FAM")
    pass


# -----
# ## Funções das Variáveis Independentes das Pessoas


def passo_f_pess(passo, df):
    """
    # Checar se existe algum erro na coluna "F_PESS"
    #
    # ####Categorias
    # Valor|Descrição
    # ----|----
    # 0|Demais registros
    # 1|Primeiro Registro da Pessoa
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return:
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - F_PESS")

    #Verifying if there was left some value other than 0 or 1
    verifica_DUMMY(df, 'F_PESS')


def passo_fe_pess(passo, df):
    """
    # Nada há que se fazer em relação aos dados das colunas "FE_PESS"
    :param passo:
    :param df:
    :return:
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - FE_PESS")
    pass


def passo_sit_fam(passo, df):
    """
    # * Substituir todos valores **5** por **4**
    # * Substituir todos valores **6** por **5**
    # * Substituir todos valores **7** por **6**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Chefe
    # 2|Cônjuge
    # 3|Filho(a)
    # 4|Parente
    # 5|Agregado
    # 6|Empregado Residente
    # 7|Visitante não Residente
    #
    # ####Categorias novas:
    # Valor|Descrição
    # ----|----
    # 1| Pessoa Responsável
    # 2| Cônjuge/Companheiro(a)
    # 3| Filho(a)/Enteado(a)
    # 4| Outro Parente / Agregado
    # 5| Empregado Residente
    # 6| Outros (visitante não residente / parente do empregado)
    #
    # [Teste: Checar se existe algum número < 1 ou > 6. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: retorna o dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - SIT_FAM")

    # Verificando condição inicial dos dados
    log_verificador.info("SIT_FAM: Situação inicial dos dados: \n" + str(df['SIT_FAM'].value_counts()))

    # Replacing the values 5 for 4
    df.loc[df['SIT_FAM']==5,'SIT_FAM'] = 4
    # Replacing the values 6 for 5
    df.loc[df['SIT_FAM']==6,'SIT_FAM'] = 5
    # Replacing the values 7 for 6
    df.loc[df['SIT_FAM']==7,'SIT_FAM'] = 6

    # Verifying value interval for check - conditions: "SIT_FAM < 1" and "SIT_FAM > 6"
    verifica_RANGE(df, 'SIT_FAM', 1, 6)

    return df


def passo_idade(passo, df):
    """
    # Nada há que se fazer em relação aos dados da coluna "IDADE"
    :param passo:
    :param df:
    :return:
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - IDADE")
    pass


def passo_sexo(passo, df):
    """
    # Substituir valores da coluna "SEXO"
    #
    # * Substituir todos valores **2** por **0**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Masculino
    # 2|Feminino
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Feminino
    # 1|Masculino
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: retorna o dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - SEXO")

    # Verificando condição inicial dos dados
    log_verificador.info("SIT_FAM: Situação inicial dos dados: \n" + str(df['SIT_FAM'].value_counts()))

    # Replacing the values 2 for 0
    df.loc[df['SEXO']==2,'SEXO'] = 0

    # Verifying if there was left some value other than 0 or 1
    verifica_DUMMY(df, 'SEXO')

    return df


def passo_grau_instr(passo, df):
    """
    # Substituir valores da coluna "GRAU_INSTR"
    #
    # * Substituir todos valores **2** por **1**
    # * Substituir todos valores **3** por **1**
    # * Substituir todos valores **4** por **1**
    # * Substituir todos valores **5** por **2**
    # * Substituir todos valores **6** por **2**
    # * Substituir todos valores **7** por **3**
    # * Substituir todos valores **8** por **3**
    # * Substituir todos valores **9** por **4**
    #
    # ####Categorias anteriores:
    # Valor|Descrição
    # ----|----
    # 1|Sem Instrução
    # 2|Primário Incompleto
    # 3|Primário Completo
    # 4|Ginasial Incompleto
    # 5|Ginasial Completo
    # 6|Colegial Incompleto
    # 7|Colegial Completo
    # 8|Universitário Incompleto
    # 9|Universitário Completo
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não declarou
    # 1|Não-Alfabetizado/Fundamental Incompleto
    # 2|Fundamental Completo/Médio Incompleto
    # 3|Médio Completo/Superior Incompleto
    # 4|Superior completo
    #
    # [Teste: Checar se existe algum número < 1 ou > 4. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: retorna dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - GRAU_INSTR")

    # Verificando condição inicial dos dados
    log_verificador.info("GRAU_INSTR: Situação inicial dos dados: \n" + str(df['GRAU_INSTR'].value_counts()))

    # Replacing the values 2 for 1
    df.loc[df['GRAU_INSTR']==2,'GRAU_INSTR'] = 1
    # Replacing the values 3 for 1
    df.loc[df['GRAU_INSTR']==3,'GRAU_INSTR'] = 1
    # Replacing the values 4 for 1
    df.loc[df['GRAU_INSTR']==4,'GRAU_INSTR'] = 1
    # Replacing the values 5 for 2
    df.loc[df['GRAU_INSTR']==5,'GRAU_INSTR'] = 2
    # Replacing the values 6 for 2
    df.loc[df['GRAU_INSTR']==6,'GRAU_INSTR'] = 2
    # Replacing the values 7 for 3
    df.loc[df['GRAU_INSTR']==7,'GRAU_INSTR'] = 3
    # Replacing the values 8 for 3
    df.loc[df['GRAU_INSTR']==8,'GRAU_INSTR'] = 3
    # Replacing the values 9 for 4
    df.loc[df['GRAU_INSTR']==9,'GRAU_INSTR'] = 4

    # Verifying value interval for check - conditions: "GRAU_INSTR < 1" and "GRAU_INSTR > 4"
    verifica_RANGE(df, 'GRAU_INSTR', 1, 4)

    return df


def passo_ocup(passo, df):
    """
    # Substituir valores da coluna "OCUP"
    #
    # * Substituir todos valores **1** por **7**
    # * Substituir todos valores **2** por **6**
    # * Substituir todos valores **4** por **5**
    # * Substituir todos valores **5** por **4**
    # * Substituir todos valores **6** por **2**
    # * Substituir todos valores **7** em diante por **1**
    #
    # ####Categorias anteriores:
    # Valor|Descrição
    # ----|----
    # 1|Estudante
    # 2|Prendas Domésticas
    # 3|Aposentado
    # 4|Sem Ocupação (nunca trabalhou)
    # 5|Desempregado
    # 6|Em licença
    # 7 em diante|diversas profissões
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 1|Tem trabalho
    # 2|Em licença médica
    # 3|Aposentado / pensionista
    # 4|Desempregado
    # 5|Sem ocupação
    # 6|Dona de casa
    # 7|Estudante
    #
    # [Teste: Checar se existe algum número < 0 ou > 7. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: retorna dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - OCUP")

    # Verificando condição inicial dos dados
    log_verificador.info("OCUP: Situação inicial dos dados: \n" + str(df['OCUP'].value_counts()))

    # Replacing the values 1 for 7
    df.loc[df['OCUP']==1,'OCUP'] = 7
    # Replacing the values 2 for 9
    df.loc[df['OCUP']==2,'OCUP'] = 9
    # Replacing the values 4 for 8
    df.loc[df['OCUP']==4,'OCUP'] = 8
    # Replacing the values 5 for 4
    df.loc[df['OCUP']==5,'OCUP'] = 4
    # Replacing the values 8 for 5
    df.loc[df['OCUP']==8,'OCUP'] = 5
    # Replacing the values 6 for 2
    df.loc[df['OCUP']==6,'OCUP'] = 2
    # Replacing the values 9 for 6
    df.loc[df['OCUP']==9,'OCUP'] = 6
    # Replacing the values > 6 for 7
    df.loc[df['OCUP']>7,'OCUP'] = 1

    # Verifying value interval for check - conditions: "OCUP < 1" and "OCUP > 7"
    verifica_RANGE(df, 'OCUP', 1, 7)

    return df


def passo_cd_renind(passo, df):
    """
    # Checar se existe algum erro na coluna "CD_RENIND"
    #
    # ####Categorias
    # Valor|Descrição
    # ----|----
    # 1|Tem renda
    # 2|Não tem renda
    # 3|Não declarou
    #
    # [Teste: Checar se existe algum número < 1 ou > 3. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - CD_RENIND")

    # Verifying value interval for check - conditions: "CD_RENIND < 1" and "CD_RENIND > 3"
    verifica_RANGE(df, 'CD_RENIND', 1, 3)


def passo_ren_ind(passo, df):
    """
    # Nada há que se fazer em relação aos dados da coluna "REN_IND"
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - REN_IND")
    pass

def passo_zona_esc(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: Sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ZONA_ESC")

    # Verifying value interval for check - conditions: "ZONA_ESC < 1" and "ZONA_ESC > 243"
    # The 'error' returns must be related to "ZONA_ESC" == 0, that is, trips that are not school purposed
    verifica_RANGE(df, 'ZONA_ESC', 1, 243)


def passo_subzona_esc(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - SUBZONA_ESC")

    # Verifying value interval for check - conditions: "SUBZONA_ESC < 1" and "SUBZONA_ESC > 633"
    verifica_RANGE(df, 'SUBZONA_ESC', 1, 633)


def passo_mun_esc(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MUN_ESC")

    # Verifying value interval for check - conditions: "MUN_ESC < 1" and "MUN_ESC > 27"
    # The 'error' returns must be related to "MUN_ESC" == 0, that is, trips that are not school purposed
    verifica_RANGE(df, 'MUN_ESC', 1, 27)


def passo_zona_trab1(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ZONA_TRAB1")

    # Verifying value interval for check - conditions: "ZONA_TRAB1 < 1" and "ZONA_TRAB1 > 243"
    # The 'error' returns must be related to "ZONA_TRAB1"==0, that is, trips that are not work purposed
    verifica_RANGE(df, 'ZONA_TRAB1', 1, 243)


def passo_subzona_trab1(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - SUBZONA_TRAB1")

    # Verifying value interval for check - conditions: "SUBZONA_TRAB1 < 1" and "SUBZONA_TRAB1 > 633"
    verifica_RANGE(df, 'SUBZONA_TRAB1', 1, 633)


def passo_mun_trab1(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MUN_TRAB1")

    # Verifying value interval for check - conditions: "MUN_TRAB1 < 1" ou de "MUN_TRAB1 > 27"
    # The 'error' returns must be related to "MUN_TRAB1" == 0, that is, trips that are not work purposed
    verifica_RANGE(df, 'MUN_TRAB1', 1, 27)


def passo_zona_trab2(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ZONA_TRAB2")

    # Verifying value interval for check - conditions: "ZONA_TRAB2 < 1" and "ZONA_TRAB2 > 243
    # The 'error' returns must be related to "ZONA_TRAB2"==0, that is, trips that are not work purposed
    verifica_RANGE(df, 'ZONA_TRAB2', 1, 243)


def passo_subzona_trab2(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - SUBZONA_TRAB2")

    # Verifying value interval for check - conditions: "SUBZONA_TRAB2 < 1" and "SUBZONA_TRAB2 > 633"
    verifica_RANGE(df, 'SUBZONA_TRAB2', 1, 633)


def passo_mun_trab2(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MUN_TRAB2")

    # Verifying value interval for check - conditions: "MUN_TRAB2 < 1" ou de "MUN_TRAB2 > 27"
    # The 'error' returns must be related to "MUN_TRAB2" == 0, that is, trips that are not work purposed
    verifica_RANGE(df, 'MUN_TRAB2', 1, 27)


# -----
# ## Funções das Variáveis Independentes das Viagens


def passo_f_viag(passo, df):
    """
    # Excluir a coluna "F_VIAG", porque as viagens são numeradas,
        então já se saber pelo NO_VIAG qual é a primeira do indivíduo.
    :param passo:
    :param df:
    :return: retorna o dataframe sem a coluna F_VIAG
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - F_VIAG")

    if 'F_VIAG' in df.columns.tolist():
        df = df.drop('F_VIAG', 1)
        log_acompanhamento.info('F_VIAG: Coluna "F_VIAG" removida.')

    return df


def passo_fe_viag(passo, df):
    """
    # Nada há que se fazer em relação aos dados da coluna "FE_VIAG"
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - FE_VIAG")
    pass


def passo_zona_orig(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ZONA_ORIG")

    #Verifying value interval for check - conditions: "ZONA_ORIG < 1" and "ZONA_ORIG > 243"
    #The 'error' returns must be related to "ZONA_ORIG"==0, that is, trips that were not made
    verifica_RANGE(df, 'ZONA_ORIG', 1, 243)


def passo_subzona_orig(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - SUBZONA_ORIG")

    #Verifying value interval for check - conditions: "SUBZONA_ORIG < 1" and "SUBZONA_ORIG > 633"
    verifica_RANGE(df, 'SUBZONA_ORIG', 1, 633)


def passo_mun_orig(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MUN_ORIG")

    #Verifying value interval for check - conditions: "MUN_ORIG < 1" ou de "MUN_ORIG > 27"
    #The 'error' returns must be related to "MUN_ORIG" == 0, that is, trips that were not made
    verifica_RANGE(df, 'MUN_ORIG', 1, 27)


def passo_zona_dest(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ZONA_DEST")

    #Verifying value interval for check - conditions: "ZONA_DEST < 1" and "ZONA_DEST > 243"
    #The 'error' returns must be related to "ZONA_DEST"==0, that is, trips that are not school purposed
    verifica_RANGE(df, 'ZONA_DEST', 1, 243)


def passo_subzona_dest(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - SUBZONA_DEST")

    #Verifying value interval for check - conditions: "SUBZONA_DEST < 1" and "SUBZONA_DEST > 633"
    verifica_RANGE(df, 'SUBZONA_DEST', 1, 633)


def passo_mun_dest(passo, df):
    """
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MUN_DEST")

    #Verifying value interval for check - conditions: "MUN_DEST < 1" ou de "MUN_DEST > 27"
    #The 'error' returns must be related to "MUN_DEST" == 0, that is, trips that were not made
    verifica_RANGE(df, 'MUN_DEST', 1, 27)


def passo_motivo_orig(passo, df):
    """
    # * Substituir todos valores **6** por **11**
    # * Substituir todos valores **7** por **6**
    # * Substituir todos valores **8** por **7**
    # * Substituir todos valores **10** por **8**
    # * Substituir todos valores **11** por **9**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Trabalho/Indústria
    # 2|Trabalho/Comércio
    # 3|Trabalho/Serviços
    # 4|Escola/Educação
    # 5|Compras
    # 6|Negócios
    # 7|Médico/Dentista/Saúde
    # 8|Recreação/Visitas
    # 9|Servir Passageiro
    # 10|Residência
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Trabalho/Indústria
    # 2|Trabalho/Comércio
    # 3|Trabalho/Serviços
    # 4|Educação
    # 5|Compras
    # 6|Saúde
    # 7|Lazer
    # 8|Residência
    # 9|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 9. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MOTIVO_ORIG")

    # Verificando condição inicial dos dados
    log_verificador.info("MOTIVO_ORIG: Situação inicial dos dados: \n" + str(df['MOTIVO_ORIG'].value_counts()))

    #Replacing the values 6 for 11
    df.loc[df['MOTIVO_ORIG']==6,'MOTIVO_ORIG'] = 11
    #Replacing the values 7 for 6
    df.loc[df['MOTIVO_ORIG']==7,'MOTIVO_ORIG'] = 6
    #Replacing the values 8 for 7
    df.loc[df['MOTIVO_ORIG']==8,'MOTIVO_ORIG'] = 7
    #Replacing the values 10 for 8
    df.loc[df['MOTIVO_ORIG']==10,'MOTIVO_ORIG'] = 8
    #Replacing the values 11 for 9
    df.loc[df['MOTIVO_ORIG']==11,'MOTIVO_ORIG'] = 9

    #Verifying value interval for check - conditions: "MOTIVO_ORIG < 0" and "MOTIVO_ORIG > 9"
    verifica_RANGE(df, 'MOTIVO_ORIG', 0, 9)

    return df


def passo_motivo_dest(passo, df):
    """
    # * Substituir todos valores **6** por **11**
    # * Substituir todos valores **7** por **6**
    # * Substituir todos valores **8** por **7**
    # * Substituir todos valores **10** por **8**
    # * Substituir todos valores **11** por **9**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Trabalho/Indústria
    # 2|Trabalho/Comércio
    # 3|Trabalho/Serviços
    # 4|Escola/Educação
    # 5|Compras
    # 6|Negócios
    # 7|Médico/Dentista/Saúde
    # 8|Recreação/Visitas
    # 9|Servir Passageiro
    # 10|Residência
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Trabalho/Indústria
    # 2|Trabalho/Comércio
    # 3|Trabalho/Serviços
    # 4|Educação
    # 5|Compras
    # 6|Saúde
    # 7|Lazer
    # 8|Residência
    # 9|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 9. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MOTIVO_DEST")

    log_verificador.info("MOTIVO_DEST: Situação inicial dos dados: \n" + str(df['MOTIVO_DEST'].value_counts()))

    #Replacing the values 6 for 11
    df.loc[df['MOTIVO_DEST']==6,'MOTIVO_DEST'] = 11
    #Replacing the values 7 for 6
    df.loc[df['MOTIVO_DEST']==7,'MOTIVO_DEST'] = 6
    #Replacing the values 8 for 7
    df.loc[df['MOTIVO_DEST']==8,'MOTIVO_DEST'] = 7
    #Replacing the values 10 for 8
    df.loc[df['MOTIVO_DEST']==10,'MOTIVO_DEST'] = 8
    #Replacing the values 11 for 9
    df.loc[df['MOTIVO_DEST']==11,'MOTIVO_DEST'] = 9

    #Verifying value interval for check - conditions: "MOTIVO_DEST < 0" and "MOTIVO_DEST > 9"
    verifica_RANGE(df, 'MOTIVO_DEST', 0, 9)

    return df


def passo_modo1(passo, df):
    """
    # Não há o que fazer com os valores da coluna "MODO1"
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Ônibus trólebus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua
    # 7|Metrô
    # 8|Trem
    # 9|Motocicleta
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Ônibus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua / Van / Microônibus
    # 7|Metrô
    # 8|Trem
    # 9|Moto
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 12. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MODO1")

    log_verificador.info("MODO1: Situação inicial dos dados: \n" + str(df['MODO1'].value_counts()))

    #Verifying value interval for check - conditions: "MODO1 < 0" and "MODO1 > 12"
    verifica_RANGE(df, 'MODO1', 0, 12)


def passo_modo2(passo, df):
    """
    # Não há o que fazer com os valores da coluna "MODO2"
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Ônibus trólebus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua
    # 7|Metrô
    # 8|Trem
    # 9|Motocicleta
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Ônibus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua / Van / Microônibus
    # 7|Metrô
    # 8|Trem
    # 9|Moto
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 12. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MODO2")

    log_verificador.info("MODO2: Situação inicial dos dados: \n" + str(df['MODO2'].value_counts()))

    #Verifying value interval for check - conditions: "MODO2 < 0" and "MODO2 > 12"
    verifica_RANGE(df, 'MODO2', 0, 12)

def passo_modo3(passo, df):
    """
    # Não há o que fazer com os valores da coluna "MODO3"
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Ônibus trólebus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua
    # 7|Metrô
    # 8|Trem
    # 9|Motocicleta
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Ônibus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua / Van / Microônibus
    # 7|Metrô
    # 8|Trem
    # 9|Moto
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 12. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MODO3")

    log_verificador.info("MODO3: Situação inicial dos dados: \n" + str(df['MODO3'].value_counts()))

    #Verifying value interval for check - conditions: "MODO3 < 0" and "MODO3 > 12"
    verifica_RANGE(df, 'MODO3', 0, 12)

def passo_modo4(passo, df):
    """
    # Nada há que se fazer em relação à coluna "MODO4" - não há dados de 1977, coluna permanecerá vazia
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MODO4")

    log_verificador.info("MODO4: Situação inicial dos dados: \n" + str(df['MODO4'].value_counts()))


def passo_modo_prin(passo, df):
    """
    # Não há o que fazer com os valores da coluna "MODO_PRIN"
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Ônibus trólebus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua
    # 7|Metrô
    # 8|Trem
    # 9|Motocicleta
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Ônibus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua / Van / Microônibus
    # 7|Metrô
    # 8|Trem
    # 9|Moto
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 12. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - MODO_PRIN")

    log_verificador.info("MODO_PRIN: Situação inicial dos dados: \n" + str(df['MODO_PRIN'].value_counts()))

    #Verifying value interval for check - conditions: "MODO_PRIN < 0" and "MODO_PRIN > 12"
    verifica_RANGE(df, 'MODO_PRIN', 0, 12)


def passo_tipo_est_auto(passo, df):
    """
    # Substituir valores da coluna "TIPO_EST_AUTO"
    #
    # * Substituir todos valores **1** por **5**
    # * Substituir todos valores **2** por **2**
    # * Substituir todos valores **3** por **2**
    # * Substituir todos valores **4** por **3**
    # * Substituir todos valores **5** por **5**
    # * Substituir todos valores **6** por **4**
    # * Substituir todos valores **7** por **1**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Zona Azul / Parqímetro
    # 2|Estacionamento Avulso
    # 3|Estacionamento Mensal
    # 4|Estacionamento Próprio
    # 5|Meio-Fio / Logradouro
    # 6|Estacionamento Patrocinado
    # 7|Não estacionou
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não Respondeu
    # 1|Não Estacionou
    # 2|Estacionamento Particular (Avulso / Mensal)
    # 3|Estacionamento Próprio
    # 4|Estacionamento Patrocinado
    # 5|Rua (meio fio / zona azul / zona marrom / parquímetro)
    #
    # [Teste: Checar se existe algum número < 0 ou > 5. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: retorna dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - TIPO_EST_AUTO")

    log_verificador.info("TIPO_EST_AUTO: Situação inicial dos dados: \n" + str(df['TIPO_EST_AUTO'].value_counts()))

    #Replacing the values 1 for 5
    df.loc[df['TIPO_EST_AUTO']==1,'TIPO_EST_AUTO'] = 5
    #Replacing the values 3 for 2
    df.loc[df['TIPO_EST_AUTO']==3,'TIPO_EST_AUTO'] = 2
    #Replacing the values 4 for 3
    df.loc[df['TIPO_EST_AUTO']==4,'TIPO_EST_AUTO'] = 3
    #Replacing the values 6 for 4
    df.loc[df['TIPO_EST_AUTO']==6,'TIPO_EST_AUTO'] = 4
    #Replacing the values 7 for 1
    df.loc[df['TIPO_EST_AUTO']==7,'TIPO_EST_AUTO'] = 1

    #Verifying value interval for check - conditions: "TIPO_EST_AUTO < 0" and "TIPO_EST_AUTO > 5"
    verifica_RANGE(df, 'TIPO_EST_AUTO', 0, 5)

    return df


def passo_valor_est_auto(passo, df):
    """
    # Nada há que se fazer em relação à coluna "VALOR_EST_AUTO".
    :param passo:
    :param df:
    :return: sem retorno
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - VALOR_EST_AUTO")
    pass


# -----
# ## Funções das Variáveis que dependem de consulta a arquivos externos


def passo_setor_ativ(passo, df, df_setor):
    """
    # Substituir valores da coluna "SETOR_ATIV"
    #
    # Na coluna "SETOR_ATIV", linha i, ler o valor da linha i da coluna "SETOR_ATIV", daí,
    # buscar o mesmo valor na coluna "COD" do arquivo setor_ativ-1977.csv.
    # Ao achar, retornar o valor da mesma linha, só que da coluna "COD_UNIF"
    #
    # ####Categorias anteriores
    # > ver arquivo .csv
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu
    # 1|Agrícola
    # 2|Construção Civil
    # 3|Indústria
    # 4|Comércio
    # 5|Administração Pública
    # 6|Serviços de Transporte
    # 7|Serviços
    # 8|Serviços Autônomos
    # 9|Outros
    # 10|Não se aplica
    #
    # [Teste: Checar se existe algum número < 1 ou > 10. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: retorna dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - SETOR_ATIV")

    log_verificador.info("SETOR_ATIV: Situação inicial dos dados: \n" + str(df['SETOR_ATIV'].value_counts()))

    # Getting from the csv file the "CD_UNIF" (unified code for activity sector) correspondent to the "SETOR_ATIV" code
    df['SETOR_ATIV'] = df.apply(lambda row: consulta_refext(row, df_setor, 'COD', 'SETOR_ATIV', 'COD_UNIF'), axis=1)

    # Verifying value interval for check - conditions: "SETOR_ATIV < 0" and "SETOR_ATIV > 10"
    verifica_RANGE(df, 'SETOR_ATIV', 0, 10)

    return df


def passo_ucod(passo, df, ucod, tipo_ucod):
    """
    Na coluna "UCOD_DOM", linha i, ler o valor da linha i da coluna "ZONA_XXX", daí,
    buscar o mesmo valor na coluna "Zona 1977" do arquivo UCOD-1977.csv.
    Ao encotrar, retornar o valor da mesma linha, só que da coluna "UCOD_XXX"
    [Teste: no banco completo, checar se o min == 1 e o max == 67]
    XXX = DOM ou ESC ou TRAB1 ou TRAB2 ou ORIG ou DEST

    :param passo: número do passo para o log
    :param df: Dafaframe a ser modificado
    :param ucod: dataframe com os códigos a serem consultados
    :param tipo_ucod: Tipo da UCOD sendo avaliada (DOM ou ESC ou TRAB1 ou TRAB2 ou ORIG ou DEST)
    :return: retorna o dataframe (df) modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - UCOD_" + tipo_ucod)

    log_acompanhamento.info("UCOD_" + tipo_ucod + ": Consultando referência externa com os parâmetros: " +
                            "Zona 1977 " +
                            "ZONA_" + tipo_ucod +
                            ' UCOD')
    # Getting from the csv file the "UCOD" code correspondent to the "ZONA_XXX" code
    df['UCOD_' + tipo_ucod] = df.apply(lambda row: consulta_refext(row, ucod, 'Zona 1977', 'ZONA_' + tipo_ucod, 'UCOD'), axis=1)

    # Verifying value interval for check - conditions: "UCOD_XXX < 1" and "UCOD_XXX > 67"
    verifica_RANGE(df, 'UCOD_' + tipo_ucod, 1, 67)

    return df


def passo_coord(passo, df, coords, tipo, eixo):
    """
    Na linha i ler o valor da coluna "SUBZONA_TIPO" (ex.: SUBZONA_DOM),
    buscar o valor encontrado no arquivo , na coluna "SUBZONA".
    Retornar o valor da coluna "CO_EIXO" (ex.: CO_X) da linha encontrada no dataframe coord e
    salvar o valor na coluna "CO_TIPO_EIXO" (ex.: CO_DOM_X) na linha atual do dataframe df

    :param passo: número do passo para o log
    :param df: Dafaframe a ser modificado (ex.: od1977)
    :param coords: dataframe com as coordenadas a serem consultadas (ex.: COORD-SUBZONA-1977.csv)
    :param tipo: Tipo da COORD sendo avaliada (DOM ou ESC ou TRAB1 ou TRAB2 ou ORIG ou DEST)
    :param eixo: Eixo a ser consultado (X ou Y)
    :return: retorna o dataframe modificado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - CO_" + tipo + "_" + eixo)

    #Getting from the csv file the "CO_X" code correspondent to the "SUBZONA_DOM" code
    df["CO_" + tipo + "_" + eixo] = df.apply(lambda row: consulta_refext(row, coords, 'SUBZONA', 'SUBZONA_' + tipo, 'CO_' + eixo), axis=1)

    return df


def passo_no_dom(passo, df):
    """
    # Gerando "NO_DOM" como um subindíce de cada "ZONA_DOM"
    # Para cada "ZONA_DOM" o "NO_DOM" será atualizado sempre que "F_DOM" for igual a 1
    # Do contrário, se "F_DOM" for igual a zero, então "NO_DOM" será igual ao "NO_DOM" da linha anterior.
    :param passo:
    :param df:
    :return: retorna dataframe modificado com novo NO_DOM
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - NO_DOM")

    log_verificador.info("NO_DOM: INICIANDO GERAÇÃO DO NO_DOM")

    def gera_no_dom(row):
        # Use this function with:
        #     dataframe.apply(gera_NO_DOM, axis=1)
        #
        # Return 0 if the "NO_DOM" was applied and 1 if it was not (error).
        # row.name is the index of the specific row.

        if row.name == 0 or df.loc[row.name, 'ZONA_DOM'] != df.loc[row.name - 1, 'ZONA_DOM']:
            # If first row of dataframe, then NO_DOM = 1. or
            # If first row of a ZONA_DOM, then NO_DOM = 1 also.
            # This considers that the dataframe is (also) ordered by ZONA_DOM.
            # It is a strong requirement.

            df.loc[row.name, 'NO_DOM'] = 1
        elif row['F_DOM'] == 1:
            df.loc[row.name, 'NO_DOM'] = df.loc[row.name - 1, 'NO_DOM'] + 1
        elif row['F_DOM'] == 0:
            df.loc[row.name, 'NO_DOM'] = df.loc[row.name - 1, 'NO_DOM']
        else:
            log_acompanhamento.warn("NO_DOM: Erro na composição da linha" + str(row.name))
            return 1
        return 0

    # The fucntion gera_NO_DOM is called and due to the fact it returns 1 if well suceeded,
    # it is possible to sum and verify errors existence.
    erros = df.apply(gera_no_dom, axis=1).sum()
    if erros > 0:
        log_acompanhamento.warn("NO_DOM: Número de composições em que ocorreu algum erro: " + str(erros))
    else:
        log_acompanhamento.info("NO_DOM: Nenhum erro encontrado")

    return df


def passo_no_fam(passo, df):
    """
    # Gerando "NO_FAM" como subíndice do "NO_DOM"
    # Para cada "NO_DOM" o "NO_FAM" será incrementado sempre que "F_FAM" for igual a 1
    # Do contrário, caso "F_FAM" seja igual a 0, então "NO_FAM" receberá o valor de "NO_FAM" da linha anterior.
    :param passo:
    :param df:
    :return: dataframe modificado com o NO_FAM gerado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - NO_FAM")

    log_verificador.info("NO_FAM: INICIANDO GERAÇÃO DO NO_FAM")

    def gera_no_fam(row):
        # Use this function with:
        #     dataframe.apply(gera_NO_FAM, axis=1)
        #
        # Return 0 if the "NO_FAM" was applied and 1 if it was not (error).
        # row.name is the index of the specific row.

        if row.name == 0 or df.loc[row.name, 'NO_DOM'] != df.loc[row.name - 1, 'NO_DOM']:
            # If first row of dataframe, then NO_FAM = 1. or
            # If first row of a NO_DOM, then NO_FAM = 1 also.
            # This considers that the dataframe is (also) ordered by NO_DOM.
            # It is a strong requirement.

            df.loc[row.name, 'NO_FAM'] = 1
        elif row['F_FAM'] == 1:
            df.loc[row.name, 'NO_FAM'] = df.loc[row.name - 1, 'NO_FAM'] + 1
        elif row['F_FAM'] == 0:
            df.loc[row.name, 'NO_FAM'] = df.loc[row.name - 1, 'NO_FAM']
        else:
            log_acompanhamento.warn("NO_FAM: Erro na composição da linha" + str(row.name))
            return 1
        return 0

    # The fucntion gera_NO_FAM is called and due to the fact it returns 1 if well suceeded,
    # it is possible to sum and verify errors existence.
    erros = df.apply(gera_no_fam, axis=1).sum()
    if erros > 0:
        log_acompanhamento.warn("NO_FAM: Número de composições em que ocorreu algum erro: " + str(erros))
    else:
        log_acompanhamento.info("NO_FAM: Nenhum erro encontrado")

    return df


def passo_no_pess(passo, df):
    """
    # Gerando "NO_PESS" como subíndice do "NO_FAM"
    # Para cada "NO_FAM" o "NO_PESS" será incrementado sempre que "F_PESS" for igual a 1
    # Do contrário, caso "F_PESS" seja igual a 0, então "NO_PESS" receberá o valor de "NO_PESS" da linha anterior.
    :param passo:
    :param df:
    :return: retorna o dataframe modificado com o novo NO_PESS
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - NO_PESS")

    log_verificador.info("NO_PESS: INICIANDO GERAÇÃO DO NO_PESS")

    def gera_no_pess(row):
        # Use this function with:
        #     dataframe.apply(gera_NO_PESS, axis=1)
        #
        # Return 0 if the "NO_PESS" was applied and 1 if it was not. (error)
        # row.name is the index of the specific row.

        if row.name == 0 or df.loc[row.name, 'NO_FAM'] != df.loc[row.name - 1, 'NO_FAM']:
            # If first row of dataframe, then NO_PESS = 1. or
            # If first row of a NO_FAM, then NO_PESS = 1 also.
            # This considers that the dataframe is (also) ordered by NO_FAM.
            # It is a strong requirement.

            df.loc[row.name, 'NO_PESS'] = 1
        elif row['F_PESS'] == 1:
            df.loc[row.name, 'NO_PESS'] = df.loc[row.name - 1, 'NO_PESS'] + 1
        elif row['F_PESS'] == 0:
            df.loc[row.name, 'NO_PESS'] = df.loc[row.name - 1, 'NO_PESS']
        else:
            log_acompanhamento.warn("NO_PESS: Erro na composição da linha" + str(row.name))
            return 0
        return 1

    # The fucntion gera_NO_PESS is called and due to the fact it returns 1 if well suceeded,
    # it is possible to sum and verify errors existence.
    erros = df.apply(gera_no_pess, axis=1).sum()
    if erros > 0:
        log_acompanhamento.warn("NO_PESS: Número de composições em que ocorreu algum erro: " + str(erros))
    else:
        log_acompanhamento.info("NO_PESS: Nenhum erro encontrado")

    return df


def passo_no_viag(passo, df):
    """
    # Gerando "NO_VIAG" como subíndice do "NO_PESS"
    # Para cada "NO_PESS" o "NO_VIAG" será incrementado sempre que "F_PESS" for igual a 1
    # Do contrário, caso "F_PESS" seja igual a 0, então "NO_VIAG" receberá o valor de "NO_VIAG" da linha anterior.
    :param passo:
    :param df:
    :return: retorna o dataframe modificado com o novo NO_PESS
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - NO_VIAG")

    log_verificador.info("NO_VIAG: INICIANDO GERAÇÃO DO NO_VIAG")

    def gera_no_viag(row):
        # Use this function with:
        #     dataframe.apply(gera_NO_VIAG, axis=1)
        #
        # Return 1 if the "NO_VIAG" was applied and 0 if it was not.
        # row.name is the index of the specific row.

        if row.name == 0 or df.loc[row.name, 'NO_PESS'] != df.loc[row.name - 1, 'NO_PESS']:
            # If first row of dataframe, then NO_VIAG = 1. or
            # If first row of a NO_PESS, then NO_VIAG = 1 also.
            # This considers that the dataframe is (also) ordered by NO_PESS.
            # It is a strong requirement.

            df.loc[row.name, 'NO_VIAG'] = 1
        elif row['F_PESS'] == 1:
            df.loc[row.name, 'NO_VIAG'] = df.loc[row.name - 1, 'NO_VIAG'] + 1
        elif row['F_PESS'] == 0:
            df.loc[row.name, 'NO_VIAG'] = df.loc[row.name - 1, 'NO_VIAG']
        else:
            log_acompanhamento.warn("NO_VIAG: Erro na composição da linha" + str(row.name))
            return 1
        if row['FE_VIAG'] == 0:
            df.loc[row.name, 'NO_VIAG'] = 0
        return 0

    # The fucntion gera_NO_VIAG is called and due to the fact it returns 1 if well suceeded,
    # it is possible to sum and verify errors existence.

    erros = df.apply(gera_no_viag, axis=1).sum()
    if erros > 0:
        log_acompanhamento.warn("NO_VIAG: Número de composições em que ocorreu algum erro: " + str(erros))
    else:
        log_acompanhamento.info("NO_VIAG: Nenhum erro encontrado")

    return df


def passo_id_dom(passo, df):
    """
    # Construir o "NO_DOM" e o "ID_DOM"
    #
    # [Na coluna "ID_DOM", linha i, ler o valor da linha i da coluna "ZONA_DOM",
    # e concatenar esse valor (com 3 dígitos) com o número do domicílio,
    # que é o valor da linha i da coluna "NO_DOM" (com 4 dígitos).
    # Resultado será um ID_DOM, que pode se repetir nas linhas, de 7 dígitos.
    # Isso deve ser concatenado com o "Ano". Resultado = 8 dígitos]
    :param passo:
    :param df:
    :return: retorna dataframe modificado com o ID_DOM gerado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ID_DOM")
    def gera_id_dom(row):
        """
        Gera o ID_DOM baseado no 'ANO', na 'ZONA_DOM' e no 'NO_DOM'
        O argumento passado é a "linha".
            Uso:
            od1977['ID_DOM'] = od1977.apply(gera_ID_DOM, axis=1)
        Retorna: ID_DOM da respectiva linha
        """
        ano = int(row['ANO'])
        zona = int(row['ZONA_DOM'])
        no_dom = int(row['NO_DOM'])
        return int(str(ano)+str('%03d' % zona) + str('%04d' % no_dom))

    # Generating "ID_DOM" from the concatenation of "ANO", "ZONA_DOM" and "NO_DOM" variables
    df['ID_DOM'] = df.apply(gera_id_dom, axis=1)

    return df


def passo_id_fam(passo, df):
    """
    # Construir "ID_FAM"
    # Na coluna "ID_FAM", linha i, ler o valor da linha i da coluna "ID_DOM",
    # e concatenar esse valor (com 8 dígitos) com o número da família,
    # que é o valor da linha i da coluna "NO_FAM" (com 2 dígitos).
    #
    # Resultado será um ID_FAM, que pode se repetir nas linhas, de 10 dígitos.
    :param passo:
    :param df:
    :return: retorna o dataframe com o ID_FAM atualizado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ID_FAM")

    def gera_id_fam(row):
        """
        Gera o ID_FAM baseado no 'ID_DOM' e no 'NO_FAM'
        O argumento passado é a "linha".
            Uso:
            od1977['ID_FAM'] = od1977.apply(gera_ID_FAM, axis=1)
        Retorna: ID_FAM da respectiva linha
        """
        id_dom = int(row['ID_DOM'])
        no_fam = int(row['NO_FAM'])
        return int(str(id_dom) + str('%02d' % no_fam))

    #Generating "ID_FAM" from the concatenation of "ID_DOM" and "NO_FAM" variables
    df['ID_FAM'] = df.apply(gera_id_fam, axis=1)

    return df


def passo_id_pess(passo, df):
    """
    # Construir "ID_PESS" e "NO_PESS"
    # Na coluna "ID_PESS", linha i, ler o valor da linha i da coluna "ID_FAM", e
    # concatenar esse valor (10 dígitos) com o número da pessoa,
    # que é o valor da linha i da coluna "NO_PESS" (com 2 dígitos).
    #
    # Resultado será um ID_PESS, que pode se repetir nas linhas, de 12 dígitos.
    :param passo:
    :param df:
    :return: retorna o dataframe com o ID_PESS atualizado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ID_PESS")

    def gera_id_pess(row):
        """
        Gera o ID_PESS baseado no 'ID_FAM' e no 'NO_PESS'
        O argumento passado é a "linha".
            Uso:
            od1977['ID_PESS'] = od1977.apply(gera_ID_PESS, axis=1)
        Retorna: ID_PESS da respectiva linha
        """
        id_fam = int(row['ID_FAM'])
        no_pess = int(row['NO_PESS'])
        return int(str(id_fam) + str('%02d' % no_pess))

    #Generating "ID_PESS" from the concatenation of "ID_FAM" and "NO_PESS" variables
    df['ID_PESS'] = df.apply(gera_id_pess, axis=1)

    return df


def passo_id_viag(passo, df):
    """
    # Construir "ID_VIAG" e "NO_VIAG"
    # Na coluna "ID_VIAG", linha i, ler o valor da linha i da coluna "ID_PESS", e
    # concatenar esse valor (12 dígitos) com o número da pessoa,
    # que é o valor da linha i da coluna "NO_VIAG" (com 2 dígitos).
    #
    # Resultado será um ID_VIAG, que pode se repetir nas linhas, 14 dígitos.
    :param passo:
    :param df:
    :return: retorna o dataframe com o ID_VIAG atualizado
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ID_VIAG")

    def gera_id_viag(row):
        """
        Gera o ID_VIAG baseado no 'ID_PESS' e no 'NO_VIAG'
        O argumento passado é a "linha".
            Uso:
            od1977['ID_VIAG'] = od1977.apply(gera_ID_VIAG, axis=1)
        Retorna ID_VIAG da respectiva linha
        """
        id_pess = int(row['ID_PESS'])
        no_viag = int(row['NO_VIAG'])
        return int(str(id_pess) + str('%02d' % no_viag))

    # Generating "ID_VIAG" from the concatenation of "ID_PESS" and "NO_VIAG" variables
    df['ID_VIAG'] = df.apply(gera_id_viag, axis=1)

    return df


def passo_tot_viag(passo, df):
    """
    # Calcula e confere o campo TOT_VIAG, baseado no maior valor de NO_VIAG para cada pessoa
    :param passo:
    :param df:
    :return: retorna o dataframe com o "TOT_VIAG" corrigido
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - TOT_VIAG")

    log_verificador.info("TOT_VIAG: Situação inicial dos dados: \n" + str(df['TOT_VIAG'].describe()))
    log_verificador.info("TOT_VIAG: Situação inicial dos dados: \n" + str(df['TOT_VIAG'].value_counts()))

    #df[:30][['ID_PESS','NO_VIAG','TOT_VIAG']]

    def atrib_tot_viag(row):
        df.loc[df['ID_PESS']==row['ID_PESS'],'TOT_VIAG'] = row['NO_VIAG']
        # print('id_pessoa: ' + str(row['ID_PESS']) + ' | no_viag:' + str(row['NO_VIAG'])
        # print(row)

    df.loc[:,['ID_PESS','NO_VIAG']].groupby(['ID_PESS'],sort=False).agg({'NO_VIAG':max,'ID_PESS':max}).apply(atrib_tot_viag, axis=1)
    # od1977[od1977['ID_PESS']==20002030404][['ID_PESS','NO_VIAG']]

    #display(od1977.loc[:90,['ID_PESS','NO_VIAG','TOT_VIAG']])
    # Verificar as viagens do ID_PESS = 100100070403
    # display(od1977.loc[od1977['ID_PESS']==100100070403,['ID_PESS','NO_VIAG','TOT_VIAG','ID_DOM','NO_DOM','ID_VIAG']])

    log_verificador.info("TOT_VIAG: Situação final dos dados: \n" + str(df['TOT_VIAG'].describe()))
    log_verificador.info("TOT_VIAG: Situação final dos dados: \n" + str(df['TOT_VIAG'].value_counts()))

    # Agora uma função que irá verificar se para todo "ID_PESS" o "TOT_VIAG" é igual ao 'NO_VIAG' máximo.
    def verifica_no_viag_tot_viag(row):
        if row['NO_VIAG'] != row['TOT_VIAG']:
            log_verificador.warn('TOT_VIAG: Erro encontrado na linha\n' + str(row) + '\n\n')
            #print(row)
    df.loc[:,['ID_PESS','NO_VIAG','TOT_VIAG']].groupby('ID_PESS').agg({'NO_VIAG':'max','ID_PESS':'max','TOT_VIAG':'max'}).apply(verifica_no_viag_tot_viag, axis=1)

    return df


def passo_cd_entre(passo, df):
    """
    # -----
    # ##Passo 3: "CD_ENTRE"
    # Substituir valores da coluna "CD_ENTRE"
    # Todas viagens são consideradas "completas", segundo informações do Metrô
    #
    # * sem viagem: se TOT_VIAG == 0
    # * com viagem: se TOT_VIAG != 0
    #
    # ####Categorias novas
    # | Valor | Descrição |
    # | -------- | -------- |
    # | 0 | Completa sem viagem |
    # | 1 | Completa com viagem |
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return:
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - CD_ENTRE")

    log_verificador.info("CD_ENTRE: Dados antes da modificação")
    log_verificador.info("CD_ENTRE: \n" + str(df['CD_ENTRE'].value_counts()))

    # Definindo 'CD_ENTRE' baseado no valor de 'TOT_VIAG'
    df.loc[df['TOT_VIAG'] == 0, 'CD_ENTRE'] = 0
    df.loc[df['TOT_VIAG'] != 0, 'CD_ENTRE'] = 1

    # Verifying if there was left some value other than 0 or 1
    verifica_DUMMY(df, 'CD_ENTRE')

    return df


def passo_cd_renfam(passo, df):
    """
    # Substituir valores da coluna "CD_RENFAM"
    #
    # * Excluir único registro na categoria 3
    # * Substituir todos valores **1** por **2**
    # * Quando categoria original for 0 (respondeu) checar no campo REN_FAM se o valor é nulo.
    #      Se for nulo, manter na categoria 0 (Renda Familiar Declarada como Zero), senão,
    #      mover para a categoria 1 (Renda Familiar Declarada e Maior que Zero).
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 0|Respondeu
    # 1|Não Sabe
    # 2|Não Respondeu
    # 3|Não se Aplica
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Renda Familiar Declarada como Zero
    # 1|Renda Familiar Declarada e Maior que Zero
    # 2|Renda Atribuída
    #
    # [Teste: Checar se existe algum número < 0 ou > 2. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: retorna o dataframe corrigido
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - CD_RENFAM")

    log_verificador.info("CD_RENFAM: Dados antes da modificação")
    log_verificador.info("CD_RENFAM: \n" + str(df['CD_RENFAM'].value_counts()))

    # Replacing the values 1 for 2
    df.loc[df['CD_RENFAM']==1,'CD_RENFAM'] = 2

    def melhora_cd_renfam(row):
        # * Quando categoria original for 0 (respondeu) checar no campo REN_FAM se o valor é nulo.
        #      Se for nulo, manter na categoria 0 (Renda Familiar Declarada como Zero), senão,
        #      mover para a categoria 1 (Renda Familiar Declarada e Maior que Zero).
        #
        if row['CD_RENFAM'] == 0 and row['REN_FAM'] != 0 and row['REN_FAM'] is not None:
            df.loc[row.name, 'CD_RENFAM'] = 1

    # Spliting the category 0 "Respondeu" into 0 "Renda Familiar Declarada como Zero" and 1 "Renda Familiar Declarada e Maior que Zero"
    df.apply(melhora_cd_renfam, axis=1)

    # Verifying value interval for check - conditions: "CD_RENFAM < 0" and "CD_RENFAM > 2"
    verifica_RANGE(df, 'CD_RENFAM', 0, 2)

    return df


def passo_estuda(passo, df):
    """
    #  Se zona da escola for zero (==0) então não estuda (0), senão, não estuda (1)
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não estuda
    # 1|Estuda
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    :param passo:
    :param df:
    :return: retorna dataframe com campo ESCOLA resolvido
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - ESTUDA")

    log_verificador.info("ESTUDA: Dados antes da modificação")
    log_verificador.info("ESTUDA: \n" + str(df['ESTUDA'].value_counts()))

    df['ESTUDA'] = 1
    log_verificador.info("ESTUDA: \n" + str(df['ESTUDA'].value_counts()))
    df.loc[df['ZONA_ESC'] == 0, 'ESTUDA'] = 0

    # Verifying if there was left some value other than 0 or 1
    verifica_DUMMY(df, 'ESTUDA')

    return df


#TODO >> Falta um teste de verificação!!!
def passo_dist_viag(passo, df):
    """
    # Calcula-se a distância euclidiana (a partir da CO_ORIG_X;CO_ORIG_Y e CO_DEST_X;CO_DEST_Y)
    :param passo:
    :param df:
    :return: Retorna dataframe com DIST_VIAG calculada e preenchida
    """
    log_acompanhamento.info("### PASSO " + str(passo) + " - DIST_VIAG")

    def calcula_dist_viag(row):
        """
        Calcula a distância euclidiana dadas as coordenadas (x,y) de origem e coordenadas (x,y) de destino da viagem.
        O argumento passado é a "linha".
            Uso:
            od1977['DIST_VIAG'] = od1977.apply(calcula_DIST_VIAG, axis=1)
        Retorna: DIST_VIAG da respetiva linha
        """
        co_orig_x = float(row['CO_ORIG_X'])
        co_orig_y = float(row['CO_ORIG_Y'])
        co_dest_x = float(row['CO_DEST_X'])
        co_dest_y = float(row['CO_DEST_Y'])
        return math.sqrt(math.pow((co_orig_x - co_dest_x), 2) + math.pow((co_orig_y - co_dest_y), 2))

    # Calculating "DIST_VIAG" (euclidian distance) from the origin cordinates (CO_ORIG_X;CO_ORIG_Y)
    #                                               and destinations cordinates (CO_DEST_X;CO_DEST_Y)
    df['DIST_VIAG'] = df.apply(lambda row: calcula_dist_viag(row), axis=1)

    return df


def main():

    # ----

    log_acompanhamento.info('\nReading csv files and generating dataframes')

    # Reading csv file and store its contend in an intern dataframe
    od1977 = pd.read_csv('OD_1977.csv', sep=';', decimal=',')

    # Reading other accessory files that will be used on the consulta_refext function
    ucod1977 = pd.read_csv('UCOD-1977.csv', sep=';')
    setor_ativ1977 = pd.read_csv('setor_ativ-1977.csv', sep=';')
    coord_subzona1977 = pd.read_csv('coord_subzonas_1987.csv', sep=';')

    # Filtering the dataframe to get a smaller sample
    # logging.info('\nFiltering the main dataframe to get just a sample')
    od1977 = od1977[:20000]

    log_acompanhamento.info('\nBasic column creation or rename on the main dataframe')
    # Renaming the column UCOD to UCOD_DOM
    od1977.rename(columns={'UCOD':'UCOD_DOM'}, inplace=True)

    # Creating the column UCOD_ESC (it will go to the end of dataframe)
    od1977['UCOD_ESC']=None

    # Creating the column UCOD_TRAB1 (it will go to the end of dataframe)
    od1977['UCOD_TRAB1']=None

    # Creating the column UCOD_TRAB2 (it will go to the end of dataframe)
    od1977['UCOD_TRAB2']=None

    # Creating the column UCOD_ORIG (it will go to the end of dataframe)
    od1977['UCOD_ORIG']=None

    # Creating the column UCOD_DEST (it will go to the end of dataframe)
    od1977['UCOD_DEST']=None

    # Reordering the columns, precisely, these that were just created (at the end of dataframe)
    # near to other contend related variables
    od1977 = od1977[['ANO',
     'CD_ENTRE',
     'DIA_SEM',
     'UCOD_DOM',
     'ZONA_DOM',
     'SUBZONA_DOM',
     'MUN_DOM',
     'CO_DOM_X',
     'CO_DOM_Y',
     'ID_DOM',
     'F_DOM',
     'FE_DOM',
     'NO_DOM',
     'TIPO_DOM',
     'TOT_FAM',
     'ID_FAM',
     'F_FAM',
     'FE_FAM',
     'NO_FAM',
     'COND_MORA',
     'QT_AUTO',
     'QT_BICI',
     'QT_MOTO',
     'CD_RENFAM',
     'REN_FAM',
     'ID_PESS',
     'F_PESS',
     'FE_PESS',
     'NO_PESS',
     'SIT_FAM',
     'IDADE',
     'SEXO',
     'ESTUDA',
     'GRAU_INSTR',
     'OCUP',
     'SETOR_ATIV',
     'CD_RENIND',
     'REN_IND',
     'UCOD_ESC',
     'ZONA_ESC',
     'SUBZONA_ESC',
     'MUN_ESC',
     'CO_ESC_X',
     'CO_ESC_Y',
     'UCOD_TRAB1',
     'ZONA_TRAB1',
     'SUBZONA_TRAB1',
     'MUN_TRAB1',
     'CO_TRAB1_X',
     'CO_TRAB1_Y',
     'UCOD_TRAB2',
     'ZONA_TRAB2',
     'SUBZONA_TRAB2',
     'MUN_TRAB2',
     'CO_TRAB2_X',
     'CO_TRAB2_Y',
     'ID_VIAG',
     'F_VIAG',
     'FE_VIAG',
     'NO_VIAG',
     'TOT_VIAG',
     'UCOD_ORIG',
     'ZONA_ORIG',
     'SUBZONA_ORIG',
     'MUN_ORIG',
     'CO_ORIG_X',
     'CO_ORIG_Y',
     'UCOD_DEST',
     'ZONA_DEST',
     'SUBZONA_DEST',
     'MUN_DEST',
     'CO_DEST_X',
     'CO_DEST_Y',
     'DIST_VIAG',
     'MOTIVO_ORIG',
     'MOTIVO_DEST',
     'MODO1',
     'MODO2',
     'MODO3',
     'MODO4',
     'MODO_PRIN',
     'TIPO_VIAG',
     'H_SAIDA',
     'MIN_SAIDA',
     'ANDA_ORIG',
     'H_CHEG',
     'MIN_CHEG',
     'ANDA_DEST',
     'DURACAO',
     'TIPO_EST_AUTO',
     'VALOR_EST_AUTO']]


    # Describing dataframe columns
    log_acompanhamento.debug('\n# Printing "cols" variable to check if the reorder operation was effective')
    log_acompanhamento.debug(od1977.columns.tolist())

    # Describing data (whole dataframe)- count, mean, std, min and max
    log_acompanhamento.debug('\n# Describing data (whole dataframe)- count, mean, std, min and max')
    log_acompanhamento.debug(od1977.describe())


    #Contador de 'PASSO'
    passo = 0

    # -----
    # #Variáveis Independentes de outras variáveis e sem consulta externa

    # -----
    # ##Passo 1: "ANO"
    # Preenche a coluna "ANO" com  valor 1 em todas células
    #
    # ####Categorias:
    # |valor|ano_correspondente|
    # |-------|-----|
    # |1|1977|
    # |2|1987|
    # |3|1997|
    # |4|2007|
    od1977 = passo_ano(passo, od1977)
    passo += 1

    # ----
    # ##Passo 2: "DIA_SEM"
    # Não existe essa informação no banco de dados de 1977, logo, este campo será preenchido com 'None'.
    #
    # ####Categorias:
    # Valor|Descrição
    # -----|-----
    # 0|Não disponível
    # 2|Segunda-Feira
    # 3|Terça-Feira
    # 4|Quarta-Feira
    # 5|Quinta-Feira
    # 6|Sexta-Feira
    od1977 = passo_dia_sem(passo, od1977)
    passo += 1

    # -----
    # ##Passo 3: "ZONA_DOM"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    passo_zona_dom(passo, od1977)
    passo += 1

    # -----
    # ##Passo 4: "SUBZONA_DOM"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    passo_subzona_dom(passo, od1977)
    passo += 1

    # -----
    # ##Passo 5: "MUN_DOM"
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    passo_mun_dom(passo, od1977)
    passo += 1

    # -----
    # ##Passo 6: "F_DOM"
    # Checar se existe algum erro na coluna "F_DOM"
    #
    # ####Categorias
    # Valor|Descrição
    # ----|----
    # 0|Demais registros
    # 1|Primeiro Registro do Domicílio
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    passo_f_dom(passo, od1977)
    passo += 1

    # -----
    # ##"FE_DOM"
    # Nada há que se fazer em relação aos dados da coluna "FE_DOM"

    # -----
    # ##Passo 7: "TIPO_DOM"
    # Checar se existe algum erro
    #
    # ####Categorias anteriores / novas
    # Valor | Descrição
    # ----|----
    # 0|Não respondeu
    # 1|Particular
    # 2|Coletivo
    #
    # [Teste: Checar se existe algum número < 0 ou > 2. Se encontrar, retornar erro indicando em qual linha.]
    passo_tipo_dom(passo, od1977)
    passo += 1

    # -----
    # ##"TOT_FAM"
    # Nada há que se fazer em relação aos dados da coluna "TOT_FAM"

    # -----
    # ##Passo 8: "F_FAM"
    # Checar se existe algum erro na coluna "F_FAM"
    #
    # ####Categorias
    # Valor|Descrição
    # ----|----
    # 0|Demais registros
    # 1|Primeiro Registro da Família
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    passo_f_fam(passo, od1977)
    passo += 1

    # -----
    # ##"FE_FAM"
    # Nada há que se fazer em relação aos dados da coluna "FE_FAM"

    # -----
    # ##Passo 9: "COND_MORA"
    # Substituir valores da coluna "COND_MORA"
    #
    # * Substituir todos valores **1** por **2**
    # * Substituir todos valores **3** por **1**
    # * Substituir todos valores **4** por **3**
    # * Substituir todos valores **5** por **3**
    # * Substituir todos valores **0** por **4**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Própria paga
    # 2|Própria em pagamento
    # 3|Alugada
    # 4|Cedida
    # 5|Outro
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 1|Alugada
    # 2|Própria
    # 3|Outros
    # 4|Não respondeu
    #
    # [Teste: Checar se existe algum número < 1 ou > 4. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_cond_mora(passo, od1977)
    passo += 1

    # -----
    # ##"QT_AUTO"
    # Nada há que se fazer em relação aos dados da coluna "QT_AUTO"

    # -----
    # ##Passo 10: "QT_BICI"
    # Não existe essa informação no banco de dados de 1977, logo, este campo será preenchido com 'None'.
    od1977 = passo_qt_bici(passo, od1977)
    passo += 1

    # -----
    # ##Passo 11: "QT_MOTO"
    # Não existe essa informação no banco de dados de 1977, logo, este campo será preenchido com 'None'.
    od1977 = passo_qt_moto(passo, od1977)
    passo += 1

    # -----
    # ##"REN_FAM"
    # Nada há que se fazer em relação aos dados da colunas "REN_FAM"

    # -----
    # ##Passo 12: "F_PESS"
    # Checar se existe algum erro na coluna "F_PESS"
    #
    # ####Categorias
    # Valor|Descrição
    # ----|----
    # 0|Demais registros
    # 1|Primeiro Registro da Pessoa
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    passo_f_pess(passo, od1977)
    passo += 1

    # -----
    # ##"FE_PESS"
    # Nada há que se fazer em relação aos dados das colunas "FE_PESS"

    # -----
    # ##Passo 13: "SIT_FAM"
    #
    # * Substituir todos valores **5** por **4**
    # * Substituir todos valores **6** por **5**
    # * Substituir todos valores **7** por **6**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Chefe
    # 2|Cônjuge
    # 3|Filho(a)
    # 4|Parente
    # 5|Agregado
    # 6|Empregado Residente
    # 7|Visitante não Residente
    #
    # ####Categorias novas:
    # Valor|Descrição
    # ----|----
    # 1| Pessoa Responsável
    # 2| Cônjuge/Companheiro(a)
    # 3| Filho(a)/Enteado(a)
    # 4| Outro Parente / Agregado
    # 5| Empregado Residente
    # 6| Outros (visitante não residente / parente do empregado)
    #
    # [Teste: Checar se existe algum número < 1 ou > 6. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_sit_fam(passo, od1977)
    passo += 1

    # -----
    # ##"IDADE"
    # Nada há que se fazer em relação aos dados da coluna "IDADE"

    # -----
    # ##Passo 14: "SEXO"
    # Substituir valores da coluna "SEXO"
    #
    # * Substituir todos valores **2** por **0**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Masculino
    # 2|Feminino
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Feminino
    # 1|Masculino
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_sexo(passo, od1977)
    passo += 1

    # -----
    # ##Passo 15: "GRAU_INSTR"
    # Substituir valores da coluna "GRAU_INSTR"
    #
    # * Substituir todos valores **2** por **1**
    # * Substituir todos valores **3** por **1**
    # * Substituir todos valores **4** por **1**
    # * Substituir todos valores **5** por **2**
    # * Substituir todos valores **6** por **2**
    # * Substituir todos valores **7** por **3**
    # * Substituir todos valores **8** por **3**
    # * Substituir todos valores **9** por **4**
    #
    # ####Categorias anteriores:
    # Valor|Descrição
    # ----|----
    # 1|Sem Instrução
    # 2|Primário Incompleto
    # 3|Primário Completo
    # 4|Ginasial Incompleto
    # 5|Ginasial Completo
    # 6|Colegial Incompleto
    # 7|Colegial Completo
    # 8|Universitário Incompleto
    # 9|Universitário Completo
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não declarou
    # 1|Não-Alfabetizado/Fundamental Incompleto
    # 2|Fundamental Completo/Médio Incompleto
    # 3|Médio Completo/Superior Incompleto
    # 4|Superior completo
    #
    # [Teste: Checar se existe algum número < 1 ou > 4. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_grau_instr(passo, od1977)
    passo += 1

    # -----
    # ##Passo 16: "OCUP"
    # Substituir valores da coluna "OCUP"
    #
    # * Substituir todos valores **1** por **7**
    # * Substituir todos valores **2** por **6**
    # * Substituir todos valores **4** por **5**
    # * Substituir todos valores **5** por **4**
    # * Substituir todos valores **6** por **2**
    # * Substituir todos valores **7** em diante por **1**
    #
    # ####Categorias anteriores:
    # Valor|Descrição
    # ----|----
    # 1|Estudante
    # 2|Prendas Domésticas
    # 3|Aposentado
    # 4|Sem Ocupação (nunca trabalhou)
    # 5|Desempregado
    # 6|Em licença
    # 7 em diante|diversas profissões
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 1|Tem trabalho
    # 2|Em licença médica
    # 3|Aposentado / pensionista
    # 4|Desempregado
    # 5|Sem ocupação
    # 6|Dona de casa
    # 7|Estudante
    #
    # [Teste: Checar se existe algum número < 0 ou > 7. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_ocup(passo, od1977)
    passo += 1

    # ##Passo 17: "CD_RENIND"
    # Checar se existe algum erro na coluna "CD_RENIND"
    #
    # ####Categorias
    # Valor|Descrição
    # ----|----
    # 1|Tem renda
    # 2|Não tem renda
    # 3|Não declarou
    #
    # [Teste: Checar se existe algum número < 1 ou > 3. Se encontrar, retornar erro indicando em qual linha.]
    passo_cd_renind(passo, od1977)
    passo += 1

    # -----
    # ##"REN_IND"
    # Nada há que se fazer em relação aos dados da coluna "REN_IND"

    # -----
    # ##Passo 18: "ZONA_ESC"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    passo_zona_esc(passo, od1977)
    passo += 1

    # -----
    # ##Passo 19: "SUBZONA_ESC"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    passo_subzona_esc(passo, od1977)
    passo += 1

    # -----
    # ##Passo 20: "MUN_ESC"
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    passo_mun_esc(passo, od1977)
    passo += 1

    # -----
    # ##Passo 21: "ZONA_TRAB1"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    passo_zona_trab1(passo, od1977)
    passo += 1

    # -----
    # ##Passo 22: "SUBZONA_TRAB1"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    passo_subzona_trab1(passo, od1977)
    passo += 1

    # -----
    # ##Passo 23: "MUN_TRAB1"
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    passo_mun_trab1(passo, od1977)
    passo += 1

    # -----
    # ##Passo 24: "ZONA_TRAB2"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    passo_zona_trab2(passo, od1977)
    passo += 1

    # -----
    # ##Passo 25: "SUBZONA_TRAB2"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    passo_subzona_trab2(passo, od1977)
    passo += 1

    # -----
    # ##Passo 26: "MUN_TRAB2"
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    passo_mun_trab2(passo, od1977)
    passo += 1

    # -----
    # ##Passo 27: "F_VIAG"
    # Excluir a coluna "F_VIAG", porque as viagens são numeradas,
    # então já se saber pelo NO_VIAG qual é a primeira do indivíduo.
    od1977 = passo_f_viag(passo, od1977)
    passo += 1

    # -----
    # ##"FE_VIAG"
    # Nada há que se fazer em relação aos dados da coluna "FE_VIAG"

    # -----
    # ##Passo 28: "ZONA_ORIG"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    passo_zona_orig(passo, od1977)
    passo += 1

    # -----
    # ##Passo 29: "SUBZONA_ORIG"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    passo_subzona_orig(passo, od1977)
    passo += 1

    # -----
    # ##Passo 30: "MUN_ORIG"
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    passo_mun_orig(passo, od1977)
    passo += 1

    # -----
    # ##Passo 31: "ZONA_DEST"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 243
    #
    # [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
    passo_zona_dest(passo, od1977)
    passo += 1

    # -----
    # ##Passo 32: "SUBZONA_DEST"
    # Checar se existe algum erro
    #
    # ####Categorias:
    # > 1 a 633
    #
    # [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]
    passo_subzona_dest(passo, od1977)
    passo += 1

    # -----
    # ##Passo 33: "MUN_DEST"
    # Checar se existe algum erro
    #
    # ####Categorias
    # > 1 a 27
    #
    # [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]
    passo_mun_dest(passo, od1977)
    passo += 1

    # -----
    # ##Passo 34: "MOTIVO_ORIG"
    # * Substituir todos valores **6** por **11**
    # * Substituir todos valores **7** por **6**
    # * Substituir todos valores **8** por **7**
    # * Substituir todos valores **10** por **8**
    # * Substituir todos valores **11** por **9**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Trabalho/Indústria
    # 2|Trabalho/Comércio
    # 3|Trabalho/Serviços
    # 4|Escola/Educação
    # 5|Compras
    # 6|Negócios
    # 7|Médico/Dentista/Saúde
    # 8|Recreação/Visitas
    # 9|Servir Passageiro
    # 10|Residência
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Trabalho/Indústria
    # 2|Trabalho/Comércio
    # 3|Trabalho/Serviços
    # 4|Educação
    # 5|Compras
    # 6|Saúde
    # 7|Lazer
    # 8|Residência
    # 9|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 9. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_motivo_orig(passo, od1977)
    passo += 1

    # -----
    # ##Passo 35: "MOTIVO_DEST"
    # * Substituir todos valores **6** por **11**
    # * Substituir todos valores **7** por **6**
    # * Substituir todos valores **8** por **7**
    # * Substituir todos valores **10** por **8**
    # * Substituir todos valores **11** por **9**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Trabalho/Indústria
    # 2|Trabalho/Comércio
    # 3|Trabalho/Serviços
    # 4|Escola/Educação
    # 5|Compras
    # 6|Negócios
    # 7|Médico/Dentista/Saúde
    # 8|Recreação/Visitas
    # 9|Servir Passageiro
    # 10|Residência
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Trabalho/Indústria
    # 2|Trabalho/Comércio
    # 3|Trabalho/Serviços
    # 4|Educação
    # 5|Compras
    # 6|Saúde
    # 7|Lazer
    # 8|Residência
    # 9|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 9. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_motivo_dest(passo, od1977)
    passo += 1

    # -----
    # ##Passo 36: "MODO1"
    # Não há o que fazer com os valores da coluna "MODO1"
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Ônibus trólebus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua
    # 7|Metrô
    # 8|Trem
    # 9|Motocicleta
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Ônibus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua / Van / Microônibus
    # 7|Metrô
    # 8|Trem
    # 9|Moto
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 12. Se encontrar, retornar erro indicando em qual linha.]
    passo_modo1(passo, od1977)
    passo += 1

    # -----
    # ##Passo 37: "MODO2"
    # Não há o que fazer com os valores da coluna "MODO2"
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Ônibus trólebus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua
    # 7|Metrô
    # 8|Trem
    # 9|Motocicleta
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Ônibus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua / Van / Microônibus
    # 7|Metrô
    # 8|Trem
    # 9|Moto
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 12. Se encontrar, retornar erro indicando em qual linha.]
    passo_modo2(passo, od1977)
    passo += 1

    # -----
    # ##Passo 38: "MODO3"
    # Não há o que fazer com os valores da coluna "MODO3"
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Ônibus trólebus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua
    # 7|Metrô
    # 8|Trem
    # 9|Motocicleta
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Ônibus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua / Van / Microônibus
    # 7|Metrô
    # 8|Trem
    # 9|Moto
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 12. Se encontrar, retornar erro indicando em qual linha.]
    passo_modo3(passo, od1977)
    passo += 1

    # -----
    # ##Passo 39: "MODO4"
    # Não há o que fazer com os valores da coluna "MODO4"
    passo_modo4(passo, od1977)
    passo += 1

    # -----
    # ##Passo 40: "MODO_PRIN"
    # Não há o que fazer com os valores da coluna "MODO_PRIN"
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Ônibus trólebus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua
    # 7|Metrô
    # 8|Trem
    # 9|Motocicleta
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu/não fez viagem
    # 1|Ônibus
    # 2|Ônibus Escolar / Empresa
    # 3|Dirigindo Automóvel
    # 4|Passageiro de Automóvel
    # 5|Táxi
    # 6|Lotação / Perua / Van / Microônibus
    # 7|Metrô
    # 8|Trem
    # 9|Moto
    # 10|Bicicleta
    # 11|A Pé
    # 12|Outros
    #
    # [Teste: Checar se existe algum número < 0 ou > 12. Se encontrar, retornar erro indicando em qual linha.]
    passo_modo_prin(passo, od1977)
    passo += 1

    # -----
    # ##"TIPO_VIAG"; "H_SAIDA"; "MIN_SAIDA"; "ANDA_ORIG"; "H_CHEG"; "MIN_CHEG"; "ANDA_DEST" e "DURACAO"
    # Nada há que se fazer em relação aos dados das colunas "TIPO_VIAG"; "H_SAIDA"; "MIN_SAIDA"; "ANDA_ORIG"; "H_CHEG"; "MIN_CHEG"; "ANDA_DEST" e "DURACAO"

    # -----
    # ##Passo 41: "TIPO_EST_AUTO"
    # Substituir valores da coluna "TIPO_EST_AUTO"
    #
    # * Substituir todos valores **1** por **5**
    # * Substituir todos valores **2** por **2**
    # * Substituir todos valores **3** por **2**
    # * Substituir todos valores **4** por **3**
    # * Substituir todos valores **5** por **5**
    # * Substituir todos valores **6** por **4**
    # * Substituir todos valores **7** por **1**
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 1|Zona Azul / Parqímetro
    # 2|Estacionamento Avulso
    # 3|Estacionamento Mensal
    # 4|Estacionamento Próprio
    # 5|Meio-Fio / Logradouro
    # 6|Estacionamento Patrocinado
    # 7|Não estacionou
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não Respondeu
    # 1|Não Estacionou
    # 2|Estacionamento Particular (Avulso / Mensal)
    # 3|Estacionamento Próprio
    # 4|Estacionamento Patrocinado
    # 5|Rua (meio fio / zona azul / zona marrom / parquímetro)
    #
    # [Teste: Checar se existe algum número < 0 ou > 5. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_tipo_est_auto(passo, od1977)
    passo += 1

    # -----
    # ##"VALOR_EST_AUTO"
    # Nada há que se fazer em relação à coluna "VALOR_EST_AUTO".

    # -----
    # ##Passo 42: "SETOR_ATIV"
    # Substituir valores da coluna "SETOR_ATIV"
    #
    # Na coluna "SETOR_ATIV", linha i, ler o valor da linha i da coluna "SETOR_ATIV", daí, buscar o mesmo valor na coluna "COD" do arquivo setor_ativ-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "COD_UNIF"
    #
    # ####Categorias anteriores
    # > ver arquivo .csv
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não respondeu
    # 1|Agrícola
    # 2|Construção Civil
    # 3|Indústria
    # 4|Comércio
    # 5|Administração Pública
    # 6|Serviços de Transporte
    # 7|Serviços
    # 8|Serviços Autônomos
    # 9|Outros
    # 10|Não se aplica
    #
    # [Teste: Checar se existe algum número < 1 ou > 10. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_setor_ativ(passo, od1977, setor_ativ1977)
    passo += 1

    # -----
    # #Variáveis que consultam arquivos externos

    # -----
    # ##Passo 43: UCODs
    tipos_ucod = ['DOM', 'ESC', 'TRAB1', 'TRAB2', 'ORIG', 'DEST']

    for tipo in tipos_ucod:
        od1977 = passo_ucod(passo, od1977, ucod1977, tipo)
        passo += 1

    # -----
    # ##Passo 49: Coordenadas
    tipos_coord = ['DOM', 'ESC', 'TRAB1', 'TRAB2', 'ORIG', 'DEST']

    for tipo in tipos_coord:
        od1977 = passo_coord(passo, od1977, coord_subzona1977, tipo, 'X')
        passo += 1
        od1977 = passo_coord(passo, od1977, coord_subzona1977, tipo, 'Y')
        passo += 1

    # -----
    # #Variáveis que dependem de outras variáveis
    # -----
    # ##Passo 61: NO_DOM
    # Gerando "NO_DOM" como um subindíce de cada "ZONA_DOM"
    # Para cada "ZONA_DOM" o "NO_DOM" será atualizado sempre que "F_DOM" for igual a 1
    # Do contrário, se "F_DOM" for igual a zero, então "NO_DOM" será igual ao "NO_DOM" da linha anterior.
    od1977 = passo_no_dom(passo, od1977)
    passo += 1

    # -----
    # ##Passo 62: NO_FAM
    # Gerando "NO_FAM" como subíndice do "NO_DOM"
    # Para cada "NO_DOM" o "NO_FAM" será incrementado sempre que "F_FAM" for igual a 1
    # Do contrário, caso "F_FAM" seja igual a 0, então "NO_FAM" receberá o valor de "NO_FAM" da linha anterior.
    od1977 = passo_no_fam(passo, od1977)
    passo += 1

    # -----
    # ##Passo 63: NO_PESS
    # Gerando "NO_PESS" como subíndice do "NO_FAM"
    # Para cada "NO_FAM" o "NO_PESS" será incrementado sempre que "F_PESS" for igual a 1
    # Do contrário, caso "F_PESS" seja igual a 0, então "NO_PESS" receberá o valor de "NO_PESS" da linha anterior.
    od1977 = passo_no_pess(passo, od1977)
    passo += 1

    # -----
    # ##Passo 64: NO_VIAG
    # Gerando "NO_VIAG" como subíndice do "NO_PESS"
    # Para cada "NO_PESS" o "NO_VIAG" será incrementado sempre que "F_PESS" for igual a 1
    # Do contrário, caso "F_PESS" seja igual a 0, então "NO_VIAG" receberá o valor de "NO_VIAG" da linha anterior.
    od1977 = passo_no_viag(passo, od1977)
    passo += 1

    # -----
    # ##Passo 65: "ID_DOM"
    # Construir o "NO_DOM" e o "ID_DOM"
    # [Na coluna "ID_DOM", linha i, ler o valor da linha i da coluna "ZONA_DOM",
    # e concatenar esse valor (com 3 dígitos) com o número do domicílio,
    # que é o valor da linha i da coluna "NO_DOM" (com 4 dígitos).
    # Resultado será um ID_DOM, que pode se repetir nas linhas, de 7 dígitos.
    # Isso deve ser concatenado com o "Ano". Resultado = 8 dígitos]
    od1977 = passo_id_dom(passo, od1977)
    passo += 1

    # -----
    # ##Passo 66: "ID_FAM"
    # Construir "ID_FAM"
    # Na coluna "ID_FAM", linha i, ler o valor da linha i da coluna "ID_DOM",
    # e concatenar esse valor (com 8 dígitos) com o número da família,
    # que é o valor da linha i da coluna "NO_FAM" (com 2 dígitos).
    # Resultado será um ID_FAM, que pode se repetir nas linhas, de 10 dígitos.
    od1977 = passo_id_fam(passo, od1977)
    passo += 1

    # -----
    # ##Passo 67: "ID_PESS"
    # Construir "ID_PESS" e "NO_PESS"
    # Na coluna "ID_PESS", linha i, ler o valor da linha i da coluna "ID_FAM", e
    # concatenar esse valor (10 dígitos) com o número da pessoa,
    # que é o valor da linha i da coluna "NO_PESS" (com 2 dígitos).
    # Resultado será um ID_PESS, que pode se repetir nas linhas, de 12 dígitos.
    od1977 = passo_id_pess(passo, od1977)
    passo += 1

    # ----
    # ##Passo 68: "ID_VIAG"
    # Construir "ID_VIAG" e "NO_VIAG"
    # Na coluna "ID_VIAG", linha i, ler o valor da linha i da coluna "ID_PESS", e
    # concatenar esse valor (12 dígitos) com o número da pessoa,
    # que é o valor da linha i da coluna "NO_VIAG" (com 2 dígitos).
    # Resultado será um ID_VIAG, que pode se repetir nas linhas, 14 dígitos.
    od1977 = passo_id_viag(passo, od1977)
    passo += 1

    # ----
    # ##Passo 69: "ID_VIAG"
    # Calcula e confere o campo TOT_VIAG, baseado no maior valor de NO_VIAG para cada pessoa.
    od1977 = passo_tot_viag(passo, od1977)
    passo += 1

    # -----
    # ##Passo 70: "CD_ENTRE"
    # Substituir valores da coluna "CD_ENTRE"
    # Todas viagens são consideradas "completas", segundo informações do Metrô
    #
    # * sem viagem: se TOT_VIAG == 0
    # * com viagem: se TOT_VIAG != 0
    #
    # ####Categorias novas
    # | Valor | Descrição |
    # | -------- | -------- |
    # | 0 | Completa sem viagem |
    # | 1 | Completa com viagem |
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_cd_entre(passo, od1977)
    passo += 1

    # -----
    # ##Passo 71: "CD_RENFAM"
    # Substituir valores da coluna "CD_RENFAM"
    #
    # * Excluir único registro na categoria 3
    # * Substituir todos valores **1** por **2**
    # * Quando categoria original for 0 (respondeu) checar no campo REN_FAM se o valor é nulo. Se for nulo, manter na categoria 0 (Renda Familiar Declarada como Zero), senão, mover para a categoria 1 (Renda Familiar Declarada e Maior que Zero).
    #
    #
    # ####Categorias anteriores
    # Valor|Descrição
    # ----|----
    # 0|Respondeu
    # 1|Não Sabe
    # 2|Não Respondeu
    # 3|Não se Aplica
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Renda Familiar Declarada como Zero
    # 1|Renda Familiar Declarada e Maior que Zero
    # 2|Renda Atribuída
    #
    # [Teste: Checar se existe algum número < 0 ou > 2. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_cd_renfam(passo, od1977)
    passo += 1

    # -----
    # ##Passo 72: "ESTUDA"
    # Se zona da escola for zero (==0) então não estuda (0), senão, não estuda (1)
    #
    # ####Categorias novas
    # Valor|Descrição
    # ----|----
    # 0|Não estuda
    # 1|Estuda
    #
    # [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]
    od1977 = passo_estuda(passo, od1977)
    passo += 1

    # -----
    # ##Passo 73: "DIST_VIAG"
    # Calcula-se a distância euclidiana (a partir da CO_ORIG_X;CO_ORIG_Y e CO_DEST_X;CO_DEST_Y)
    od1977 = passo_dist_viag(passo, od1977)

    log_acompanhamento.info('Salvando dataframe como arquivo CSV')
    # -----
    # ## Salvando o dataframe num arquivo local
    od1977.to_csv('novo_od1977.csv', sep=';', decimal=',')

    log_acompanhamento.info("Terminou o main")


if __name__ == "__main__":
    main()