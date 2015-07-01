
# coding: utf-8

# In[273]:

from IPython.display import display #from IPython.core.display import HTML

import pandas as pd
pd.set_option('display.mpl_style', 'default') #Make the graphs a bit prettier

#Variable to avoid log prints when generating pdf file
impressao = False #True = to not print logs | False = to print logs


# --------
# ##Funções gerais

# In[274]:

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
    if row [name_col_filt]==0:
        return row[name_col_filt]
    return int(ext_data_frame[ext_data_frame[name_col_ref]==row[name_col_filt]][name_col_search])


# In[275]:

def verifica_DUMMY(data_frame, nome_variavel):
    """
    Verifica se uma variável, dummy, contém algum valor diferente de 0 ou de 1.
        Uso:
        verifica_DUMMY(nome_do_dataframe, 'coluna a ser verificada')
    """
    contador_de_erros = 0
    for index, value in data_frame.iterrows():
        if int(value[nome_variavel]) != 1 and int(value[nome_variavel]) != 0:
            if not impressao:
                print("Erro encontrado no registro " + str(index+1) + ".")
                print("    Valor encontrado: " + str(value[nome_variavel]))
            contador_de_erros += 1
    print("Total de erros encontrados: " + str(contador_de_erros))


# In[276]:

def verifica_RANGE(df, variavel, valor_menor, valor_maior):
    """
    Verifica se uma variável, do tipo número inteiro, contém algum valor menor que "valor_menor" ou maior que "valor_maior"
        Uso:
        verifica_RANGE(nome_do_dataframe, 'coluna a ser verificada', 'valor_menor', 'valor_maior')
    """
    df_filtrado = df[(df[variavel]<valor_menor) | (df[variavel]>valor_maior)]
    #Printing a summary of the values that not fit in the Range
    result = df_filtrado[variavel].value_counts()
    print(result)
    #If 'impressao = False', the output contains the values of dataframe that do not fit in the filter
    if not impressao:
        df_filtrado


# In[277]:

def gera_ID_DOM(row):
    """
    Gera o ID_DOM baseado no 'ANO', na 'ZONA_DOM' e no 'NO_DOM'
    O argumento passado é a "linha".
        Uso:
        od1977['ID_DOM'] = od1977.apply(gera_ID_DOM, axis=1)
    """
    ano = int(row['ANO'])
    zona = int(row['ZONA_DOM'])
    no_dom = int(row['NO_DOM'])
    return int(str(ano)+str('%03d'%(zona)) + str('%04d'%(no_dom)))


# In[278]:

def gera_ID_FAM(row):
    """
    Gera o ID_FAM baseado no 'ID_DOM' e no 'NO_FAM'
    O argumento passado é a "linha".
        Uso:
        od1977['ID_FAM'] = od1977.apply(gera_ID_FAM, axis=1)
    """
    id_dom = int(row['ID_DOM'])
    no_fam = int(row['NO_FAM'])
    return int(str(id_dom) + str('%02d'%(no_fam)))


# In[279]:

def gera_ID_PESS(row):
    """
    Gera o ID_PESS baseado no 'ID_FAM' e no 'NO_PESS'
    O argumento passado é a "linha".
        Uso:
        od1977['ID_PESS'] = od1977.apply(gera_ID_PESS, axis=1)
    """
    id_fam = int(row['ID_FAM'])
    no_pess = int(row['NO_PESS'])
    return int(str(id_fam) + str('%02d'%(no_pess)))


# In[280]:

def gera_ID_VIAG(row):
    """
    Gera o ID_VIAG baseado no 'ID_PESS' e no 'NO_VIAG'
    O argumento passado é a "linha".
        Uso:
        od1977['ID_VIAG'] = od1977.apply(gera_ID_VIAG, axis=1)
    """
    id_pess = int(row['ID_PESS'])
    no_viag = int(row['NO_VIAG'])
    return int(str(id_pess) + str('%02d'%(no_viag)))


# In[281]:

def calcula_DIST_VIAG(row):
    """
    Calcula a distância euclidiana dadas as coordenadas (x,y) de origem e coordenadas (x,y) de destino da viagem.
    O argumento passado é a "linha".
        Uso:
        od1977['DIST_VIAG'] = od1977.apply(calcula_DIST_VIAG, axis=1)
    """
    co_orig_x = float(row['CO_ORIG_X'])
    co_orig_y = float(row['CO_ORIG_Y'])
    co_dest_x = float(row['CO_DEST_X'])
    co_dest_y = float(row['CO_DEST_Y'])
    return math.sqrt(math.pow((co_orig_x - co_dest_x), 2) + math.pow((co_orig_y - co_dest_y), 2))


# ----

# In[282]:

#Reading csv file and store its contend in an intern dataframe
od1977 = pd.read_csv('OD_1977.csv', sep=';', decimal=',')


# In[283]:

#Reading other accessory files that will be used on the consulta_refext function
ucod1977 = pd.read_csv('UCOD-1977.csv',sep=';')
setor_ativ1977 = pd.read_csv('setor_ativ-1977.csv',sep=';')
coord_subzona1977 = pd.read_csv('coord_subzonas_1977.csv',sep=';')


# In[284]:

#Renaming the column UCOD to UCOD_DOM
od1977.rename(columns={'UCOD':'UCOD_DOM'}, inplace=True)


# In[285]:

#Creating the column UCOD_ESC (it will go to the end of dataframe)
od1977['UCOD_ESC']=None


# In[286]:

#Creating the column UCOD_TRAB1 (it will go to the end of dataframe)
od1977['UCOD_TRAB1']=None


# In[287]:

#Creating the column UCOD_TRAB2 (it will go to the end of dataframe)
od1977['UCOD_TRAB2']=None


# In[288]:

#Creating the column UCOD_ORIG (it will go to the end of dataframe)
od1977['UCOD_ORIG']=None


# In[289]:

#Creating the column UCOD_DEST (it will go to the end of dataframe)
od1977['UCOD_DEST']=None


# In[290]:

#od1977 = od1977[:15000]


# In[291]:

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


# In[292]:

# Storing the variables list in the "cols" variable
cols = od1977.columns.tolist()
if not impressao:
    # Printing "cols" variable to check if the reorder operation was effective
    display(cols)


# In[293]:

if not impressao:
    # Describing data (whole dataframe)- count, mean, std, min and max
    display(od1977.describe())


# -----
# ##Passo 1: UCOD_DOM
# Na coluna "UCOD_DOM", linha i, ler o valor da linha i da coluna "ZONA_DOM", daí, buscar o mesmo valor na coluna "Zona 1977" do arquivo UCOD-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "UCOD_DOM"
# 
# [Teste: no banco completo, checar se o min == 1 e o max == 67]

# In[294]:

# Getting from the csv file the "UCOD" code correspondent to the "ZONA_DOM" code
od1977['UCOD_DOM'] = od1977.apply(lambda row: consulta_refext(row, ucod1977, 'Zona 1977', 'ZONA_DOM', 'UCOD'), axis=1)


# In[295]:

if not impressao:
    # Describing data ("UCOD_DOM" column) - count, mean, std, min and max
    display(od1977['UCOD_DOM'].describe())


# In[296]:

if not impressao:
    # Count for check "UCOD_DOM"
    display(od1977['UCOD_DOM'].value_counts())


# In[297]:

# Verifying value interval for check - conditions: "UCOD_DOM < 1" and "UCOD_DOM > 67"
verifica_RANGE(od1977, 'UCOD_DOM', 1, 67)


# -----
# ##Passo 2: "ANO"
# Preencher a coluna "ANO" com  valor 1 em todas células
# ####Categorias:
# |valor|ano_correspondente|
# |-------|-----|
# |1|1977|
# |2|1987|
# |3|1997|
# |4|2007|

# In[298]:

# Assigning value '1' to all cels of the "ANO" column
od1977["ANO"]=1


# In[299]:

if not impressao:
    # Describing data ("ANO" column) - count, mean, std, min and max
    display(od1977['ANO'].describe())


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

# In[300]:

if not impressao:
    # Counting for check "CD_ENTRE"
    display(od1977['CD_ENTRE'].value_counts())


# In[301]:

# Defining 'CD_ENTRE' based on 'TOT_VIAG' values
od1977[od1977['TOT_VIAG']==0]['CD_ENTRE']=0
od1977[od1977['TOT_VIAG']!=0]['CD_ENTRE']=1


# In[302]:

if not impressao:
    # Counting "CD_ENTRE" in order to compare the values before and after the replacement
    display(od1977['CD_ENTRE'].value_counts())


# In[303]:

# Verifying if there was left some value other than 0 or 1
verifica_DUMMY(od1977, 'CD_ENTRE')


# ----
# ##Passo 4: "DIA_SEM"
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

# In[304]:

# Assigning value '0' to all cels of the "DIA_SEM" column
od1977['DIA_SEM']= None


# In[305]:

if not impressao:
    # Counting "DIA_SEM" in order to check the values after the procedure
    display(od1977['DIA_SEM'].value_counts())


# -----
# ##Passo 5: "ZONA_DOM"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 243
# 
# [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]

# In[306]:

# Verifying value interval for check - conditions: "ZONA_DOM < 1" and "ZONA_DOM > 243"
# od1977[(od1977['ZONA_DOM']<1) | (od1977['ZONA_DOM']>243)]
verifica_RANGE(od1977, 'ZONA_DOM', 1, 243)


# -----
# ##Passo 6: "SUBZONA_DOM"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 633
# 
# [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]

# In[307]:

# Verifying value interval for check - conditions: "SUBZONA_DOM < 1" and "SUBZONA_DOM > 633"
# od1977[(od1977['SUBZONA_DOM']<1) | (od1977['SUBZONA_DOM']>633)]
verifica_RANGE(od1977, 'SUBZONA_DOM', 1, 633)


# -----
# ##Passo 7: "MUN_DOM"
# Checar se existe algum erro
# 
# ####Categorias
# > 1 a 27
# 
# [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]

# In[308]:

# Verifying value interval for check - conditions: "MUN_DOM < 1" and "MUN_DOM > 27"
# od1977[(od1977['MUN_DOM']<1) | (od1977['MUN_DOM']>27)]
verifica_RANGE(od1977, 'MUN_DOM', 1, 27)


# -----
# ##Passo 8: "CO_DOM_X"
# 
# Na coluna "CO_DOM_X", linha i, ler o valor da linha i da coluna "SUBZONA_DOM", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_X"
# 

# In[309]:

#Getting from the csv file the "CO_X" code correspondent to the "SUBZONA_DOM" code
od1977['CO_DOM_X'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_DOM', 'CO_X'), axis=1)


# -----
# ##Passo 9: "CO_DOM_Y"
# 
# Na coluna "CO_DOM_Y", linha i, ler o valor da linha i da coluna "SUBZONA_DOM", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_Y"

# In[310]:

#Getting from the csv file the "CO_Y" code correspondent to the "SUBZONA_DOM" code
od1977['CO_DOM_Y'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_DOM', 'CO_Y'), axis=1)


# -----
# ##Passo 10: "ID_DOM"
# 
# Construir o "NO_DOM" e o "ID_DOM"
# 
# [Na coluna "ID_DOM", linha i, ler o valor da linha i da coluna "ZONA_DOM", e concatenar esse valor (com 3 dígitos) com o número do domicílio , que é o valor da linha i da coluna "NO_DOM" (com 4 dígitos). Resultado será um ID_DOM, que pode se repetir nas linhas, de 7 dígitos. Isso deve ser concatenado com o "Ano". Resultado = 8 dígitos]

# In[311]:

# Generating "NO_DOM" as a subindex of each "ZONA_DOM"
# For each "ZONA_DOM" the "NO_DOM" will be updated every time the "F_DOM" is equal to 1.
# Otherwise - if "F_DOM" is equal to 0; then the "NO_DOM" will be equal to the "NO_DOM" from the previous row.

def gera_NO_DOM(row):
    # Use this function with:
    #     dataframe.apply(gera_NO_DOM, axis=1)
    #
    # Return 1 if the "NO_DOM" was applied and 0 if it was not.
    # row.name is the index of the specific row.
    
    if row.name == 0 or od1977.loc[row.name, 'ZONA_DOM'] != od1977.loc[row.name - 1, 'ZONA_DOM']:
        # If first row of dataframe, then NO_DOM = 1. or
        # If first row of a ZONA_DOM, then NO_DOM = 1 also.
        # This considers that the dataframe is (also) ordered by ZONA_DOM.
        # It is a strong requirement.
        
        od1977.loc[row.name, 'NO_DOM'] = 1
    elif row['F_DOM'] == 1:
        od1977.loc[row.name, 'NO_DOM'] = od1977.loc[row.name - 1, 'NO_DOM'] + 1
    elif row['F_DOM'] == 0:
        od1977.loc[row.name, 'NO_DOM'] = od1977.loc[row.name - 1, 'NO_DOM']
    else:
        print("Erro na composição da linha" + str(row.name))
        return 0 # If the function break, it will return 0
    return 1 # If the function works out, it will return 1.


# In[312]:

# The fucntion gera_NO_DOM is called and due to the fact it returns 1 if well suceeded,
# it is possible to sum and verify errors existence.

od1977.apply(gera_NO_DOM, axis=1).sum()


# In[313]:

# Generating "ID_DOM" from the concatenation of "ANO", "ZONA_DOM" and "NO_DOM" variables
od1977['ID_DOM'] = od1977.apply(gera_ID_DOM, axis=1)


# -----
# ##Passo 11: "F_DOM"
# Checar se existe algum erro na coluna "F_DOM"
# 
# ####Categorias
# Valor|Descrição
# ----|----
# 0|Demais registros
# 1|Primeiro Registro do Domicílio
# 
# [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]

# In[314]:

# Verifying if there was left some value other than 0 or 1
verifica_DUMMY(od1977, 'F_DOM')


# -----
# ##"FE_DOM"
# Nada há que se fazer em relação aos dados da coluna "FE_DOM"

# -----
# ##Passo 12: "TIPO_DOM"
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

# In[315]:

if not impressao:
    # Counting for check "TIPO_DOM"
    display(od1977['TIPO_DOM'].value_counts())


# In[316]:

# Verifying value interval for check - conditions: "TIPO_DOM < 0" and "TIPO_DOM > 2"
# od1977[(od1977['TIPO_DOM']<0) | (od1977['TIPO_DOM']>2)]
verifica_RANGE(od1977, 'TIPO_DOM', 0, 2)


# -----
# ##"TOT_FAM"
# Nada há que se fazer em relação aos dados da coluna "TOT_FAM"

# -----
# ##Passo 13: "ID_FAM"
# Construir "NO_FAM" e "ID_FAM"
# 
# Na coluna "ID_FAM", linha i, ler o valor da linha i da coluna "ID_DOM", e concatenar esse valor (com 8 dígitos) com o número da família, que é o valor da linha i da coluna "NO_FAM" (com 2 dígitos).
# 
# Resultado será um ID_FAM, que pode se repetir nas linhas, de 10 dígitos.

# In[317]:

# Generating "NO_FAM" as a subindex of each "NO_DOM"
# For each "NO_DOM" the "NO_FAM" will be updated every time the "F_FAM" is equal to 1.
# Otherwise - if "F_FAM" is equal to 0; then the "NO_FAM" will be equal to the "NO_FAM" from the previous row.

def gera_NO_FAM(row):
    # Use this function with:
    #     dataframe.apply(gera_NO_FAM, axis=1)
    #
    # Return 1 if the "NO_FAM" was applied and 0 if it was not.
    # row.name is the index of the specific row.
    
    if row.name == 0 or od1977.loc[row.name, 'NO_DOM'] != od1977.loc[row.name - 1, 'NO_DOM']:
        # If first row of dataframe, then NO_FAM = 1. or
        # If first row of a NO_DOM, then NO_FAM = 1 also.
        # This considers that the dataframe is (also) ordered by NO_DOM.
        # It is a strong requirement.
        
        od1977.loc[row.name, 'NO_FAM'] = 1
    elif row['F_FAM'] == 1:
        od1977.loc[row.name, 'NO_FAM'] = od1977.loc[row.name - 1, 'NO_FAM'] + 1
    elif row['F_FAM'] == 0:
        od1977.loc[row.name, 'NO_FAM'] = od1977.loc[row.name - 1, 'NO_FAM']
    else:
        print("Erro na composição da linha" + str(row.name))
        return 0
    return 1


# In[318]:

# The fucntion gera_NO_FAM is called and due to the fact it returns 1 if well suceeded,
# it is possible to sum and verify errors existence.

od1977.apply(gera_NO_FAM, axis=1).sum()


# In[319]:

#Generating "ID_FAM" from the concatenation of "ID_DOM" and "NO_FAM" variables
od1977['ID_FAM'] = od1977.apply(gera_ID_FAM, axis=1)


# -----
# ##Passo 14: "F_FAM"
# Checar se existe algum erro na coluna "F_FAM"
# 
# ####Categorias
# Valor|Descrição
# ----|----
# 0|Demais registros
# 1|Primeiro Registro da Família
# 
# [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]

# In[320]:

#Verifying if there was left some value other than 0 or 1
verifica_DUMMY(od1977, 'F_FAM')


# -----
# ##"FE_FAM"
# Nada há que se fazer em relação aos dados da coluna "FE_FAM"

# -----
# ##Passo 15: "COND_MORA"
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

# In[321]:

if not impressao:
    # Counting for check "COND_MORA"
    display(od1977['COND_MORA'].value_counts())


# In[322]:

# Replacing the values 1 for 2
od1977.loc[od1977['COND_MORA']==1,'COND_MORA'] = 2
# Replacing the values 3 for 1
od1977.loc[od1977['COND_MORA']==3,'COND_MORA'] = 1
# Replacing the values 4 for 3
od1977.loc[od1977['COND_MORA']==4,'COND_MORA'] = 3
# Replacing the values 5 for 3
od1977.loc[od1977['COND_MORA']==5,'COND_MORA'] = 3
# Replacing the values 0 for 4
od1977.loc[od1977['COND_MORA']==0,'COND_MORA'] = 4


# In[323]:

if not impressao:
    # Counting "COND_MORA" in order to compare the values before and after the replacement
    display(od1977['COND_MORA'].value_counts())


# In[324]:

# Verifying value interval for check - conditions: "COND_MORA < 1" and "COND_MORA > 4"
# od1977[(od1977['COND_MORA']<1) | (od1977['COND_MORA']>4)]
verifica_RANGE(od1977, 'COND_MORA', 1, 4)


# -----
# ##"QT_AUTO"
# Nada há que se fazer em relação aos dados da coluna "QT_AUTO"

# -----
# ##"QT_BICI" e QT_MOTO"
# Não existe essa informação no banco de dados de 1977, logo, estes campos serão preenchidos com 'None'.

# In[325]:

# Assigning value 'None' to all cels of the "QT_BICI" column
od1977['QT_BICI'] = None


# In[326]:

if not impressao:
    # Counting "QT_BICI" in order to check the values after the procedure
    display(od1977['QT_BICI'].isnull().sum())


# In[327]:

# Assigning value 'None' to all cels of the "QT_BICI" column
od1977['QT_MOTO'] = None


# In[328]:

if not impressao:
    # Counting "QT_MOTO" in order to check the values after the procedure
    display(od1977['QT_MOTO'].isnull().sum())


# -----
# ##Passo 16: "CD_RENFAM"
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

# In[329]:

if not impressao:
    # Counting for check "CD_RENFAM"
    display(od1977['CD_RENFAM'].value_counts())


# In[330]:

# Replacing the values 1 for 2
od1977.loc[od1977['CD_RENFAM']==1,'CD_RENFAM'] = 2


# In[331]:

#Spliting the category 0 "Respondeu" into 0 "Renda Familiar Declarada como Zero" and 1 "Renda Familiar Declarada e Maior que Zero"
#####


# In[332]:

if not impressao:
    # Counting "CD_RENFAM" in order to compare the values before and after the replacement
    display(od1977['CD_RENFAM'].value_counts())


# In[333]:

# Verifying value interval for check - conditions: "CD_RENFAM < 0" and "CD_RENFAM > 2"
# od1977[(od1977['CD_RENFAM']<0) | (od1977['CD_RENFAM']>2)]
verifica_RANGE(od1977, 'CD_RENFAM', 0, 2)


# -----
# ##"REN_FAM"
# Nada há que se fazer em relação aos dados da colunas "REN_FAM"

# -----
# ##Passo 17: "ID_PESS"
# Construir "ID_PESS" e "NO_PESS"
# 
# Na coluna "ID_PESS", linha i, ler o valor da linha i da coluna "ID_FAM", e concatenar esse valor (10 dígitos) com o número da pessoa, que é o valor da linha i da coluna "NO_PESS" (com 2 dígitos).
# 
# Resultado será um ID_PESS, que pode se repetir nas linhas, de 12 dígitos.

# In[334]:

# Generating "NO_PESS" as a subindex of each "NO_FAM"
# For each "NO_FAM" the "NO_PESS" will be updated every time the "F_PESS" is equal to 1.
# Otherwise - if "F_PESS" is equal to 0; then the "NO_PESS" will be equal to the "NO_PESS" from the previous row.

def gera_NO_PESS(row):
    # Use this function with:
    #     dataframe.apply(gera_NO_PESS, axis=1)
    #
    # Return 1 if the "NO_PESS" was applied and 0 if it was not.
    # row.name is the index of the specific row.
    
    if row.name == 0 or od1977.loc[row.name, 'NO_FAM'] != od1977.loc[row.name - 1, 'NO_FAM']:
        # If first row of dataframe, then NO_PESS = 1. or
        # If first row of a NO_FAM, then NO_PESS = 1 also.
        # This considers that the dataframe is (also) ordered by NO_FAM.
        # It is a strong requirement.
        
        od1977.loc[row.name, 'NO_PESS'] = 1
    elif row['F_PESS'] == 1:
        od1977.loc[row.name, 'NO_PESS'] = od1977.loc[row.name - 1, 'NO_PESS'] + 1
    elif row['F_PESS'] == 0:
        od1977.loc[row.name, 'NO_PESS'] = od1977.loc[row.name - 1, 'NO_PESS']
    else:
        print("Erro na composição da linha" + str(row.name))
        return 0
    return 1


# In[335]:

# The fucntion gera_NO_PESS is called and due to the fact it returns 1 if well suceeded,
# it is possible to sum and verify errors existence.

od1977.apply(gera_NO_PESS, axis=1).sum()


# In[336]:

#Generating "ID_PESS" from the concatenation of "ID_FAM" and "NO_PESS" variables
od1977['ID_PESS'] = od1977.apply(gera_ID_PESS, axis=1)


# -----
# ##Passo 18: "F_PESS"
# Checar se existe algum erro na coluna "F_PESS"
# 
# ####Categorias
# Valor|Descrição
# ----|----
# 0|Demais registros
# 1|Primeiro Registro da Pessoa
# 
# [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]

# In[337]:

#Verifying if there was left some value other than 0 or 1
verifica_DUMMY(od1977, 'F_PESS')


# -----
# ##"FE_PESS"
# Nada há que se fazer em relação aos dados das colunas "FE_PESS"

# -----
# ##Passo 19: "SIT_FAM"
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

# In[338]:

if not impressao:
    # Counting for check "SIT_FAM"
    display(od1977['SIT_FAM'].value_counts())


# In[339]:

# Replacing the values 5 for 4
od1977.loc[od1977['SIT_FAM']==5,'SIT_FAM'] = 4
# Replacing the values 6 for 5
od1977.loc[od1977['SIT_FAM']==6,'SIT_FAM'] = 5
# Replacing the values 7 for 6
od1977.loc[od1977['SIT_FAM']==7,'SIT_FAM'] = 6


# In[340]:

if not impressao:
    # Counting "CD_RENFAM" in order to compare the values before and after the replacement
    display(od1977['SIT_FAM'].value_counts())


# In[341]:

# Verifying value interval for check - conditions: "SIT_FAM < 1" and "SIT_FAM > 6"
# od1977[(od1977['SIT_FAM']<1) | (od1977['SIT_FAM']>6)]
verifica_RANGE(od1977, 'SIT_FAM', 1, 6)


# -----
# ##"IDADE"
# Nada há que se fazer em relação aos dados da coluna "IDADE"

# -----
# ##Passo 20: "SEXO"
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

# In[342]:

if not impressao:
    # Counting for check "SEXO"
    display(od1977['SEXO'].value_counts())


# In[343]:

# Replacing the values 2 for 0
od1977.loc[od1977['SEXO']==2,'SEXO'] = 0


# In[344]:

if not impressao:
    # Counting "SEXO" in order to compare the values before and after the replacement
    display(od1977['SEXO'].value_counts())


# In[345]:

# Verifying if there was left some value other than 0 or 1
verifica_DUMMY(od1977, 'SEXO')


# -----
# ##Passo 21: "ESTUDA"
# >>> CRIAR!
#  
#  Se zona da escola for zero (==0) então não estuda (0), senão, não estuda (1)
# 
# ####Categorias novas
# Valor|Descrição
# ----|----
# 0|Não estuda
# 1|Estuda
# 
# [Teste: Checar se existe algum número diferente de 0 ou 1. Se encontrar, retornar erro indicando em qual linha.]

# In[346]:

if not impressao:
    #Counting for check "ESTUDA"
    display(od1977['ESTUDA'].value_counts())


# In[347]:

# Replacing the values 2 for 0
### implementar rotina do estuda


# In[348]:

if not impressao:
    # Counting "ESTUDA" in order to compare the values before and after the replacement
    display(od1977['ESTUDA'].value_counts())


# In[349]:

# Verifying if there was left some value other than 0 or 1
verifica_DUMMY(od1977, 'ESTUDA')


# -----
# ##Passo 22: "GRAU_INSTR"
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

# In[350]:

if not impressao:
    # Counting for check "GRAU_INSTR"
    display(od1977['GRAU_INSTR'].value_counts())


# In[351]:

# Replacing the values 2 for 1
od1977.loc[od1977['GRAU_INSTR']==2,'GRAU_INSTR'] = 1
# Replacing the values 3 for 1
od1977.loc[od1977['GRAU_INSTR']==3,'GRAU_INSTR'] = 1
# Replacing the values 4 for 1
od1977.loc[od1977['GRAU_INSTR']==4,'GRAU_INSTR'] = 1
# Replacing the values 5 for 2
od1977.loc[od1977['GRAU_INSTR']==5,'GRAU_INSTR'] = 2
# Replacing the values 6 for 2
od1977.loc[od1977['GRAU_INSTR']==6,'GRAU_INSTR'] = 2
# Replacing the values 7 for 3
od1977.loc[od1977['GRAU_INSTR']==7,'GRAU_INSTR'] = 3
# Replacing the values 8 for 3
od1977.loc[od1977['GRAU_INSTR']==8,'GRAU_INSTR'] = 3
# Replacing the values 9 for 4
od1977.loc[od1977['GRAU_INSTR']==9,'GRAU_INSTR'] = 4


# In[352]:

if not impressao:
    # Counting "GRAU_INSTR" in order to compare the values before and after the replacement
    display(od1977['GRAU_INSTR'].value_counts())


# In[353]:

# Verifying value interval for check - conditions: "GRAU_INSTR < 1" and "GRAU_INSTR > 4"
# od1977[(od1977['GRAU_INSTR']<1) | (od1977['GRAU_INSTR']>4)]
verifica_RANGE(od1977, 'GRAU_INSTR', 1, 4)


# -----
# ##Passo 23: "OCUP"
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

# In[354]:

if not impressao:
    # Counting for check "OCUP"
    display(od1977['OCUP'].value_counts())


# In[355]:

# Replacing the values 1 for 7
od1977.loc[od1977['OCUP']==1,'OCUP'] = 7
# Replacing the values 2 for 9
od1977.loc[od1977['OCUP']==2,'OCUP'] = 9
# Replacing the values 4 for 8
od1977.loc[od1977['OCUP']==4,'OCUP'] = 8
# Replacing the values 5 for 4
od1977.loc[od1977['OCUP']==5,'OCUP'] = 4
# Replacing the values 8 for 5
od1977.loc[od1977['OCUP']==8,'OCUP'] = 5
# Replacing the values 6 for 2
od1977.loc[od1977['OCUP']==6,'OCUP'] = 2
# Replacing the values 9 for 6
od1977.loc[od1977['OCUP']==9,'OCUP'] = 6
# Replacing the values > 6 for 7
od1977.loc[od1977['OCUP']>7,'OCUP'] = 1


# In[356]:

if not impressao:
    # Counting "OCUP" in order to compare the values before and after the replacement
    display(od1977['OCUP'].value_counts())


# In[357]:

# Verifying value interval for check - conditions: "OCUP < 1" and "OCUP > 7"
# od1977[(od1977['OCUP']<1) | (od1977['OCUP']>7)]
verifica_RANGE(od1977, 'OCUP', 1, 7)


# -----
# ##Passo 24: "SETOR_ATIV"
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

# In[358]:

if not impressao:
    # Counting for check "SETOR_ATIV"
    display(od1977['SETOR_ATIV'].value_counts())


# In[359]:

# Getting from the csv file the "CD_UNIF" (unified code for activity sector) correspondent to the "SETOR_ATIV" code
od1977['SETOR_ATIV'] = od1977.apply(lambda row: consulta_refext(row, setor_ativ1977, 'COD', 'SETOR_ATIV', 'COD_UNIF'), axis=1)


# In[360]:

if not impressao:
    # Counting "SETOR_ATIV" in order to compare the values before and after the replacement
    display(od1977['SETOR_ATIV'].value_counts())


# In[361]:

# Verifying value interval for check - conditions: "SETOR_ATIV < 0" and "SETOR_ATIV > 10"
# od1977[(od1977['SETOR_ATIV']<0) | (od1977['SETOR_ATIV']>10)]
verifica_RANGE(od1977, 'SETOR_ATIV', 0, 10)


# ##Passo 25: "CD_RENIND"
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

# In[362]:

if not impressao:
    # Counting for check "CD_RENIND"
    display(od1977['CD_RENIND'].value_counts())


# In[363]:

# Verifying value interval for check - conditions: "CD_RENIND < 1" and "CD_RENIND > 3"
# od1977[(od1977['CD_RENIND']<1) | (od1977['CD_RENIND']>3)]
verifica_RANGE(od1977, 'CD_RENIND', 1, 3)


# -----
# ##"REN_IND"
# Nada há que se fazer em relação aos dados da coluna "REN_IND"

# -----
# ##Passo 26: "UCOD_ESC"
# Na coluna "UCOD_ESC", linha i, ler o valor da linha i da coluna "ZONA_ESC", daí, buscar o mesmo valor na coluna "Zona 1977" do arquivo UCOD-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "UCOD_ESC"
# 
# [Teste: no banco completo, checar se o min == 1 e o max == 67]

# In[364]:

# Getting from the csv file the "UCOD" code correspondent to the "ZONA_ESC" code
od1977['UCOD_ESC'] = od1977.apply(lambda row: consulta_refext(row, ucod1977, 'Zona 1977', 'ZONA_ESC', 'UCOD'), axis=1)


# In[ ]:

if not impressao:
    # Describing data ("UCOD_ESC" column) - count, mean, std, min and max
    display(od1977['UCOD_ESC'].describe())


# In[ ]:

if not impressao:
    # Count for check "UCOD_ESC"
    display(od1977['UCOD_ESC'].value_counts())


# In[ ]:

# Verifying value interval for check - conditions: "UCOD_ESC < 1" and "UCOD_ESC > 67"
# The 'error' returns must be related to "UCOD_ESC" == 0, that is, trips that are not school purposed
# od1977[(od1977['UCOD_ESC']<1) | (od1977['UCOD_ESC']>67)]
verifica_RANGE(od1977, 'UCOD_ESC', 1, 67)


# -----
# ##Passo 27: "ZONA_ESC"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 243
# 
# [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

# Verifying value interval for check - conditions: "ZONA_ESC < 1" and "ZONA_ESC > 243"
# The 'error' returns must be related to "ZONA_ESC" == 0, that is, trips that are not school purposed
# od1977[(od1977['ZONA_ESC']<1) | (od1977['ZONA_ESC']>254)]
verifica_RANGE(od1977, 'ZONA_ESC', 1, 243)


# -----
# ##Passo 28: "SUBZONA_ESC"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 633
# 
# [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

# Verifying value interval for check - conditions: "SUBZONA_ESC < 1" and "SUBZONA_ESC > 633"
# od1977[(od1977['SUBZONA_ESC']<1) | (od1977['SUBZONA_ESC']>633)]
verifica_RANGE(od1977, 'SUBZONA_ESC', 1, 633)


# -----
# ##Passo 29: "MUN_ESC"
# Checar se existe algum erro
# 
# ####Categorias
# > 1 a 27
# 
# [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

# Verifying value interval for check - conditions: "MUN_ESC < 1" and "MUN_ESC > 27"
# The 'error' returns must be related to "MUN_ESC" == 0, that is, trips that are not school purposed
# od1977[(od1977['MUN_ESC']<1) | (od1977['MUN_ESC']>27)]
verifica_RANGE(od1977, 'MUN_ESC', 1, 27)


# 
# -----
# ##Passo 30: "CO_ESC_X"
# 
# Na coluna "CO_ESC_X", linha i, ler o valor da linha i da coluna "SUBZONA_ESC", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_X"

# In[ ]:

# Getting from the csv file the "CO_X" code correspondent to the "SUBZONA_ESC" code
od1977['CO_ESC_X'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_ESC', 'CO_X'), axis=1)


# -----
# ##Passo 31: "CO_ESC_Y"
# 
# Na coluna "CO_ESC_Y", linha i, ler o valor da linha i da coluna "SUBZONA_ESC", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_Y"
# 

# In[ ]:

# Getting from the csv file the "CO_Y" code correspondent to the "SUBZONA_ESC" code
od1977['CO_ESC_Y'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_ESC', 'CO_Y'), axis=1)


# -----
# ##Passo 32: "UCOD_TRAB1"
# Na coluna "UCOD_TRAB1", linha i, ler o valor da linha i da coluna "ZONA_TRAB1", daí, buscar o mesmo valor na coluna "Zona 1977" do arquivo UCOD-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "UCOD_TRAB1"
# 
# [Teste: no banco completo, checar se o min == 1 e o max == 67]

# In[ ]:

# Getting from the csv file the "UCOD" code correspondent to the "ZONA_TRAB1" code
od1977['UCOD_TRAB1'] = od1977.apply(lambda row: consulta_refext(row, ucod1977, 'Zona 1977', 'ZONA_TRAB1', 'UCOD'), axis=1)


# In[ ]:

if not impressao:
    # Describing data ("UCOD_TRAB1" column) - count, mean, std, min and max
    display(od1977['UCOD_TRAB1'].describe())


# In[ ]:

if not impressao:
    # Count for check "UCOD_TRAB1"
    display(od1977['UCOD_TRAB1'].value_counts())


# In[ ]:

# Verifying value interval for check - conditions: "UCOD_TRAB1 < 1" and "UCOD_TRAB1 > 67"
# The 'error' returns must be related to "UCOD_TRAB1" == 0, that is, trips that are not work purposed
# od1977[(od1977['UCOD_TRAB1']<1) | (od1977['UCOD_TRAB1']>67)]
verifica_RANGE(od1977, 'UCOD_TRAB1', 1, 67)


# 
# -----
# ##Passo 33: "ZONA_TRAB1"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 243
# 
# [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]
# 

# In[ ]:

# Verifying value interval for check - conditions: "ZONA_TRAB1 < 1" and "ZONA_TRAB1 > 243"
# The 'error' returns must be related to "ZONA_TRAB1"==0, that is, trips that are not work purposed
# od1977[(od1977['ZONA_TRAB1']<1) | (od1977['ZONA_TRAB1']>243)]
verifica_RANGE(od1977, 'ZONA_TRAB1', 1, 243)


# -----
# ##Passo 34: "SUBZONA_TRAB1"
# 
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 633
# 
# [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

# Verifying value interval for check - conditions: "SUBZONA_TRAB1 < 1" and "SUBZONA_TRAB1 > 633"
# od1977[(od1977['SUBZONA_TRAB1']<1) | (od1977['SUBZONA_TRAB1']>633)]
verifica_RANGE(od1977, 'SUBZONA_TRAB1', 1, 633)


# -----
# ##Passo 35: "MUN_TRAB1"
# Checar se existe algum erro
# 
# ####Categorias
# > 1 a 27
# 
# [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

# Verifying value interval for check - conditions: "MUN_TRAB1 < 1" ou de "MUN_TRAB1 > 27"
# The 'error' returns must be related to "MUN_TRAB1" == 0, that is, trips that are not work purposed
# od1977[(od1977['MUN_TRAB1']<1) | (od1977['MUN_TRAB1']>27)]
verifica_RANGE(od1977, 'MUN_TRAB1', 1, 27)


# -----
# ##Passo 36: "CO_TRAB1_X"
# 
# Na coluna "CO_TRAB1_X", linha i, ler o valor da linha i da coluna "SUBZONA_TRAB1", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_X"

# In[ ]:

# Getting from the csv file the "CO_X" code correspondent to the "SUBZONA_TRAB1" code
od1977['CO_TRAB1_X'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_TRAB1', 'CO_X'), axis=1)


# -----
# ##Passo 37: "CO_TRAB1_Y"
# 
# Na coluna "CO_TRAB1_Y", linha i, ler o valor da linha i da coluna "SUBZONA_TRAB1", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_Y"

# In[ ]:

# Getting from the csv file the "CO_Y" code correspondent to the "SUBZONA_TRAB1" code
od1977['CO_TRAB1_Y'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_TRAB1', 'CO_Y'), axis=1)


# -----
# ##Passo 38: "UCOD_TRAB2"
# Na coluna "UCOD_TRAB2", linha i, ler o valor da linha i da coluna "ZONA_TRAB2", daí, buscar o mesmo valor na coluna "Zona 1977" do arquivo UCOD-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "UCOD_TRAB2"
# 
# [Teste: no banco completo, checar se o min == 1 e o max == 67]

# In[ ]:

# Getting from the csv file the "UCOD" code correspondent to the "ZONA_TRAB2" code
od1977['UCOD_TRAB2'] = od1977.apply(lambda row: consulta_refext(row, ucod1977, 'Zona 1977', 'ZONA_TRAB2', 'UCOD'), axis=1)


# In[ ]:

if not impressao:
    # Describing data ("UCOD_TRAB2" column) - count, mean, std, min and max
    display(od1977['UCOD_TRAB2'].describe())


# In[ ]:

if not impressao:
    # Count for check "UCOD_TRAB2"
    display(od1977['UCOD_TRAB2'].value_counts())


# In[ ]:

# Verifying value interval for check - conditions: "UCOD_TRAB2 < 1" and "UCOD_TRAB2 > 67"
# The 'error' returns must be related to "UCOD_TRAB2" == 0, that is, trips that are not work purposed
# od1977[(od1977['UCOD_TRAB2']<1) | (od1977['UCOD_TRAB2']>67)]
verifica_RANGE(od1977, 'UCOD_TRAB2', 1, 67)


# -----
# ##Passo 39: "ZONA_TRAB2"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 243
# 
# [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

# Verifying value interval for check - conditions: "ZONA_TRAB2 < 1" and "ZONA_TRAB2 > 243
# The 'error' returns must be related to "ZONA_TRAB2"==0, that is, trips that are not work purposed
# od1977[(od1977['ZONA_TRAB2']<1) | (od1977['ZONA_TRAB2']>243)]
verifica_RANGE(od1977, 'ZONA_TRAB2', 1, 243)


# -----
# ##Passo 40: "SUBZONA_TRAB2"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 633
# 
# [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

# Verifying value interval for check - conditions: "SUBZONA_TRAB2 < 1" and "SUBZONA_TRAB2 > 633"
# od1977[(od1977['SUBZONA_TRAB2']<1) | (od1977['SUBZONA_TRAB2']>633)]
verifica_RANGE(od1977, 'SUBZONA_TRAB2', 1, 633)


# -----
# ##Passo 41: "MUN_TRAB2"
# Checar se existe algum erro
# 
# ####Categorias
# > 1 a 27
# 
# [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

# Verifying value interval for check - conditions: "MUN_TRAB2 < 1" ou de "MUN_TRAB2 > 27"
# The 'error' returns must be related to "MUN_TRAB2" == 0, that is, trips that are not work purposed
# od1977[(od1977['MUN_TRAB2']<1) | (od1977['MUN_TRAB2']>27)]
verifica_RANGE(od1977, 'MUN_TRAB2', 1, 27)


# -----
# ##Passo 42: "CO_TRAB2_X"
# 
# Na coluna "CO_TRAB2_X", linha i, ler o valor da linha i da coluna "SUBZONA_TRAB2", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_X"
# 

# In[ ]:

# Getting from the csv file the "CO_X" code correspondent to the "SUBZONA_TRAB2" code
od1977['CO_TRAB2_X'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_TRAB2', 'CO_X'), axis=1)


# -----
# ##Passo 43: "CO_TRAB2_Y"
# 
# Na coluna "CO_TRAB2_Y", linha i, ler o valor da linha i da coluna "SUBZONA_TRAB2", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_Y"

# In[ ]:

# Getting from the csv file the "CO_Y" code correspondent to the "SUBZONA_TRAB2" code
od1977['CO_TRAB2_Y'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_TRAB2', 'CO_Y'), axis=1)


# ----
# ##Passo 44: "ID_VIAG"
# Construir "ID_VIAG" e "NO_VIAG"
# 
# Na coluna "ID_VIAG", linha i, ler o valor da linha i da coluna "ID_PESS", e concatenar esse valor (12 dígitos) com o número da pessoa, que é o valor da linha i da coluna "NO_VIAG" (com 2 dígitos).
# 
# Resultado será um ID_VIAG, que pode se repetir nas linhas, 14 dígitos.

# In[ ]:

# Generating "NO_VIAG" as a subindex of each "NO_PESS"
# For each "NO_PESS" the "NO_VIAG" will be updated every time the "F_PESS" is equal to 1.
# Otherwise - if "F_PESS" is equal to 0; then the "NO_VIAG" will be equal to the "NO_VIAG" from the previous row.

def gera_NO_VIAG(row):
    # Use this function with:
    #     dataframe.apply(gera_NO_VIAG, axis=1)
    #
    # Return 1 if the "NO_VIAG" was applied and 0 if it was not.
    # row.name is the index of the specific row.
    
    if row.name == 0 or od1977.loc[row.name, 'NO_PESS'] != od1977.loc[row.name - 1, 'NO_PESS']:
        # If first row of dataframe, then NO_VIAG = 1. or
        # If first row of a NO_PESS, then NO_VIAG = 1 also.
        # This considers that the dataframe is (also) ordered by NO_PESS.
        # It is a strong requirement.
        
        od1977.loc[row.name, 'NO_VIAG'] = 1
    elif row['F_PESS'] == 1:
        od1977.loc[row.name, 'NO_VIAG'] = od1977.loc[row.name - 1, 'NO_VIAG'] + 1
    elif row['F_PESS'] == 0:
        od1977.loc[row.name, 'NO_VIAG'] = od1977.loc[row.name - 1, 'NO_VIAG']
    else:
        print("Erro na composição da linha" + str(row.name))
        return 0
    if row['FE_VIAG'] == 0:
        od1977.loc[row.name, 'NO_VIAG'] = 0
    return 1


# In[ ]:

# The fucntion gera_NO_VIAG is called and due to the fact it returns 1 if well suceeded,
# it is possible to sum and verify errors existence.

od1977.apply(gera_NO_VIAG, axis=1).sum()


# In[ ]:

# Generating "ID_VIAG" from the concatenation of "ID_PESS" and "NO_VIAG" variables
od1977['ID_VIAG'] = od1977.apply(gera_ID_VIAG, axis=1)


# -----
# ##Passo 45: "F_VIAG"
# Excluir a coluna "F_VIAG", porque as viagens são numeradas, então já se saber pelo NO_VIAG qual é a primeira do indivíduo.

# In[ ]:

od1977 = od1977.drop('F_VIAG', 1)


# In[ ]:

# Storing the variables list in the "cols" variable
cols = od1977.columns.tolist()
if not impressao:
    # Printing "cols" variable to check if the reorder operation was effective
    display(cols)


# -----
# ##"FE_VIAG"
# Nada há que se fazer em relação aos dados da coluna "FE_VIAG"

# -----
# ##"TOT_VIAG"
# 
# >>> CALCULAR

# In[ ]:

if not impressao:
    # Describing data ("TOT_VIAG" column) - count, mean, std, min and max
    display(od1977['TOT_VIAG'].describe())
display(od1977[:30][['ID_PESS','NO_VIAG','TOT_VIAG']])


# In[ ]:

def atrib_tot_viag(row):
    od1977.loc[od1977['ID_PESS']==row['ID_PESS'],'TOT_VIAG'] = row['NO_VIAG']
    # print('id_pessoa: ' + str(row['ID_PESS']) + ' | no_viag:' + str(row['NO_VIAG'])
    # print(row)

od1977.loc[:,['ID_PESS','NO_VIAG']].groupby(['ID_PESS'],sort=False).agg({'NO_VIAG':max,'ID_PESS':max}).apply(atrib_tot_viag, axis=1)
# od1977[od1977['ID_PESS']==20002030404][['ID_PESS','NO_VIAG']]


# In[ ]:

display(od1977.loc[:90,['ID_PESS','NO_VIAG','TOT_VIAG']])
# Verificar as viagens do ID_PESS = 100100070403
# display(od1977.loc[od1977['ID_PESS']==100100070403,['ID_PESS','NO_VIAG','TOT_VIAG','ID_DOM','NO_DOM','ID_VIAG']])


# In[ ]:

if not impressao:
    # Count for check "TOT_VIAG"
    display(od1977['TOT_VIAG'].value_counts())


# In[ ]:

# Agora uma função que irá verificar se para todo "ID_PESS" o "TOT_VIAG" é igual ao 'NO_VIAG' máximo.
def verifica_no_viag_tot_viag(row):
    if row['NO_VIAG'] != row['TOT_VIAG']:
        print(row)
od1977.loc[:,['ID_PESS','NO_VIAG','TOT_VIAG']].groupby('ID_PESS').agg({'NO_VIAG':'max','ID_PESS':'max','TOT_VIAG':'max'}).apply(verifica_no_viag_tot_viag, axis=1)


# In[ ]:

if not impressao:
    # Describing data ("TOT_VIAG" column) - count, mean, std, min and max
    display(od1977['TOT_VIAG'].describe())


# -----
# ##Passo 46: "UCOD_ORIG"
# Na coluna "UCOD_ORIG", linha i, ler o valor da linha i da coluna "ZONA_ORIG", daí, buscar o mesmo valor na coluna "Zona 1977" do arquivo UCOD-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "UCOD_ORIG"
# 
# [Teste: no banco completo, checar se o min == 1 e o max == 67]

# In[119]:

#Getting from the csv file the "UCOD" code correspondent to the "ZONA_ORIG" code
od1977['UCOD_ORIG'] = od1977.apply(lambda row: consulta_refext(row, ucod1977, 'Zona 1977', 'ZONA_ORIG', 'UCOD'), axis=1)


# In[ ]:

if not impressao:
    #Describing data ("UCOD_ORIG" column) - count, mean, std, min and max
    display(od1977['UCOD_ORIG'].describe())


# In[ ]:

if not impressao:
    #Count for check "UCOD_ORIG"
    display(od1977['UCOD_ORIG'].value_counts())


# In[ ]:

#Verifying value interval for check - conditions: "UCOD_ORIG < 1" and "UCOD_ORIG > 67"
#The 'error' returns must be related to "UCOD_ORIG" == 0, that is, trips that were not made
#od1977[(od1977['UCOD_ORIG']<1) | (od1977['UCOD_ORIG']>67)]
verifica_RANGE(od1977, 'UCOD_ORIG', 1, 67)


# -----
# ##Passo 47: "ZONA_ORIG"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 243
# 
# [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

#Verifying value interval for check - conditions: "ZONA_ORIG < 1" and "ZONA_ORIG > 243"
#The 'error' returns must be related to "ZONA_ORIG"==0, that is, trips that were not made
#od1977[(od1977['ZONA_ORIG']<1) | (od1977['ZONA_ORIG']>243)]
verifica_RANGE(od1977, 'ZONA_ORIG', 1, 243)


# -----
# ##Passo 48: "SUBZONA_ORIG"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 633
# 
# [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

#Verifying value interval for check - conditions: "SUBZONA_ORIG < 1" and "SUBZONA_ORIG > 633"
#od1977[(od1977['SUBZONA_ORIG']<1) | (od1977['SUBZONA_ORIG']>633)]
verifica_RANGE(od1977, 'SUBZONA_ORIG', 1, 633)


# -----
# ##Passo 49: "MUN_ORIG"
# Checar se existe algum erro
# 
# ####Categorias
# > 1 a 27
# 
# [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

#Verifying value interval for check - conditions: "MUN_ORIG < 1" ou de "MUN_ORIG > 27"
#The 'error' returns must be related to "MUN_ORIG" == 0, that is, trips that were not made
#od1977[(od1977['MUN_ORIG']<1) | (od1977['MUN_ORIG']>27)]
verifica_RANGE(od1977, 'MUN_ORIG', 1, 27)


# -----
# ##Passo 50: "CO_ORIG_X"
# 
# Na coluna "CO_ORIG_X", linha i, ler o valor da linha i da coluna "SUBZONA_ORIG", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_X"

# In[ ]:

#Getting from the csv file the "CO_X" code correspondent to the "SUBZONA_ORIG" code
od1977['CO_ORIG_X'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_ORIG', 'CO_X'), axis=1)


# -----
# ##Passo 51: "CO_ORIG_Y"
# 
# a coluna "CO_ORIG_Y", linha i, ler o valor da linha i da coluna "SUBZONA_ORIG", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_Y"

# In[ ]:

#Getting from the csv file the "CO_Y" code correspondent to the "SUBZONA_ORIG" code
od1977['CO_ORIG_Y'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_ORIG', 'CO_Y'), axis=1)


# -----
# ##Passo 52: "UCOD_DEST"
# Na coluna "UCOD_DEST", linha i, ler o valor da linha i da coluna "ZONA_DEST", daí, buscar o mesmo valor na coluna "Zona 1977" do arquivo UCOD-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "UCOD_DEST"
# 
# [Teste: no banco completo, checar se o min == 1 e o max == 67]

# In[ ]:

#Getting from the csv file the "UCOD" code correspondent to the "ZONA_DEST" code
od1977['UCOD_DEST'] = od1977.apply(lambda row: consulta_refext(row, ucod1977, 'Zona 1977', 'ZONA_DEST', 'UCOD'), axis=1)


# In[ ]:

if not impressao:
    #Describing data ("UCOD_DEST" column) - count, mean, std, min and max
    display(od1977['UCOD_DEST'].describe())


# In[ ]:

if not impressao:
    #Count for check "UCOD_DEST"
    display(od1977['UCOD_DEST'].value_counts())


# In[ ]:

#Verifying value interval for check - conditions: "UCOD_DEST < 1" and "UCOD_DEST > 67"
#The 'error' returns must be related to "UCOD_DEST" == 0, that is, trips that were not made
#od1977[(od1977['UCOD_DEST']<1) | (od1977['UCOD_DEST']>67)]
verifica_RANGE(od1977, 'UCOD_DEST', 1, 67)


# -----
# ##Passo 53: "ZONA_DEST"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 243
# 
# [Teste: Checar se existe algum número < 1 ou > 243. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

#Verifying value interval for check - conditions: "ZONA_DEST < 1" and "ZONA_DEST > 243"
#The 'error' returns must be related to "ZONA_DEST"==0, that is, trips that are not school purposed
#od1977[(od1977['ZONA_DEST']<1) | (od1977['ZONA_DEST']>243)]
verifica_RANGE(od1977, 'ZONA_DEST', 1, 243)


# -----
# ##Passo 54: "SUBZONA_DEST"
# Checar se existe algum erro
# 
# ####Categorias:
# > 1 a 633
# 
# [Teste: Checar se existe algum número < 1 ou > 633. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

#Verifying value interval for check - conditions: "SUBZONA_DEST < 1" and "SUBZONA_DEST > 633"
#od1977[(od1977['SUBZONA_DEST']<1) | (od1977['SUBZONA_DEST']>633)]
verifica_RANGE(od1977, 'SUBZONA_DEST', 1, 633)


# -----
# ##Passo 55: "MUN_DEST"
# Checar se existe algum erro
# 
# ####Categorias
# > 1 a 27
# 
# [Teste: Checar se existe algum número < 1 ou > 27. Se encontrar, retornar erro indicando em qual linha.]

# In[ ]:

#Verifying value interval for check - conditions: "MUN_DEST < 1" ou de "MUN_DEST > 27"
#The 'error' returns must be related to "MUN_DEST" == 0, that is, trips that were not made
#od1977[(od1977['MUN_DEST']<1) | (od1977['MUN_DEST']>27)]
verifica_RANGE(od1977, 'MUN_DEST', 1, 27)


# -----
# ##Passo 56: "CO_DEST_X"
# 
# Na coluna "CO_DEST_X", linha i, ler o valor da linha i da coluna "SUBZONA_DEST", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_X"

# In[ ]:

#Getting from the csv file the "CO_X" code correspondent to the "SUBZONA_DEST" code
od1977['CO_DEST_X'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_DEST', 'CO_X'), axis=1)


# -----
# ##Passo 57: "CO_DEST_Y"
# 
# Na coluna "CO_DEST_Y", linha i, ler o valor da linha i da coluna "SUBZONA_DEST", daí, buscar o mesmo valor na coluna "SUBZONA" do arquivo COORD-SUBZONA-1977.csv. Ao achar, retornar o valor da mesma linha, só que da coluna "CO_Y"

# In[ ]:

#Getting from the csv file the "CO_Y" code correspondent to the "SUBZONA_DEST" code
od1977['CO_DEST_Y'] = od1977.apply(lambda row: consulta_refext(row, coord_subzona1977, 'SUBZONA', 'SUBZONA_DEST', 'CO_Y'), axis=1)


# -----
# ##Passo 58: "DIST_VIAG"
# 
# Calcula-se a distância euclidiana (a partir da CO_ORIG_X;CO_ORIG_Y e CO_DEST_X;CO_DEST_Y)
# 
# >> Falta um teste de verificação!!!

# In[ ]:

#Calculating "DIST_VIAG" (euclidian distance) from the origin cordinates (CO_ORIG_X;CO_ORIG_Y)and destinations cordinates (CO_DEST_X;CO_DEST_Y)
od1977['DIST_VIAG'] = od1977.apply(lambda row: calcula_DIST_VIAG(row), axis=1)


# -----
# ##Passo 59: "MOTIVO_ORIG"
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

# In[ ]:

if not impressao:
    #Counting "MOTIVO_ORIG" in order to compare the values before and after the replacement
    display(od1977['MOTIVO_ORIG'].value_counts())


# In[ ]:

#Replacing the values 6 for 11
od1977.loc[od1977['MOTIVO_ORIG']==6,'MOTIVO_ORIG'] = 11
#Replacing the values 7 for 6
od1977.loc[od1977['MOTIVO_ORIG']==7,'MOTIVO_ORIG'] = 6
#Replacing the values 8 for 7
od1977.loc[od1977['MOTIVO_ORIG']==8,'MOTIVO_ORIG'] = 7
#Replacing the values 10 for 8
od1977.loc[od1977['MOTIVO_ORIG']==10,'MOTIVO_ORIG'] = 8
#Replacing the values 11 for 9
od1977.loc[od1977['MOTIVO_ORIG']==11,'MOTIVO_ORIG'] = 9


# In[ ]:

if not impressao:
    #Counting "MOTIVO_ORIG" in order to compare the values before and after the replacement
    display(od1977['MOTIVO_ORIG'].value_counts())


# In[ ]:

#Verifying value interval for check - conditions: "MOTIVO_ORIG < 0" and "MOTIVO_ORIG > 9"
#od1977[(od1977['MOTIVO_ORIG']<0) | (od1977['MOTIVO_ORIG']>9)]
verifica_RANGE(od1977, 'MOTIVO_ORIG', 0, 9)


# -----
# ##Passo 60: "MOTIVO_DEST"
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

# In[ ]:

if not impressao:
    #Counting "MOTIVO_DEST in order to compare the values before and after the replacement
    display(od1977['MOTIVO_DEST'].value_counts())


# In[ ]:

#Replacing the values 6 for 11
od1977.loc[od1977['MOTIVO_DEST']==6,'MOTIVO_DEST'] = 11
#Replacing the values 7 for 6
od1977.loc[od1977['MOTIVO_DEST']==7,'MOTIVO_DEST'] = 6
#Replacing the values 8 for 7
od1977.loc[od1977['MOTIVO_DEST']==8,'MOTIVO_DEST'] = 7
#Replacing the values 10 for 8
od1977.loc[od1977['MOTIVO_DEST']==10,'MOTIVO_DEST'] = 8
#Replacing the values 11 for 9
od1977.loc[od1977['MOTIVO_DEST']==11,'MOTIVO_DEST'] = 9


# In[ ]:

if not impressao:
    #Counting "MOTIVO_DEST" in order to compare the values before and after the replacement
    display(od1977['MOTIVO_DEST'].value_counts())


# In[ ]:

#Verifying value interval for check - conditions: "MOTIVO_DEST < 0" and "MOTIVO_DEST > 9"
#od1977[(od1977['MOTIVO_DEST']<0) | (od1977['MOTIVO_DEST']>9)]
verifica_RANGE(od1977, 'MOTIVO_DEST', 0, 9)


# -----
# ##Passo 61: "MODO1"
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

# In[ ]:

if not impressao:
    #Counting for check "MODO1"
    display(od1977['MODO1'].value_counts())


# In[ ]:

#Verifying value interval for check - conditions: "MODO1 < 0" and "MODO1 > 12"
#od1977[(od1977['MODO1']<0) | (od21977['MODO1']>12)]
verifica_RANGE(od1977, 'MODO1', 0, 12)


# ##Passo 62: "MODO2"
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

# In[ ]:

if not impressao:
    #Counting for check "MODO2"
    display(od1977['MODO2'].value_counts())


# In[ ]:

#Verifying value interval for check - conditions: "MODO2 < 0" and "MODO2 > 12"
#od1977[(od1977['MODO2']<0) | (od1977['MODO2']>12)]
verifica_RANGE(od1977, 'MODO2', 0, 12)


# ##Passo 63: "MODO3"
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

# In[ ]:

if not impressao:
    #Counting for check "MODO3"
    display(od1977['MODO3'].value_counts())


# In[ ]:

#Verifying value interval for check - conditions: "MODO3 < 0" and "MODO3 > 12"
#od1977[(od1977['MODO3']<0) | (od1977['MODO3']>12)]
verifica_RANGE(od1977, 'MODO3', 0, 12)


# ##Passo 64: "MODO4"
# Nada há que se fazer em relação à coluna "TIPO_EST_AUTO" - não há dados de 1977, coluna permanecerá vazia

# In[ ]:

if not impressao:
    #Counting for check "MODO4"
    display(od1977['MODO4'].value_counts())


# ##Passo 65: "MODO_PRIN"
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

# In[ ]:

if not impressao:
    #Counting for check "MODO_PRIN"
    display(od1977['MODO_PRIN'].value_counts())


# In[ ]:

#Verifying value interval for check - conditions: "MODO_PRIN < 0" and "MODO_PRIN > 12"
#od1977[(od1977['MODO_PRIN']<0) | (od1977['MODO_PRIN']>12)]
verifica_RANGE(od1977, 'MODO_PRIN', 0, 12)


# -----
# ##"TIPO_VIAG"; "H_SAIDA"; "MIN_SAIDA"; "ANDA_ORIG"; "H_CHEG"; "MIN_CHEG"; "ANDA_DEST" e "DURACAO"
# Nada há que se fazer em relação aos dados das colunas "TIPO_VIAG"; "H_SAIDA"; "MIN_SAIDA"; "ANDA_ORIG"; "H_CHEG"; "MIN_CHEG"; "ANDA_DEST" e "DURACAO"

# ##"TIPO_EST_AUTO"
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

# In[ ]:

if not impressao:
    #Counting for check "TIPO_EST_AUTO"
    display(od1977['TIPO_EST_AUTO'].value_counts())


# In[ ]:

#Replacing the values 1 for 5
od1977.loc[od1977['TIPO_EST_AUTO']==1,'TIPO_EST_AUTO'] = 5
#Replacing the values 3 for 2
od1977.loc[od1977['TIPO_EST_AUTO']==3,'TIPO_EST_AUTO'] = 2
#Replacing the values 4 for 3
od1977.loc[od1977['TIPO_EST_AUTO']==4,'TIPO_EST_AUTO'] = 3
#Replacing the values 6 for 4
od1977.loc[od1977['TIPO_EST_AUTO']==6,'TIPO_EST_AUTO'] = 4
#Replacing the values 7 for 1
od1977.loc[od1977['TIPO_EST_AUTO']==7,'TIPO_EST_AUTO'] = 1


# In[ ]:

if not impressao:
    #Counting "TIPO_EST_AUTO in order to compare the values before and after the replacement
    display(od1977['TIPO_EST_AUTO'].value_counts())


# In[ ]:

#Verifying value interval for check - conditions: "TIPO_EST_AUTO < 0" and "TIPO_EST_AUTO > 5"
#od1977[(od1977['TIPO_EST_AUTO']<0) | (od1977['TIPO_EST_AUTO']>5)]
verifica_RANGE(od1977, 'TIPO_EST_AUTO', 0, 5)


# -----
# ##"VALOR_EST_AUTO"
# Nada há que se fazer em relação à coluna "VALOR_EST_AUTO".

# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



