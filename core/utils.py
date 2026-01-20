import re
import time
import requests

# Coordenadas da SUA empresa (Origem Padrão para cálculo de rotas)
# IMPORTANTE: Altere para a Latitude/Longitude do seu Centro de Distribuição
ORIGEM_PADRAO = {'lat': -15.7975, 'lon': -47.8919} 

COORDS_UF = {
    'AC': (-8.77, -70.55), 'AL': (-9.62, -36.82), 'AM': (-3.65, -64.75), 'AP': (1.41, -51.77),
    'BA': (-12.96, -38.51), 'CE': (-3.71, -38.54), 'DF': (-15.78, -47.93), 'ES': (-20.31, -40.31),
    'GO': (-16.64, -49.31), 'MA': (-2.55, -44.30), 'MG': (-18.10, -44.38), 'MS': (-20.51, -54.54),
    'MT': (-12.64, -55.42), 'PA': (-5.53, -52.29), 'PB': (-7.06, -35.55), 'PE': (-8.28, -35.07),
    'PI': (-8.28, -43.68), 'PR': (-24.89, -51.55), 'RJ': (-22.84, -43.15), 'RN': (-5.22, -36.52),
    'RO': (-11.22, -62.80), 'RR': (1.99, -61.33), 'RS': (-30.01, -51.22), 'SC': (-27.33, -49.44),
    'SE': (-10.90, -37.07), 'SP': (-23.55, -46.64), 'TO': (-10.25, -48.25)
}

def get_regiao(uf):
    uf = str(uf).upper().strip()
    if uf in ['RS','SC','PR']: return 'Sul'
    if uf in ['SP','MG','RJ','ES']: return 'Sudeste'
    if uf in ['MT','MS','GO','DF']: return 'Centro-Oeste'
    if uf in ['AM','RR','AP','PA','TO','RO','AC']: return 'Norte'
    return 'Nordeste'

def limpar_cnpj(c):
    return ''.join(filter(str.isdigit, str(c)))

def xml_float(t):
    if not t: return 0.0
    return float(t.replace(",", "."))

def br_money(v):
    if v is None: v = 0.0
    # Formato R$ 1.500.000,00
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def br_weight(kg):
    if kg is None: kg = 0.0
    val = float(kg)
    if val < 1000:
        # Se for menor que 1000kg, mostra em kg (ex: 999 kg)
        return f"{val:,.0f} kg".replace(",", ".")
    else:
        # Se for maior, mostra em Toneladas (ex: 8.846 Tons)
        tons = val / 1000
        return f"{tons:,.3f} Tons".replace(",", "X").replace(".", ",").replace("X", ".")

def br_num(v):
    if v is None: v = 0
    # Formato 1.000 (inteiro) ou 1.000,00 (decimal)
    if isinstance(v, int):
        return f"{v:,.0f}".replace(",", ".")
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def get_lat_lon(endereco, bairro, cidade, uf, cep):
    """
    Busca coordenadas usando Nominatim (OpenStreetMap).
    Lógica especial para DF.
    """
    base_url = "https://nominatim.openstreetmap.org/search"
    headers = {'User-Agent': 'LeitorFiscalMaster/1.0'} # Obrigatório identificar o app
    
    # Lógica de Query
    query = ""
    
    if str(uf).upper() == 'DF':
        # No DF, cidade é "Brasília", focamos no Bairro ou CEP
        if bairro:
            query = f"{bairro}, Brasília, DF, Brazil"
        elif cep:
            query = f"{cep}, Brazil"
        else:
            query = "Brasília, DF, Brazil"
    else:
        # Outros estados: Bairro + Cidade + UF
        partes = []
        if bairro: partes.append(bairro)
        if cidade: partes.append(cidade)
        if uf: partes.append(uf)
        partes.append("Brazil")
        query = ", ".join(partes)

    params = {
        'q': query,
        'format': 'json',
        'limit': 1
    }

    try:
        # Nominatim pede 1 segundo de intervalo entre requests para não bloquear
        time.sleep(1) 
        response = requests.get(base_url, params=params, headers=headers, timeout=5)
        data = response.json()
        
        if data:
            return float(data[0]['lat']), float(data[0]['lon'])
    except Exception as e:
        print(f"Erro Geo Nominatim: {e}")
    
    return None, None

def get_distancia_osrm(lat_origem, lon_origem, lat_dest, lon_dest):
    """
    Calcula distância de carro em KM usando OSRM.
    Formato URL: /route/v1/driving/{lon},{lat};{lon},{lat}
    """
    if not lat_dest or not lon_dest:
        return 0.0
        
    # OSRM usa longitude,latitude
    url = f"http://router.project-osrm.org/route/v1/driving/{lon_origem},{lat_origem};{lon_dest},{lat_dest}?overview=false"
    
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        
        if data.get('code') == 'Ok':
            # Distância vem em metros
            metros = data['routes'][0]['distance']
            return metros / 1000.0 # Retorna em KM
    except Exception as e:
        print(f"Erro Rota OSRM: {e}")
        
    return 0.0

def extrair_peso_do_nome(nome):
    """
    Tenta extrair o peso total em KG baseado no nome do produto.
    Suporta: '25KG', '10.1KG', '6UN 3KG', '350G/12UN', etc.
    """
    nome = nome.upper().replace(',', '.') # Padroniza para maiúsculo e ponto decimal
    
    peso_final = 0.0
    
    # PADRÃO 1: Multiplicação com Barra (Ex: 350G/12UN ou 280G/24UN)
    # Procura algo como "XXXG / XXUN" ou "XXXG/XXUN"
    match_div = re.search(r'(\d+)\s*G\s*/\s*(\d+)\s*UN', nome)
    if match_div:
        gramas = float(match_div.group(1))
        unidades = float(match_div.group(2))
        peso_final = (gramas * unidades) / 1000 # Converte g para kg
        return round(peso_final, 4)

    # PADRÃO 2: Multiplicação Explícita (Ex: 06UN 3KG ou 6X3KG)
    match_mult_kg = re.search(r'(\d+)\s*(?:UN|CX|PC|X)\s*(\d+(?:\.\d+)?)\s*KG', nome)
    if match_mult_kg:
        qtd = float(match_mult_kg.group(1))
        peso_un = float(match_mult_kg.group(2))
        return round(qtd * peso_un, 4)

    # PADRÃO 3: Peso Simples em KG (Ex: 25KG, 10.1KG)
    # Pega o número logo antes de "KG"
    match_kg = re.search(r'(\d+(?:\.\d+)?)\s*KG', nome)
    if match_kg:
        return round(float(match_kg.group(1)), 4)

    # PADRÃO 4: Peso Simples em Gramas isolado (Ex: 500G)
    match_g = re.search(r'(\d+)\s*G(?!\w)', nome) # G no final ou seguido de espaço
    if match_g:
        return round(float(match_g.group(1)) / 1000, 4)

    return 0.0