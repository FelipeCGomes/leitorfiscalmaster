import re
import time
import requests
from unicodedata import normalize

# ==============================================================================
# CONFIGURAÇÕES GERAIS
# ==============================================================================

# Coordenadas do SEU Centro de Distribuição (Ponto A)
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

# ==============================================================================
# FUNÇÕES DE FORMATAÇÃO
# ==============================================================================

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
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def br_weight(kg):
    if kg is None: kg = 0.0
    val = float(kg)
    if val < 1000:
        return f"{val:,.0f} kg".replace(",", ".")
    else:
        tons = val / 1000
        return f"{tons:,.3f} Tons".replace(",", "X").replace(".", ",").replace("X", ".")

def br_num(v):
    if v is None: v = 0
    if isinstance(v, int):
        return f"{v:,.0f}".replace(",", ".")
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def limpar_texto_endereco(texto):
    if not texto: return ""
    texto = normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII').upper()
    texto = re.sub(r'[^\w\s,]', '', texto)
    padrao_corte = r'\b(SALA|LOJA|LJ|APTO|APT|BLOCO|BL|QD|LT|KM|RODOVIA|ROD|SETOR|Q\.)\b.*'
    texto = re.sub(padrao_corte, '', texto)
    texto = re.sub(r'\b(SNR|SN|NUMERO|NR|CASA|TERREO|FRENTE|FUNDOS)\b', '', texto)
    return texto.strip()

# ==============================================================================
# EXTRAÇÃO DE PESO
# ==============================================================================

def extrair_peso_do_nome(nome):
    nome = nome.upper().replace(',', '.')
    if "005/09FD" in nome: return 3.62
    match_div = re.search(r'(\d+)\s*G\s*/\s*(\d+)\s*UN', nome)
    if match_div:
        gramas = float(match_div.group(1))
        unidades = float(match_div.group(2))
        return round((gramas * unidades) / 1000, 4)
    match_mult_kg = re.search(r'(\d+)\s*(?:UN|CX|PC|X)\s*(\d+(?:\.\d+)?)\s*KG', nome)
    if match_mult_kg:
        return round(float(match_mult_kg.group(1)) * float(match_mult_kg.group(2)), 4)
    match_kg = re.search(r'(\d+(?:\.\d+)?)\s*KG', nome)
    if match_kg: return round(float(match_kg.group(1)), 4)
    match_g = re.search(r'(\d+)\s*G(?!\w)', nome)
    if match_g: return round(float(match_g.group(1)) / 1000, 4)
    return 0.0

# ==============================================================================
# GEOLOCALIZAÇÃO E ROTAS
# ==============================================================================

def get_lat_lon(endereco, bairro, cidade, uf, cep):
    base_url = "https://nominatim.openstreetmap.org/search"
    headers = {'User-Agent': 'LeitorFiscalMaster/4.0'}
    
    end_clean = limpar_texto_endereco(endereco)
    cidade_clean = limpar_texto_endereco(cidade)
    bairro_clean = limpar_texto_endereco(bairro)
    cep_clean = str(cep).replace('-', '').replace('.', '').strip()
    uf_upper = str(uf).upper().strip()
    
    queries = []
    if cep_clean and len(cep_clean) == 8: queries.append(f"{cep_clean}, Brazil")

    if uf_upper == 'DF':
        if bairro_clean: queries.append(f"{bairro_clean}, Brasília, DF, Brazil")
        if end_clean: queries.append(f"{end_clean}, Brasília, DF, Brazil")
    else:
        if end_clean and cidade_clean: queries.append(f"{end_clean}, {cidade_clean}, {uf}, Brazil")
        if end_clean:
            rua_sem_num = re.sub(r'\d+$', '', end_clean).strip()
            if rua_sem_num and len(rua_sem_num) > 3:
                queries.append(f"{rua_sem_num}, {cidade_clean}, {uf}, Brazil")

    if bairro_clean and cidade_clean: queries.append(f"{bairro_clean}, {cidade_clean}, {uf}, Brazil")
    if cidade_clean: queries.append(f"{cidade_clean}, {uf}, Brazil")

    for q in queries:
        if not q.strip(): continue
        try:
            time.sleep(1.1) 
            params = {'q': q, 'format': 'json', 'limit': 1}
            response = requests.get(base_url, params=params, headers=headers, timeout=4)
            if response.status_code == 200:
                data = response.json()
                # CORREÇÃO: Verifica se data[0] existe E se é um dicionário
                if data and isinstance(data, list) and isinstance(data[0], dict): 
                    return float(data[0].get('lat', 0)), float(data[0].get('lon', 0))
        except Exception as e:
            print(f"⚠️ Erro Query Geo: {e}")
            continue

    return COORDS_UF.get(uf, (None, None))

def get_distancia_osrm(lat_origem, lon_origem, lat_dest, lon_dest):
    if not lat_dest or not lon_dest or not lat_origem or not lon_origem:
        return 0.0
    
    endpoints = [
        "http://router.project-osrm.org/route/v1/driving/",
        "https://routing.openstreetmap.de/routed-car/route/v1/driving/"
    ]

    for base_url in endpoints:
        url = f"{base_url}{lon_origem},{lat_origem};{lon_dest},{lat_dest}?overview=false"
        try:
            response = requests.get(url, timeout=4) 
            if response.status_code == 200:
                data = response.json()
                # CORREÇÃO: Garante que 'routes' exista
                if data.get('code') == 'Ok' and data.get('routes') and len(data['routes']) > 0:
                    dist_km = data['routes'][0]['distance'] / 1000.0
                    return round(dist_km, 2)
        except Exception as e:
            print(f"⚠️ Falha na rota ({base_url}): {e}")
            continue

    print("❌ Falha total no cálculo de rota rodoviária.")
    return 0.0