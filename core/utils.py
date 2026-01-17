import re

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