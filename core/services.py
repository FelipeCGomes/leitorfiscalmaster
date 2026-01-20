import pandas as pd
from django.core.cache import cache
from .models import Nfe, Cte, Item, MemoriaIa, Cliente, ProdutoMap # <--- ProdutoMap incluído
from .config import CNPJS_CIA, TABELA_ANTT
from .utils import limpar_cnpj, get_regiao, COORDS_UF
from .utils import extrair_peso_do_nome # <--- Utils novo

def get_items_por_nf(chave_nf):
    qs = Item.objects.filter(chave_nf=chave_nf).values()
    return pd.DataFrame(qs)

def obter_peso_produto(nome_produto_xml):
    """
    1. Busca no banco de dados (ProdutoMap).
    2. Se não existir, tenta calcular via Regex.
    3. Salva no banco para o futuro.
    """
    nome_limpo = str(nome_produto_xml).strip()[:255]
    
    # 1. Tenta pegar do banco
    produto_map = ProdutoMap.objects.filter(nome_produto=nome_limpo).first()
    
    if produto_map:
        return produto_map.peso_unitario_kg
    
    # 2. Se não existe, tenta extrair
    peso_calculado = extrair_peso_do_nome(nome_limpo)
    
    # 3. Salva para o futuro (mesmo que seja 0)
    # Use get_or_create para evitar erro de duplicidade em concorrência
    ProdutoMap.objects.get_or_create(
        nome_produto=nome_limpo,
        defaults={'peso_unitario_kg': peso_calculado, 'manual': False}
    )
    
    return peso_calculado

def cadastrar_ou_atualizar_cliente(dados_header, buscar_geo=True):
    from .utils import get_lat_lon, get_distancia_osrm, ORIGEM_PADRAO
    
    cnpj = dados_header.get('cnpj_dest')
    if not cnpj: return

    # Tenta obter ou criar o cliente
    cliente, created = Cliente.objects.get_or_create(
        cpf_cnpj=cnpj,
        defaults={
            'nome': dados_header.get('destinatario')[:255],
            'razao_social': dados_header.get('destinatario')[:255],
            'cidade': dados_header.get('cidade_destino')[:100],
            'uf': dados_header.get('uf_dest')[:2],
            'endereco': dados_header.get('endereco')[:255],
            'bairro': dados_header.get('bairro')[:100],
            'cep': dados_header.get('cep')[:10]
        }
    )

    if not created:
        mudou = False
        if not cliente.bairro and dados_header.get('bairro'):
            cliente.bairro = dados_header.get('bairro'); mudou = True
        if not cliente.cep and dados_header.get('cep'):
            cliente.cep = dados_header.get('cep'); mudou = True
        if mudou: cliente.save()

    if not buscar_geo: return

    if cliente.latitude and cliente.longitude: return

    print(f"Buscando Geo para: {cliente.nome}...")
    lat, lon = get_lat_lon(cliente.endereco, cliente.bairro, cliente.cidade, cliente.uf, cliente.cep)

    if lat and lon:
        cliente.latitude = lat
        cliente.longitude = lon
        dist = get_distancia_osrm(ORIGEM_PADRAO['lat'], ORIGEM_PADRAO['lon'], lat, lon)
        cliente.distancia_km = dist
        cliente.save()

def get_dashboard_data():
    # 1. Tenta pegar do Cache
    cached_df = cache.get('dashboard_df')
    if cached_df is not None:
        return cached_df

    # 2. Carrega dados do Banco
    nf_qs = list(Nfe.objects.all().values())
    cte_qs = list(Cte.objects.all().values())
    
    # Carrega dados geográficos dos Clientes
    clientes_qs = list(Cliente.objects.values('cpf_cnpj', 'latitude', 'longitude', 'distancia_km'))
    df_clientes = pd.DataFrame(clientes_qs)

    df_n = pd.DataFrame(nf_qs)
    df_c = pd.DataFrame(cte_qs)

    if df_n.empty and df_c.empty: return pd.DataFrame()

    if df_n.empty:
        df_n = pd.DataFrame(columns=['chave_nf','valor_nf','peso_bruto'])
    
    # Garante colunas padrão na NF
    cols_n = ['chave_nf','numero_nf','destinatario','cnpj_dest','cnpj_emit','emitente','uf_dest','valor_nf','peso_bruto','data','mod_frete','tipo_operacao','cfop_predominante','cidade_origem','cidade_destino','transportadora','qtd_itens','cep_origem','cep_destino']
    for c in cols_n: 
        if c not in df_n.columns: df_n[c] = None
    
    # Conversões e Limpezas NF
    df_n['valor_nf'] = pd.to_numeric(df_n['valor_nf'], errors='coerce').fillna(0)
    df_n['peso_bruto'] = pd.to_numeric(df_n['peso_bruto'], errors='coerce').fillna(0)
    df_n['chave_nf'] = df_n['chave_nf'].astype(str).str.strip()
    df_n['cnpj_emit'] = df_n['cnpj_emit'].astype(str).str.strip()
    df_n['cnpj_dest'] = df_n['cnpj_dest'].astype(str).str.strip()

    # --- Cruzamento com Clientes ---
    if not df_clientes.empty:
        df_n = pd.merge(df_n, df_clientes, left_on='cnpj_dest', right_on='cpf_cnpj', how='left')
    else:
        df_n['latitude'] = None; df_n['longitude'] = None; df_n['distancia_km'] = 0

    # --- PROCESSAMENTO DE CT-E E RATEIO ---
    if not df_c.empty:
        df_c['chave_nf'] = df_c['chave_nf'].astype(str).str.strip()
        df_c['chave_cte_propria'] = df_c['chave_cte_propria'].astype(str).str.strip()
        df_c['frete_valor'] = pd.to_numeric(df_c['frete_valor'], errors='coerce').fillna(0)
        df_c['pedagio_valor'] = pd.to_numeric(df_c['pedagio_valor'], errors='coerce').fillna(0)
        df_c['peso_kg'] = pd.to_numeric(df_c['peso_kg'], errors='coerce').fillna(0)
        
        if 'tp_cte' not in df_c.columns: df_c['tp_cte'] = '0'
        df_c['tp_cte'] = df_c['tp_cte'].fillna('0').astype(str)

        pesos_nf = df_n[['chave_nf', 'peso_bruto']].rename(columns={'peso_bruto': 'peso_nf_ref'})
        df_c_calc = pd.merge(df_c, pesos_nf, on='chave_nf', how='left')
        df_c_calc['peso_nf_ref'] = df_c_calc['peso_nf_ref'].fillna(0)
        
        cte_totals = df_c_calc.groupby('chave_cte_propria').agg(
            total_peso_cte=('peso_nf_ref', 'sum'),
            qtd_notas=('chave_nf', 'count')
        ).reset_index()
        
        df_c_calc = pd.merge(df_c_calc, cte_totals, on='chave_cte_propria', how='left')
        
        def calcular_parcela(row):
            total_frete = float(row['frete_valor'])
            total_peso = float(row['total_peso_cte'])
            peso_indiv = float(row['peso_nf_ref'])
            qtd = row['qtd_notas']
            if total_frete == 0: return 0.0
            if total_peso > 0: return total_frete * (peso_indiv / total_peso)
            else: return total_frete / qtd if qtd > 0 else total_frete

        df_c_calc['frete_rateado'] = df_c_calc.apply(calcular_parcela, axis=1)
        df_c_calc['pedagio_rateado'] = df_c_calc['pedagio_valor']
        df_c_calc['is_complementar'] = df_c_calc['tp_cte'] == '1'
        
        # === CORREÇÃO AQUI: FILTRAR ORFÃOS ===
        # Removemos CT-es com chave vazia antes de agrupar para não criar a linha gigante
        mask_valida = (df_c_calc['chave_nf'].notnull()) & (df_c_calc['chave_nf'] != '')
        df_agg = df_c_calc[mask_valida]
        # =====================================

        nf_metrics = df_agg.groupby('chave_nf').agg({
            'frete_rateado': 'sum',
            'pedagio_rateado': 'sum',
            'peso_kg': 'sum',
            'emitente': 'first'
        }).reset_index().rename(columns={
            'frete_rateado': 'frete_valor',
            'pedagio_rateado': 'pedagio_valor',
            'peso_kg': 'peso_cte_total',
            'emitente': 'transportadora_cte'
        })

        def join_ctes(series): return ', '.join(sorted(list(set(series.astype(str)))))
        
        # Também usamos o df_agg filtrado aqui
        ctes_normais = df_agg[~df_agg['is_complementar']].groupby('chave_nf')['numero_cte'].apply(join_ctes).reset_index(name='numero_cte')
        ctes_comp = df_agg[df_agg['is_complementar']].groupby('chave_nf')['numero_cte'].apply(join_ctes).reset_index(name='cte_complementar')

        nf_costs = pd.merge(nf_metrics, ctes_normais, on='chave_nf', how='left')
        nf_costs = pd.merge(nf_costs, ctes_comp, on='chave_nf', how='left')
        
        df = pd.merge(df_n, nf_costs, on='chave_nf', how='left')
    else:
        df = df_n.copy()
        for col in ['frete_valor', 'pedagio_valor', 'peso_cte_total']: df[col] = 0.0
        df['transportadora_cte'] = None
        df['numero_cte'] = ''
        df['cte_complementar'] = ''

    # Preenchimentos Finais
    for col in ['numero_cte', 'cte_complementar']:
        if col not in df.columns: df[col] = ''
    df['numero_cte'] = df['numero_cte'].fillna('')
    df['cte_complementar'] = df['cte_complementar'].fillna('')
    
    for col in ['frete_valor', 'pedagio_valor', 'peso_cte_total']:
        df[col] = df[col].fillna(0.0)

    # Preenchimento de Lat/Lon e Distância
    if 'distancia_km' not in df.columns: df['distancia_km'] = 0.0
    df['distancia_km'] = pd.to_numeric(df['distancia_km'], errors='coerce').fillna(0.0)
    
    if 'latitude' in df.columns:
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    else:
        df['latitude'] = None; df['longitude'] = None

    df['Transportadora_Final'] = df['transportadora_cte'].fillna(df['transportadora']).fillna('---')
    
    df['Dt_Ref'] = pd.to_datetime(df['data'], errors='coerce')
    df['Ano'] = df['Dt_Ref'].dt.year.fillna(0).astype(int)
    df['Mes'] = df['Dt_Ref'].dt.month.fillna(0).astype(int)
    df['Dia'] = df['Dt_Ref'].dt.day.fillna(0).astype(int)
    
    def ext_uf(x): return str(x).split('-')[-1].strip() if '-' in str(x) else "ND"
    df['UF_Dest'] = df['cidade_destino'].apply(ext_uf)
    df['Regiao'] = df['UF_Dest'].apply(get_regiao)
    
    if 'mod_frete' in df.columns:
         df['Frete_Tipo'] = df['mod_frete'].apply(lambda x: 'CIF' if str(x)=='0' else ('FOB' if str(x)=='1' else 'Outros'))
    else: df['Frete_Tipo'] = 'Outros'

    # Operação e Legibilidade
    if isinstance(CNPJS_CIA, dict):
        MEUS_CNPJS = set([limpar_cnpj(str(c)) for c in CNPJS_CIA.keys()])
        DE_PARA_CNPJ = {limpar_cnpj(str(k)): v for k, v in CNPJS_CIA.items()}
    else:
        MEUS_CNPJS = set([limpar_cnpj(str(c)) for c in CNPJS_CIA])
        DE_PARA_CNPJ = {}

    def recalcular_operacao(row):
        emit = limpar_cnpj(str(row['cnpj_emit']))
        dest = limpar_cnpj(str(row['cnpj_dest']))
        eh_emitente = emit in MEUS_CNPJS
        eh_destinatario = dest in MEUS_CNPJS
        if eh_emitente and eh_destinatario: return "Transferência"
        elif eh_emitente and not eh_destinatario: return "Venda"
        elif not eh_emitente and eh_destinatario: return "Compra"
        else: return "Outros"

    df['Operacao'] = df.apply(recalcular_operacao, axis=1)

    def traduzir_empresa(row, col_nome, col_cnpj):
        cnpj_limpo = limpar_cnpj(str(row[col_cnpj]))
        if cnpj_limpo in DE_PARA_CNPJ: return DE_PARA_CNPJ[cnpj_limpo]
        return row[col_nome]

    df['Emitente_Legivel'] = df.apply(lambda x: traduzir_empresa(x, 'emitente', 'cnpj_emit'), axis=1)
    df['Destinatario_Legivel'] = df.apply(lambda x: traduzir_empresa(x, 'destinatario', 'cnpj_dest'), axis=1)
    
    cache.set('dashboard_df', df, 3600)
    return df