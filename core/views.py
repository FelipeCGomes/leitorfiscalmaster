from django.contrib.auth.decorators import login_required
from django.db import close_old_connections
from django.shortcuts import render
from .models import Nfe, Cte, Item, Log
from . import parsers, services, utils
import pandas as pd
import zipfile
from datetime import datetime
import plotly.express as px

# ==============================================================================
# 1. DASHBOARD
# ==============================================================================
@login_required
def dashboard(request):
    df = services.get_dashboard_data()
    context = {}
    
    if df.empty:
        context['no_data'] = True
        return render(request, 'core/dashboard.html', context)

    # --- PRÉ-PROCESSAMENTO ---
    df['dt_obj'] = pd.to_datetime(df['data'], errors='coerce')
    df['Dia'] = df['dt_obj'].dt.day.fillna(0).astype(int)
    df['Mes'] = df['dt_obj'].dt.month.fillna(0).astype(int)
    df['Ano'] = df['dt_obj'].dt.year.fillna(0).astype(int)

    # --- CAPTURA DE FILTROS ---
    sel_ano = request.GET.getlist('ano')
    sel_mes = request.GET.getlist('mes')
    sel_dia = request.GET.getlist('dia')
    sel_filial = request.GET.getlist('filial')
    sel_cliente = request.GET.getlist('cliente')
    sel_transp = request.GET.getlist('transp')
    sel_mod = request.GET.getlist('mod_frete')
    sel_tipo = request.GET.getlist('tipo_op')

    # --- APLICAÇÃO DOS FILTROS ---
    df_filtered = df.copy()

    if sel_ano: df_filtered = df_filtered[df_filtered['Ano'].astype(str).isin(sel_ano)]
    if sel_mes: df_filtered = df_filtered[df_filtered['Mes'].astype(str).isin(sel_mes)]
    if sel_dia: df_filtered = df_filtered[df_filtered['Dia'].astype(str).isin(sel_dia)]
    if sel_filial: df_filtered = df_filtered[df_filtered['Emitente_Legivel'].isin(sel_filial)]
    if sel_cliente: df_filtered = df_filtered[df_filtered['Destinatario_Legivel'].isin(sel_cliente)]
    if sel_transp: df_filtered = df_filtered[df_filtered['Transportadora_Final'].isin(sel_transp)]
    if sel_mod: df_filtered = df_filtered[df_filtered['Frete_Tipo'].isin(sel_mod)]
    if sel_tipo: df_filtered = df_filtered[df_filtered['Operacao'].isin(sel_tipo)]

    if df_filtered.empty:
        context['no_data'] = True
        df_filtered = df.copy()
        empty_search = True
    else:
        empty_search = False

    # --- KPIS ---
    v_frete = df_filtered['frete_valor'].sum()
    v_nf = df_filtered['valor_nf'].sum()
    v_pedagio = df_filtered['pedagio_valor'].sum()
    v_peso = df_filtered['peso_bruto'].sum()
    
    perc_frete_nf = (v_frete / v_nf * 100) if v_nf > 0 else 0
    
    kpis = {
        'total_nf': utils.br_money(v_nf),
        'peso_total': utils.br_weight(v_peso),
        'frete_total': utils.br_money(v_frete),
        'pedagio_total': utils.br_money(v_pedagio),
        'perc_frete': f"{perc_frete_nf:,.2f}%".replace('.', ','),
        'viagens': len(df_filtered)
    }

    if empty_search: kpis = {k: '-' for k in kpis}

    layout_common = {'template': 'plotly_white', 'margin': dict(l=10, r=10, t=30, b=10), 'height': 300}
    config_plot = {'displayModeBar': True, 'scrollZoom': True, 'responsive': True}

    # Mapa
    agg_map = df_filtered.groupby('UF_Dest').agg({
        'peso_bruto': 'sum', 'frete_valor': 'sum', 'valor_nf': 'sum', 
        'pedagio_valor': 'sum', 'chave_nf': 'count'
    }).reset_index()
    
    agg_map['custo_ton'] = agg_map.apply(lambda x: x['frete_valor'] / (x['peso_bruto']/1000) if x['peso_bruto']>0 else 0, axis=1)
    agg_map['perc_frete'] = agg_map.apply(lambda x: (x['frete_valor'] / x['valor_nf'] * 100) if x['valor_nf']>0 else 0, axis=1)
    agg_map['txt_peso'] = agg_map['peso_bruto'].apply(utils.br_weight)
    agg_map['txt_frete'] = agg_map['frete_valor'].apply(utils.br_money)
    agg_map['txt_pedagio'] = agg_map['pedagio_valor'].apply(utils.br_money)
    agg_map['txt_ton'] = agg_map['custo_ton'].apply(lambda x: f"R$ {x:,.2f}".replace('.',','))
    agg_map['txt_perc'] = agg_map['perc_frete'].apply(lambda x: f"{x:,.2f}%".replace('.',','))
    agg_map['lat'] = agg_map['UF_Dest'].apply(lambda x: utils.COORDS_UF.get(x, (0,0))[0])
    agg_map['lon'] = agg_map['UF_Dest'].apply(lambda x: utils.COORDS_UF.get(x, (0,0))[1])
    
    center_lat = agg_map['lat'].mean() if not agg_map.empty else -15
    center_lon = agg_map['lon'].mean() if not agg_map.empty else -55
    start_zoom = 4.5 if not agg_map.empty and len(agg_map) <= 3 else 3

    fig_map = px.scatter_mapbox(
        agg_map, lat='lat', lon='lon', size='peso_bruto', color='frete_valor',
        hover_name='UF_Dest',
        hover_data={'lat': False, 'lon': False, 'peso_bruto': False, 'frete_valor': False, 'chave_nf': True, 'txt_peso': True, 'txt_frete': True, 'txt_ton': True, 'txt_perc': True, 'txt_pedagio': True},
        labels={'chave_nf': 'Qtd NFs', 'txt_peso': 'Peso Bruto', 'txt_frete': 'Valor Frete', 'txt_ton': 'Custo/Ton', 'txt_perc': 'Frete %', 'txt_pedagio': 'Pedágio'},
        zoom=start_zoom, center=dict(lat=center_lat, lon=center_lon),
        mapbox_style="carto-positron", title="Mapa Analítico (Use Zoom/Scroll)"
    )
    fig_map.update_layout(margin=dict(l=0, r=0, t=30, b=0), height=400)

    # Gráficos
    agg_ped = df_filtered.groupby('UF_Dest')['pedagio_valor'].sum().reset_index().sort_values('pedagio_valor', ascending=False)
    fig_ped = px.bar(agg_ped, x='UF_Dest', y='pedagio_valor', title="Custo de Pedágio por UF", text_auto='.2s')
    fig_ped.update_layout(**layout_common); fig_ped.update_traces(marker_color='#fd7e14')

    def generate_top10(groupby_col, metric_col, title, color, is_rs_ton=False):
        if is_rs_ton:
            temp = df_filtered.groupby(groupby_col).agg({'frete_valor':'sum', 'peso_bruto':'sum'}).reset_index()
            temp['rs_ton'] = temp.apply(lambda x: x['frete_valor'] / (x['peso_bruto']/1000) if x['peso_bruto']>0 else 0, axis=1)
            temp = temp.sort_values('rs_ton', ascending=False).head(10)
            y_val = 'rs_ton'
        else:
            temp = df_filtered.groupby(groupby_col)[metric_col].sum().reset_index().sort_values(metric_col, ascending=False).head(10)
            y_val = metric_col
        
        temp['label'] = temp[groupby_col].astype(str).apply(lambda x: x[:20] + '...' if len(x)>20 else x)
        fig = px.bar(temp, x=y_val, y='label', orientation='h', title=title, text_auto='.2s')
        fig.update_traces(marker_color=color, hovertemplate=f"<b>%{{y}}</b><br>Valor: %{{x}}<br>Nome: %{{customdata}}", customdata=temp[groupby_col])
        fig.update_layout(yaxis={'categoryorder':'total ascending', 'title': None}, **layout_common)
        return fig.to_html(full_html=False, include_plotlyjs=False, config=config_plot)

    c_vol = '#0d6efd'; c_cus = '#dc3545'; c_eff = '#198754'
    charts = {
        'cli_vol': generate_top10('Destinatario_Legivel', 'peso_bruto', 'Top Clientes: Volume', c_vol),
        'cli_cst': generate_top10('Destinatario_Legivel', 'frete_valor', 'Top Clientes: Custo Frete', c_cus),
        'cli_rst': generate_top10('Destinatario_Legivel', None, 'Top Clientes: R$/Ton', c_eff, True),
        'fil_vol': generate_top10('Emitente_Legivel', 'peso_bruto', 'Top Filiais: Volume', c_vol),
        'fil_cst': generate_top10('Emitente_Legivel', 'frete_valor', 'Top Filiais: Custo Frete', c_cus),
        'fil_rst': generate_top10('Emitente_Legivel', None, 'Top Filiais: R$/Ton', c_eff, True),
        'cid_vol': generate_top10('cidade_destino', 'peso_bruto', 'Top Cidades: Volume', c_vol),
        'cid_cst': generate_top10('cidade_destino', 'frete_valor', 'Top Cidades: Custo Frete', c_cus),
        'cid_rst': generate_top10('cidade_destino', None, 'Top Cidades: R$/Ton', c_eff, True),
    }

    opts = {
        'ano': sorted(df['Ano'].unique(), reverse=True),
        'mes': sorted(df['Mes'].unique()),
        'dia': sorted(df['Dia'].unique()),
        'filial': sorted(df['Emitente_Legivel'].astype(str).unique()),
        'cliente': sorted(df['Destinatario_Legivel'].astype(str).unique()),
        'transp': sorted(df['Transportadora_Final'].unique()),
        'mod': sorted(df['Frete_Tipo'].unique()),
        'tipo': sorted(df['Operacao'].unique())
    }
    
    selected = {'ano': sel_ano, 'mes': sel_mes, 'dia': sel_dia, 'filial': sel_filial, 'cliente': sel_cliente, 'transp': sel_transp, 'mod': sel_mod, 'tipo': sel_tipo}

    context.update({'kpis': kpis, 'charts': charts, 'chart_map': fig_map.to_html(full_html=False, include_plotlyjs='cdn', config=config_plot), 'chart_ped': fig_ped.to_html(full_html=False, include_plotlyjs=False, config=config_plot), 'opts': opts, 'sel': selected, 'empty_search': empty_search})
    return render(request, 'core/dashboard.html', context)

# ==============================================================================
# 2. ANÁLISE DETALHADA
# ==============================================================================
@login_required
def analise(request):
    df = services.get_dashboard_data()
    context = {}
    
    if df.empty:
        context['no_data'] = True
        return render(request, 'core/analise.html', context)

    # 1. Pré-processamento
    df['dt_obj'] = pd.to_datetime(df['data'], errors='coerce')
    df['Dia'] = df['dt_obj'].dt.day.fillna(0).astype(int)
    df['Mes'] = df['dt_obj'].dt.month.fillna(0).astype(int)
    df['Ano'] = df['dt_obj'].dt.year.fillna(0).astype(int)

    # 2. Captura Filtros
    sel_ano = request.GET.getlist('ano')
    sel_mes = request.GET.getlist('mes')
    sel_dia = request.GET.getlist('dia')
    sel_filial = request.GET.getlist('filial')
    sel_cliente = request.GET.getlist('cliente')
    sel_transp = request.GET.getlist('transp')
    sel_mod = request.GET.getlist('mod_frete')
    sel_tipo = request.GET.getlist('tipo_op')
    
    f_cte = request.GET.get('numero_cte', '').strip()
    f_nf = request.GET.get('numero_nf', '').strip()

    # 3. Aplicação Filtros
    df_filtered = df.copy()

    if sel_ano: df_filtered = df_filtered[df_filtered['Ano'].astype(str).isin(sel_ano)]
    if sel_mes: df_filtered = df_filtered[df_filtered['Mes'].astype(str).isin(sel_mes)]
    if sel_dia: df_filtered = df_filtered[df_filtered['Dia'].astype(str).isin(sel_dia)]
    if sel_filial: df_filtered = df_filtered[df_filtered['Emitente_Legivel'].isin(sel_filial)]
    if sel_cliente: df_filtered = df_filtered[df_filtered['Destinatario_Legivel'].isin(sel_cliente)]
    if sel_transp: df_filtered = df_filtered[df_filtered['Transportadora_Final'].isin(sel_transp)]
    if sel_mod: df_filtered = df_filtered[df_filtered['Frete_Tipo'].isin(sel_mod)]
    if sel_tipo: df_filtered = df_filtered[df_filtered['Operacao'].isin(sel_tipo)]

    # Blindagem de Colunas
    if f_cte and 'numero_cte' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['numero_cte'].astype(str).str.contains(f_cte, na=False)]
    
    if f_nf and 'numero_nf' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['numero_nf'].astype(str) == f_nf]

    # 4. KPIs
    v_frete = df_filtered['frete_valor'].sum()
    v_nf = df_filtered['valor_nf'].sum()
    v_pedagio = df_filtered['pedagio_valor'].sum()
    v_peso = df_filtered['peso_bruto'].sum()
    
    perc_frete_nf = (v_frete / v_nf * 100) if v_nf > 0 else 0

    kpis = {
        'total_nf': utils.br_money(v_nf),
        'peso_total': utils.br_weight(v_peso),
        'frete_total': utils.br_money(v_frete),
        'pedagio_total': utils.br_money(v_pedagio),
        'perc_frete': f"{perc_frete_nf:,.2f}%".replace('.', ','),
        'viagens': len(df_filtered)
    }

    # 5. Tabela
    tabela_docs = df_filtered.copy()
    if not tabela_docs.empty:
        tabela_docs['frete_fmt'] = tabela_docs['frete_valor'].apply(utils.br_money)
        tabela_docs['peso_fmt'] = tabela_docs['peso_bruto'].apply(utils.br_weight)
        # Formatação para o novo campo de peso do CTE
        tabela_docs['peso_cte_fmt'] = tabela_docs['peso_cte_total'].apply(utils.br_weight)
        tabela_docs['valor_nf_fmt'] = tabela_docs['valor_nf'].apply(utils.br_money)
        docs_list = tabela_docs.head(1000).to_dict('records')
    else:
        docs_list = []

    # 6. Drill-down
    selected_nf = request.GET.get('selected_nf')
    detalhes = {}
    if selected_nf:
        df_items = services.get_items_por_nf(selected_nf)
        if not df_items.empty:
            df_items['vl_total_fmt'] = df_items['vl_total'].apply(lambda x: utils.br_money(float(x)))
            df_items['qtd_fmt'] = df_items['qtd_display']
            detalhes['itens'] = df_items.to_dict('records')
            detalhes['numero_nf'] = df_items.iloc[0]['numero_nf']
        else:
            detalhes['msg'] = "Nenhum item encontrado."

    opts = {
        'ano': sorted(df['Ano'].unique(), reverse=True),
        'mes': sorted(df['Mes'].unique()),
        'dia': sorted(df['Dia'].unique()),
        'filial': sorted(df['Emitente_Legivel'].astype(str).unique()),
        'cliente': sorted(df['Destinatario_Legivel'].astype(str).unique()),
        'transp': sorted(df['Transportadora_Final'].unique()),
        'mod': sorted(df['Frete_Tipo'].unique()),
        'tipo': sorted(df['Operacao'].unique())
    }
    
    selected = {
        'ano': sel_ano, 'mes': sel_mes, 'dia': sel_dia, 'filial': sel_filial, 'cliente': sel_cliente,
        'transp': sel_transp, 'mod': sel_mod, 'tipo': sel_tipo, 'val_cte': f_cte, 'val_nf': f_nf
    }

    context.update({'kpis': kpis, 'docs': docs_list, 'detalhes': detalhes, 'opts': opts, 'sel': selected, 'selected_nf': selected_nf})
    return render(request, 'core/analise.html', context)

# ==============================================================================
# 3. UPLOAD DE ARQUIVOS
# ==============================================================================
@login_required
def upload_files(request):
    if request.method == 'GET':
        return render(request, 'core/upload.html')
        
    if request.method == 'POST':
        files = request.FILES.getlist('files')
        tipo = request.POST.get('tipo') 
        
        def file_processor_generator():
            yield render_to_string('core/progress.html', {'request': request})
            
            total_docs = 0
            for f in files:
                if f.name.endswith('.zip'):
                    try:
                        with zipfile.ZipFile(f) as zf:
                            total_docs += len([n for n in zf.namelist() if n.endswith('.xml')])
                    except: total_docs += 1
                else:
                    total_docs += 1
            
            processed_count = 0
            BATCH_SIZE = 200  
            
            objs_cte = []; objs_nfe = []; objs_item = []; logs = []

            def parse_date(dt_str):
                try: return datetime.strptime(dt_str, "%d/%m/%Y").date()
                except: return None
            
            def save_batch():
                close_old_connections()
                try:
                    if objs_cte:
                        Cte.objects.bulk_create(objs_cte, ignore_conflicts=True)
                        objs_cte.clear()
                    if objs_nfe:
                        Nfe.objects.bulk_create(objs_nfe, ignore_conflicts=True)
                        objs_nfe.clear()
                    if objs_item:
                        Item.objects.bulk_create(objs_item, ignore_conflicts=True, batch_size=500)
                        objs_item.clear()
                    if logs:
                        Log.objects.bulk_create(logs, ignore_conflicts=True)
                        logs.clear()
                except Exception as db_err:
                    close_old_connections()
                    print(f"Erro no Batch DB: {db_err}")

            for f in files:
                f.seek(0) 
                try:
                    if f.name.endswith('.zip'):
                        with zipfile.ZipFile(f) as zf:
                            xml_files = [n for n in zf.namelist() if n.endswith('.xml')]
                            for xml_name in xml_files:
                                content = zf.read(xml_name)
                                process_content(content, xml_name, tipo, objs_cte, objs_nfe, objs_item, logs, parse_date)
                                
                                processed_count += 1
                                percent = int((processed_count / total_docs) * 100) if total_docs > 0 else 0
                                yield f'<script>updateProgress({processed_count}, {total_docs}, {percent});</script>'
                                
                                if len(objs_nfe) >= BATCH_SIZE or len(objs_cte) >= BATCH_SIZE:
                                    save_batch()
                    else:
                        content = f.read()
                        process_content(content, f.name, tipo, objs_cte, objs_nfe, objs_item, logs, parse_date)
                        
                        processed_count += 1
                        percent = int((processed_count / total_docs) * 100) if total_docs > 0 else 0
                        yield f'<script>updateProgress({processed_count}, {total_docs}, {percent});</script>'

                        if len(objs_nfe) >= BATCH_SIZE or len(objs_cte) >= BATCH_SIZE:
                            save_batch()
                        
                except Exception as e:
                    logs.append(Log(arquivo=f.name, tipo_doc=tipo, status='ERRO', mensagem=str(e)))
                    yield f'<script>addLog("Erro em {f.name}: {str(e)}");</script>'

            yield '<script>addLog("Gravando dados finais...");</script>'
            save_batch()
            
            msg = f"Sucesso! {processed_count} documentos processados."
            messages.success(request, msg)
            yield f"<script>finishProcess('{request.path}', '{msg}');</script>"

        return StreamingHttpResponse(file_processor_generator())

def process_content(content, fname, tipo, objs_cte, objs_nfe, objs_item, logs, parse_date):
    try:
        if tipo == 'cte':
            rows, err = parsers.parse_cte(content, fname)
            if err: logs.append(Log(arquivo=fname, tipo_doc='CT-e', status='ERRO', mensagem=err))
            else:
                for r in rows:
                    objs_cte.append(Cte(
                        chave_cte_propria=r['chave_cte_propria'], chave_nf=r['chave_nf'], data=parse_date(r['data']),
                        numero_cte=r['numero_cte'], emitente=r['emitente'], cnpj_emit=r['cnpj_emit'],
                        remetente=r['remetente'], destinatario=r['destinatario'], frete_valor=r['frete_valor'],
                        peso_kg=r['peso_kg'], numero_nf_cte=r['numero_nf_cte'], cidade_origem=r['cidade_origem'],
                        cidade_destino=r['cidade_destino'], pedagio_valor=r['pedagio_valor'], tp_cte=r['tp_cte'], arquivo=fname
                    ))
        elif tipo == 'nfe':
            header, err = parsers.parse_nfe_header(content, fname)
            if err: logs.append(Log(arquivo=fname, tipo_doc='NF-e', status='ERRO', mensagem=err))
            else:
                objs_nfe.append(Nfe(
                    chave_nf=header['chave_nf'], data=parse_date(header['data']), numero_nf=header['numero_nf'],
                    emitente=header['emitente'], destinatario=header['destinatario'], cnpj_emit=header['cnpj_emit'],
                    cnpj_dest=header['cnpj_dest'], uf_dest=header['uf_dest'], valor_nf=header['valor_nf'],
                    peso_bruto=header['peso_bruto'], transportadora=header['transportadora'], cidade_origem=header['cidade_origem'],
                    cidade_destino=header['cidade_destino'], mod_frete=header['mod_frete'], cfop_predominante=header['cfop_predominante'],
                    tipo_operacao=header['tipo_operacao'], qtd_itens=header['qtd_itens'], arquivo=fname
                ))
                items, _ = parsers.parse_nfe_items(content, fname)
                for i in items:
                    objs_item.append(Item(
                        chave_nf=i['chave_nf'], numero_nf=i['numero_nf'], emitente=i['emitente'], item_num=i['item_num'],
                        produto=i['produto'], ncm=i['ncm'], cfop=i['cfop'], unidade=i['unidade'], qtd_display=i['qtd_display'],
                        qtd_float=i['qtd_float'], vl_total=i['vl_total'], arquivo=fname
                    ))
    except Exception as e:
        logs.append(Log(arquivo=fname, tipo_doc=tipo, status='ERRO FATAL', mensagem=str(e)))