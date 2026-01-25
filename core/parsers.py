import xmltodict
from lxml import etree
from datetime import datetime
from .utils import xml_float, br_weight, limpar_cnpj

PARSER = etree.XMLParser(recover=True, encoding='utf-8')

# ==============================================================================
# PARSER DE CTE
# ==============================================================================
def strip_namespace(root):
    for elem in root.getiterator():
        if not hasattr(elem.tag, 'find'): continue
        i = elem.tag.find('}')
        if i >= 0: elem.tag = elem.tag[i+1:]
    return root

def parse_cte(raw, fname):
    try:
        if isinstance(raw, str): raw = raw.encode('utf-8')
        rt = etree.fromstring(raw, PARSER); rt = strip_namespace(rt)
        inf = rt.find(".//infCte")
        
        if inf is None:
            if rt.find(".//retEventoCTe") is not None: return [], "Evento de CT-e"
            return [], "XML Inválido"
        
        # Safe Get para atributos
        cte_id = inf.get("Id", "")
        chave_cte_propria = cte_id.replace("CTe", "").strip() if cte_id else ""
        
        dh = inf.findtext("ide/dhEmi") or ""; data = dh[:10] 
        try: data = datetime.strptime(data, "%Y-%m-%d").strftime("%d/%m/%Y")
        except: pass
        
        tp_cte = inf.findtext("ide/tpCTe")
        vp = inf.find(".//vTPrest"); frete = xml_float(vp.text) if vp is not None else 0.0
        peso = sum(xml_float(n.text) for n in inf.findall(".//qCarga"))
        
        pedagio = 0.0
        for c in inf.findall(".//Comp"):
            nm = c.findtext("xNome","").upper()
            if "PEDAGIO" in nm or "VALE" in nm: pedagio += xml_float(c.findtext("vComp","0"))
                
        m_ini = inf.findtext("ide/xMunIni"); u_ini = inf.findtext("ide/UFIni")
        m_fim = inf.findtext("ide/xMunFim") or inf.findtext("dest/enderDest/xMun")
        u_fim = inf.findtext("ide/UFFim") or inf.findtext("dest/enderDest/UF")
        chave_ref = inf.findtext(".//infCteComp/chCTe", "")
        
        chaves = [n.findtext("chave") for n in inf.findall(".//infNFe") if n.findtext("chave")]
        if not chaves: chaves = [""]

        lines = []
        for k in chaves:
            k = str(k).strip()
            n_nf = str(int(k[25:34])) if k and len(k)==44 and k.isdigit() else ""
            lines.append({
                "chave_cte_propria": chave_cte_propria,
                "chave_nf": k,
                "data": data, 
                "numero_cte": inf.findtext("ide/nCT"),
                "emitente": inf.findtext("emit/xNome"), 
                "cnpj_emit": inf.findtext("emit/CNPJ"),
                "remetente": inf.findtext("rem/xNome"), 
                "destinatario": inf.findtext("dest/xNome"),
                "frete_valor": frete, 
                "peso_kg": peso, 
                "numero_nf_cte": n_nf,
                "cidade_origem": f"{m_ini}-{u_ini}" if m_ini else "ND",
                "cidade_destino": f"{m_fim}-{u_fim}" if m_fim else "ND",
                "pedagio_valor": pedagio, 
                "chave_ref_cte": chave_ref,
                "tp_cte": tp_cte,
                "arquivo": fname
            })
        return lines, None
    except Exception as e: return [], str(e)

# ==============================================================================
# PARSER DE NFE - HEADER (BLINDADO CONTRA NoneType)
# ==============================================================================
def parse_nfe_header(content, filename):
    try:
        doc = xmltodict.parse(content)
        
        # Navegação Segura: (dict or {}) garante que nunca seja None
        nfe_proc = doc.get('nfeProc') or {}
        # Tenta pegar NFe dentro de nfeProc ou direto na raiz
        nfe_node = nfe_proc.get('NFe') or doc.get('NFe') or {}
        inf_nfe = nfe_node.get('infNFe') or {}

        ide = inf_nfe.get('ide') or {}
        emit = inf_nfe.get('emit') or {}
        dest = inf_nfe.get('dest') or {}
        
        total_group = inf_nfe.get('total') or {}
        total = total_group.get('ICMSTot') or {}
        
        transp = inf_nfe.get('transp') or {}
        transporta = transp.get('transporta') or {}
        vol = transp.get('vol') or {}
        
        # --- Endereço do DESTINATÁRIO (Blindado) ---
        ender_dest = dest.get('enderDest') or {}
        endereco_dest_completo = f"{ender_dest.get('xLgr', '')}, {ender_dest.get('nro', '')}"
        
        # --- Endereço do EMITENTE (Blindado) ---
        ender_emit = emit.get('enderEmit') or {}
        endereco_emit_completo = f"{ender_emit.get('xLgr', '')}, {ender_emit.get('nro', '')}"
        
        dt_emissao = ide.get('dhEmi', '')
        if not dt_emissao: dt_emissao = ide.get('dEmi', '')
        if dt_emissao: dt_emissao = dt_emissao[:10]
        try: dt_obj = datetime.strptime(dt_emissao, "%Y-%m-%d").strftime("%d/%m/%Y")
        except: dt_obj = None

        det = inf_nfe.get('det') or []
        # Se for um único item (dict), transforma em lista
        if isinstance(det, dict):
            qtd_itens = 1
        elif isinstance(det, list):
            qtd_itens = len(det)
        else:
            qtd_itens = 0

        peso_b = 0.0
        if isinstance(vol, list):
            for v in vol: 
                val = v.get('pesoB') or 0
                peso_b += float(val)
        elif isinstance(vol, dict):
            val = vol.get('pesoB') or 0
            peso_b = float(val)

        chave_raw = inf_nfe.get('@Id', '')
        chave_final = chave_raw.replace('NFe', '').strip() if chave_raw else ''

        header = {
            'chave_nf': chave_final,
            'data': dt_obj,
            'numero_nf': ide.get('nNF', ''),
            'emitente': emit.get('xNome', ''),
            'cnpj_emit': limpar_cnpj(emit.get('CNPJ', '')),
            'destinatario': dest.get('xNome', ''),
            'cnpj_dest': limpar_cnpj(dest.get('CNPJ', '') or dest.get('CPF', '')),
            
            # Dados Destinatário
            'uf_dest': ender_dest.get('UF', ''),
            'cidade_destino': ender_dest.get('xMun', ''),
            'endereco_dest': endereco_dest_completo,
            'bairro_dest': ender_dest.get('xBairro', ''),
            'cep_dest': ender_dest.get('CEP', ''),

            # Dados Emitente
            'uf_emit': ender_emit.get('UF', ''),
            'cidade_origem': ender_emit.get('xMun', ''),
            'endereco_emit': endereco_emit_completo,
            'bairro_emit': ender_emit.get('xBairro', ''),
            'cep_emit': ender_emit.get('CEP', ''),

            'valor_nf': float(total.get('vNF') or 0),
            'peso_bruto': peso_b,
            'transportadora': transporta.get('xNome', 'Próprio/Outros'),
            'mod_frete': transp.get('modFrete', '9'),
            'cfop_predominante': '', 'tipo_operacao': ide.get('tpNF', '1'), 'qtd_itens': qtd_itens
        }
        return header, None
    except Exception as e: return None, f"Erro Header: {str(e)}"

# ==============================================================================
# PARSER DE NFE - ITENS (BLINDADO)
# ==============================================================================
def parse_nfe_items(content, filename):
    try:
        doc = xmltodict.parse(content)
        
        nfe_proc = doc.get('nfeProc') or {}
        nfe_node = nfe_proc.get('NFe') or doc.get('NFe') or {}
        inf_nfe = nfe_node.get('infNFe') or {}

        chave_raw = inf_nfe.get('@Id', '')
        chave_nf = chave_raw.replace('NFe', '').strip() if chave_raw else ''
        
        ide = inf_nfe.get('ide') or {}
        numero_nf = ide.get('nNF', '')
        
        emit = inf_nfe.get('emit') or {}
        emitente = emit.get('xNome', '')

        dets = inf_nfe.get('det') or []
        if isinstance(dets, dict): dets = [dets]

        items = []
        for i, d in enumerate(dets):
            prod = d.get('prod') or {}
            item_num = d.get('@nItem')
            if not item_num: item_num = str(i + 1)

            nome_produto = prod.get('xProd')
            if not nome_produto: nome_produto = prod.get('cProd', 'PRODUTO SEM NOME')

            items.append({
                'chave_nf': chave_nf,
                'numero_nf': numero_nf,
                'emitente': emitente,
                'item_num': item_num,
                'produto': nome_produto,
                'ncm': prod.get('NCM', ''),
                'cfop': prod.get('CFOP', ''),
                'unidade': prod.get('uCom', ''),
                'qtd_display': br_weight(prod.get('qCom')),
                'qtd_float': float(prod.get('qCom') or 0),
                'vl_total': float(prod.get('vProd') or 0),
                'arquivo': filename
            })
        return items, None
    except Exception as e:
        return [], f"Erro Items: {str(e)}"