import xmltodict
from lxml import etree
from datetime import datetime
from .utils import xml_float, br_weight, limpar_cnpj

PARSER = etree.XMLParser(recover=True, encoding='utf-8')

# ==============================================================================
# PARSER DE CTE (Mantido com LXML - Estava funcionando bem)
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
        
        chave_cte_propria = inf.get("Id", "").replace("CTe", "")
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
# PARSER DE NFE (Atualizado para XMLTODICT - Mais preciso para tags)
# ==============================================================================
def parse_nfe_header(content, filename):
    try:
        # Garante que lê como dicionário
        doc = xmltodict.parse(content)
        
        # Navega na estrutura (pode ter nfeProc ou ser direto NFe)
        inf_nfe = doc.get('nfeProc', {}).get('NFe', {}).get('infNFe', {})
        if not inf_nfe:
            inf_nfe = doc.get('NFe', {}).get('infNFe', {})

        ide = inf_nfe.get('ide', {})
        emit = inf_nfe.get('emit', {})
        dest = inf_nfe.get('dest', {})
        total = inf_nfe.get('total', {}).get('ICMSTot', {})
        transp = inf_nfe.get('transp', {}).get('transporta', {})
        vol = inf_nfe.get('transp', {}).get('vol', {})
        
        # Endereço para Geolocalização
        ender_dest = dest.get('enderDest', {})
        endereco_completo = f"{ender_dest.get('xLgr', '')}, {ender_dest.get('nro', '')}"
        
        # Tratamento de Data
        dt_emissao = ide.get('dhEmi', '')
        if not dt_emissao: dt_emissao = ide.get('dEmi', '')
        if dt_emissao: dt_emissao = dt_emissao[:10]
        try:
            dt_obj = datetime.strptime(dt_emissao, "%Y-%m-%d").strftime("%d/%m/%Y")
        except:
            dt_obj = None

        # Conta itens
        det = inf_nfe.get('det', [])
        qtd_itens = len(det) if isinstance(det, list) else 1

        # Peso (pode ser lista ou dict)
        peso_b = 0.0
        if isinstance(vol, list):
            for v in vol: peso_b += float(v.get('pesoB', 0) or 0)
        elif isinstance(vol, dict):
            peso_b = float(vol.get('pesoB', 0) or 0)

        header = {
            'chave_nf': inf_nfe.get('@Id', '').replace('NFe', ''),
            'data': dt_obj,
            'numero_nf': ide.get('nNF', ''),
            'emitente': emit.get('xNome', ''),
            'cnpj_emit': limpar_cnpj(emit.get('CNPJ', '')),
            
            'destinatario': dest.get('xNome', ''),
            'cnpj_dest': limpar_cnpj(dest.get('CNPJ', '') or dest.get('CPF', '')),
            'uf_dest': ender_dest.get('UF', ''),
            'cidade_destino': ender_dest.get('xMun', ''),
            
            # Campos para API de Mapa
            'endereco': endereco_completo,
            'bairro': ender_dest.get('xBairro', ''),
            'cep': ender_dest.get('CEP', ''),

            'valor_nf': float(total.get('vNF', 0) or 0),
            'peso_bruto': peso_b,
            'transportadora': transp.get('xNome', 'Próprio/Outros') if transp else 'Próprio/Outros',
            'cidade_origem': emit.get('enderEmit', {}).get('xMun', ''),
            'mod_frete': inf_nfe.get('transp', {}).get('modFrete', '9'),
            'cfop_predominante': '', 
            'tipo_operacao': ide.get('tpNF', '1'), 
            'qtd_itens': qtd_itens
        }
        return header, None
    except Exception as e:
        return None, f"Erro Header: {str(e)}"

def parse_nfe_items(content, filename):
    try:
        doc = xmltodict.parse(content)
        inf_nfe = doc.get('nfeProc', {}).get('NFe', {}).get('infNFe', {})
        if not inf_nfe:
            inf_nfe = doc.get('NFe', {}).get('infNFe', {})

        chave_nf = inf_nfe.get('@Id', '').replace('NFe', '')
        numero_nf = inf_nfe.get('ide', {}).get('nNF', '')
        emitente = inf_nfe.get('emit', {}).get('xNome', '')

        # Pega a lista de detalhes (itens)
        dets = inf_nfe.get('det', [])
        
        # xmltodict retorna Dict se for 1 item, e List se forem vários. Normalizamos para Lista.
        if isinstance(dets, dict):
            dets = [dets]

        items = []
        for d in dets:
            prod = d.get('prod', {})
            
            # Aqui estava o erro: garantimos pegar 'xProd' (descrição)
            # Se xProd falhar, pegamos cProd como fallback
            nome_produto = prod.get('xProd')
            if not nome_produto:
                nome_produto = prod.get('cProd', 'PRODUTO SEM NOME')

            items.append({
                'chave_nf': chave_nf,
                'numero_nf': numero_nf,
                'emitente': emitente,
                'item_num': d.get('@nItem'), # Atributo nItem do XML
                'produto': nome_produto,     # CORREÇÃO AQUI
                'ncm': prod.get('NCM', ''),
                'cfop': prod.get('CFOP', ''),
                'unidade': prod.get('uCom', ''),
                'qtd_display': br_weight(prod.get('qCom')),
                'qtd_float': float(prod.get('qCom', 0) or 0),
                'vl_total': float(prod.get('vProd', 0) or 0),
                'arquivo': filename
            })
        return items, None
    except Exception as e:
        return [], f"Erro Items: {str(e)}"