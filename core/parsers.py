from lxml import etree
from datetime import datetime
from .utils import xml_float, br_weight
from . import config # Apenas se precisar de referencias

PARSER = etree.XMLParser(recover=True, encoding='utf-8')

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
        # Nota: Data formatada como DD/MM/YYYY no original, será convertida na view
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

# (Inclua aqui parse_nfe_header e parse_nfe_items exatamente como no seu arquivo original)
# Apenas certifique-se de importar lxml, datetime e utils no topo.
def parse_nfe_header(raw, fname):
    try:
        if isinstance(raw, str): raw = raw.encode('utf-8')
        rt = etree.fromstring(raw, PARSER); rt = strip_namespace(rt)
        inf = rt.find(".//infNFe"); 
        if inf is None: return None, "XML NFe Inválido"

        ide = inf.find("ide"); em = inf.find("emit"); dest = inf.find("dest")
        tot = inf.find(".//ICMSTot"); tr = inf.find("transp")
        if ide is None or em is None: return None, "Dados Incompletos"

        dh = ide.findtext("dhEmi") or ""; data = dh[:10]
        try: data = datetime.strptime(data, "%Y-%m-%d").strftime("%d/%m/%Y")
        except: pass
        
        pb = 0.0
        if tr is not None: 
            for v in tr.findall("vol"): pb += xml_float(v.findtext("pesoB","0"))
        
        qtd_itens = len(inf.findall(".//det"))

        def get_city(n, t):
            x = n.find(t)
            return f"{x.findtext('xMun','')}-{x.findtext('UF','')}" if x is not None else ""
        
        cid_orig = get_city(em,"enderEmit")
        cid_dest = get_city(dest,"enderDest") if dest is not None else "ND"
        
        # Lógica simplificada aqui, use o original se preferir
        header = {
            "chave_nf": inf.get("Id","").replace("NFe",""), 
            "data": data, 
            "numero_nf": ide.findtext("nNF"),
            "emitente": em.findtext("xNome"), 
            "destinatario": dest.findtext("xNome") if dest is not None else "Consumidor",
            "cnpj_emit": em.findtext("CNPJ",""), 
            "cnpj_dest": dest.findtext("CNPJ","") if dest is not None else "", 
            "uf_dest": dest.findtext("enderDest/UF") if dest is not None else "",
            "valor_nf": xml_float(tot.findtext("vNF","0")) if tot is not None else 0.0,
            "peso_bruto": pb, 
            "transportadora": tr.findtext("transporta/xNome","") if tr is not None else "",
            "cidade_origem": cid_orig, 
            "cidade_destino": cid_dest,
            "cep_origem": em.findtext("enderEmit/CEP", ""), 
            "cep_destino": dest.findtext("enderDest/CEP", "") if dest is not None else "", 
            "distancia": 0.0, 
            "mod_frete": tr.findtext("modFrete","") if tr is not None else "",
            "cfop_predominante": inf.findtext("det/prod/CFOP",""), 
            "tipo_operacao": "Outros", # Será atualizado depois
            "qtd_itens": qtd_itens, 
            "arquivo": fname
        }
        return header, None
    except Exception as e: return None, str(e)

def parse_nfe_items(raw, fname):
    try:
        if isinstance(raw, str): raw = raw.encode('utf-8')
        rt = etree.fromstring(raw, PARSER); rt = strip_namespace(rt)
        inf = rt.find(".//infNFe"); k = inf.get("Id","").replace("NFe","")
        items = []
        for d in inf.findall("det"):
            p = d.find("prod")
            items.append({
                "chave_nf": k, "numero_nf": inf.findtext("ide/nNF"), "emitente": inf.findtext("emit/xNome"),
                "item_num": d.get("nItem"), "produto": p.findtext("xProd"), "ncm": p.findtext("NCM"),
                "cfop": p.findtext("CFOP"), "unidade": p.findtext("uCom"), 
                "qtd_display": br_weight(xml_float(p.findtext("qCom"))), "qtd_float": xml_float(p.findtext("qCom")),
                "vl_total": xml_float(p.findtext("vProd")), "arquivo": fname
            })
        return items, None
    except Exception as e: return [], str(e)