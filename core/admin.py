import csv
import io
from django.shortcuts import render, redirect
from django.urls import path
from django import forms
from django.contrib import admin, messages
from django.http import HttpResponse, StreamingHttpResponse
from django.template.loader import render_to_string
from django.utils.html import mark_safe
from django.urls import reverse
from django.contrib.admin.utils import quote
from .models import Nfe, Cte, Item, Log, MemoriaIa, Cliente, ProdutoMap, Transportadora

# ==============================================================================
# AÇÕES DE EXPORTAÇÃO (CSV)
# ==============================================================================

def export_logs_csv(modeladmin, request, queryset):
    """Exporta Logs para CSV"""
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="logs_sistema.csv"'
    response.write(u'\ufeff'.encode('utf8'))
    writer = csv.writer(response, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    writer.writerow(['ID', 'Data/Hora', 'Tipo Doc', 'Status', 'Arquivo', 'Mensagem'])
    for obj in queryset:
        data_fmt = obj.data_hora.strftime("%d/%m/%Y %H:%M:%S") if obj.data_hora else ""
        writer.writerow([obj.pk, data_fmt, obj.tipo_doc, obj.status, obj.arquivo, obj.mensagem])
    return response
export_logs_csv.short_description = "📥 Baixar Logs Selecionados (.csv)"

def export_produtos_csv(modeladmin, request, queryset):
    """Exporta Produtos"""
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="mapeamento_produtos.csv"'
    response.write(u'\ufeff'.encode('utf8'))
    writer = csv.writer(response, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['Nome no XML', 'Peso Unitário (KG)'])
    for obj in queryset:
        peso_str = str(obj.peso_unitario_kg).replace('.', ',')
        writer.writerow([obj.nome_produto, peso_str])
    return response
export_produtos_csv.short_description = "📥 Baixar Planilha para Correção (.csv)"

def export_clientes_csv(modeladmin, request, queryset):
    """Exporta Clientes COMPLETO para CSV"""
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="base_clientes_completa.csv"'
    response.write(u'\ufeff'.encode('utf8'))
    writer = csv.writer(response, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    headers = [
        'ID', 'CNPJ/CPF', 'Nome Fantasia', 'Razão Social', 
        'Endereço', 'Bairro', 'CEP', 'Cidade', 'UF', 
        'Latitude', 'Longitude', 'Distancia KM'
    ]
    writer.writerow(headers)
    
    for obj in queryset:
        lat = str(obj.latitude).replace('.', ',') if obj.latitude else ""
        lon = str(obj.longitude).replace('.', ',') if obj.longitude else ""
        dist = str(obj.distancia_km).replace('.', ',') if obj.distancia_km else ""
        
        writer.writerow([
            obj.pk, obj.cpf_cnpj, obj.nome, obj.razao_social, 
            obj.endereco, obj.bairro, obj.cep, obj.cidade, 
            obj.uf, lat, lon, dist
        ])
    return response
export_clientes_csv.short_description = "📥 Baixar Base Completa (.csv)"

# ==============================================================================
# FORMULÁRIO DE IMPORTAÇÃO
# ==============================================================================
class CsvImportForm(forms.Form):
    csv_file = forms.FileField(label="Selecione o arquivo CSV (.csv ou .txt)")

# ==============================================================================
# MIXIN DE NAVEGAÇÃO (BLINDADO CONTRA NoReverseMatch)
# ==============================================================================
class NavigationMixin:
    def navigation_buttons(self, obj):
        opts = self.model._meta
        pk_name = opts.pk.name
        
        # Filtra anterior/próximo
        prev_obj = self.model.objects.filter(**{f"{pk_name}__lt": obj.pk}).order_by(f"-{pk_name}").first()
        next_obj = self.model.objects.filter(**{f"{pk_name}__gt": obj.pk}).order_by(pk_name).first()

        buttons = []
        style_btn = "background-color: #79aec8; color: white; padding: 6px 12px; text-decoration: none; border-radius: 4px; font-weight: bold; margin-right: 10px; transition: background 0.3s;"
        style_disabled = "background-color: #f0f0f0; color: #ccc; padding: 6px 12px; border-radius: 4px; border: 1px solid #ddd; margin-right: 10px; cursor: not-allowed;"

        # CORREÇÃO: Só cria o botão se o objeto existir E tiver uma PK válida (não vazia)
        if prev_obj and str(prev_obj.pk).strip():
            try:
                url = reverse(f'admin:{opts.app_label}_{opts.model_name}_change', args=[quote(prev_obj.pk)])
                buttons.append(f'<a href="{url}" style="{style_btn}">⬅️ Anterior</a>')
            except:
                buttons.append(f'<span style="{style_disabled}">⬅️ Anterior</span>')
        else:
            buttons.append(f'<span style="{style_disabled}">⬅️ Anterior</span>')

        if next_obj and str(next_obj.pk).strip():
            try:
                url = reverse(f'admin:{opts.app_label}_{opts.model_name}_change', args=[quote(next_obj.pk)])
                buttons.append(f'<a href="{url}" style="{style_btn}">Próximo ➡️</a>')
            except:
                buttons.append(f'<span style="{style_disabled}">Próximo ➡️</span>')
        else:
            buttons.append(f'<span style="{style_disabled}">Próximo ➡️</span>')

        return mark_safe('<div style="margin: 10px 0;">' + "".join(buttons) + '</div>')
    
    navigation_buttons.short_description = "Navegação"
    navigation_buttons.allow_tags = True

# ==============================================================================
# ADMINS
# ==============================================================================

@admin.register(ProdutoMap)
class ProdutoMapAdmin(NavigationMixin, admin.ModelAdmin):
    list_display = ('nome_produto', 'peso_unitario_kg', 'manual')
    search_fields = ('nome_produto',)
    list_filter = ('manual', 'peso_unitario_kg') 
    list_editable = ('peso_unitario_kg',)
    readonly_fields = ('navigation_buttons',)
    actions = [export_produtos_csv]
    change_list_template = "core/change_list_produtomap.html"

    def get_queryset(self, request):
        return super().get_queryset(request).order_by('nome_produto')

    def get_urls(self):
        urls = super().get_urls()
        my_urls = [path('import-csv/', self.admin_site.admin_view(self.import_csv), name="import_produtos_csv"),]
        return my_urls + urls

    def import_csv(self, request):
        if request.method == "POST":
            csv_file = request.FILES["csv_file"]
            if not csv_file.name.endswith('.csv') and not csv_file.name.endswith('.txt'):
                messages.error(request, 'O arquivo deve ser CSV ou TXT.')
                return redirect("..")
            try:
                file_data = csv_file.read().decode("utf-8-sig")
                io_string = io.StringIO(file_data)
                reader = csv.DictReader(io_string, delimiter=';')
                if 'Nome no XML' not in reader.fieldnames or 'Peso Unitário (KG)' not in reader.fieldnames:
                    messages.error(request, 'Colunas obrigatórias: "Nome no XML" e "Peso Unitário (KG)".')
                    return redirect("..")
                rows = list(reader)
                total_lines = len(rows)
            except Exception as e:
                messages.error(request, f"Erro leitura: {str(e)}")
                return redirect("..")

            def item_processor():
                yield render_to_string('core/progress.html', request=request)
                count = 0
                for row in rows:
                    nome = row['Nome no XML'].strip()
                    peso_str = row['Peso Unitário (KG)'].strip().replace(',', '.')
                    try:
                        peso_val = float(peso_str)
                        ProdutoMap.objects.update_or_create(nome_produto=nome, defaults={'peso_unitario_kg': peso_val, 'manual': True})
                        count += 1
                    except: pass
                    if total_lines > 0:
                        percent = int((count / total_lines) * 100)
                        yield f'<script>updateProgress({count}, {total_lines}, {percent});</script>'
                
                redirect_url = reverse('admin:core_produtomap_changelist')
                yield f"<script>finishProcess('{redirect_url}', 'Importação concluída! {count} itens processados.');</script>"

            return StreamingHttpResponse(item_processor())
        form = CsvImportForm()
        return render(request, "core/import_csv.html", {"form": form})

@admin.register(Cliente)
class ClienteAdmin(NavigationMixin, admin.ModelAdmin):
    list_display = ('nome', 'cidade', 'uf', 'distancia_km', 'latitude', 'longitude')
    search_fields = ('nome', 'cpf_cnpj', 'cidade')
    list_filter = ('uf',)
    readonly_fields = ('navigation_buttons',)
    actions = ['atualizar_geolocalizacao', export_clientes_csv]
    change_list_template = "core/change_list_cliente.html"

    def get_urls(self):
        urls = super().get_urls()
        my_urls = [path('import-clientes-csv/', self.admin_site.admin_view(self.import_csv), name="import_clientes_csv"),]
        return my_urls + urls

    def import_csv(self, request):
        if request.method == "POST":
            csv_file = request.FILES["csv_file"]
            try:
                file_data = csv_file.read().decode("utf-8-sig")
                io_string = io.StringIO(file_data)
                reader = csv.DictReader(io_string, delimiter=';')
                if 'CNPJ/CPF' not in reader.fieldnames:
                    messages.error(request, 'Erro: Coluna "CNPJ/CPF" é obrigatória.')
                    return redirect("..")
                rows = list(reader)
                total_lines = len(rows)
            except Exception as e:
                messages.error(request, f"Erro ao ler arquivo: {str(e)}")
                return redirect("..")

            def item_processor():
                yield render_to_string('core/progress.html', request=request)
                count = 0
                for row in rows:
                    cnpj = row.get('CNPJ/CPF', '').strip()
                    nome = row.get('Nome Fantasia', '').strip() or row.get('Nome', '').strip()
                    razao = row.get('Razão Social', '').strip()
                    end = row.get('Endereço', '').strip()
                    bairro = row.get('Bairro', '').strip()
                    cep = row.get('CEP', '').strip()
                    cidade = row.get('Cidade', '').strip()
                    uf = row.get('UF', '').strip()
                    
                    lat_str = row.get('Latitude', '').strip().replace(',', '.')
                    lon_str = row.get('Longitude', '').strip().replace(',', '.')
                    dist_str = row.get('Distancia KM', '').strip().replace(',', '.')

                    defaults = {}
                    if nome: defaults['nome'] = nome
                    if razao: defaults['razao_social'] = razao
                    if end: defaults['endereco'] = end
                    if bairro: defaults['bairro'] = bairro
                    if cep: defaults['cep'] = cep
                    if cidade: defaults['cidade'] = cidade
                    if uf: defaults['uf'] = uf
                    try:
                        if lat_str: defaults['latitude'] = float(lat_str)
                        if lon_str: defaults['longitude'] = float(lon_str)
                        if dist_str: defaults['distancia_km'] = float(dist_str)
                    except: pass 

                    if cnpj:
                        Cliente.objects.update_or_create(cpf_cnpj=cnpj, defaults=defaults)
                        count += 1
                    if total_lines > 0:
                        percent = int((count / total_lines) * 100)
                        yield f'<script>updateProgress({count}, {total_lines}, {percent});</script>'

                redirect_url = reverse('admin:core_cliente_changelist')
                yield f"<script>finishProcess('{redirect_url}', 'Base de Clientes atualizada! {count} registros processados.');</script>"

            return StreamingHttpResponse(item_processor())
        form = CsvImportForm()
        return render(request, "core/import_csv.html", {"form": form})

    @admin.action(description='Atualizar Geolocalização e Distância (API)')
    def atualizar_geolocalizacao(self, request, queryset):
        from .utils import get_lat_lon, get_distancia_osrm, ORIGEM_PADRAO
        count = 0
        for cliente in queryset:
            lat, lon = get_lat_lon(cliente.endereco, cliente.bairro, cliente.cidade, cliente.uf, cliente.cep)
            if lat and lon:
                cliente.latitude = lat
                cliente.longitude = lon
                dist = get_distancia_osrm(ORIGEM_PADRAO['lat'], ORIGEM_PADRAO['lon'], lat, lon)
                cliente.distancia_km = dist
                cliente.save()
                count += 1
        self.message_user(request, f"{count} clientes atualizados com sucesso!")

# ==============================================================================
# NFE ADMIN COM EXIBIÇÃO DE FRETE (NOVO)
# ==============================================================================
@admin.register(Nfe)
class NfeAdmin(NavigationMixin, admin.ModelAdmin):
    # Adicionado 'tipo_frete_display' na lista
    list_display = ('chave_nf', 'numero_nf', 'emitente', 'valor_nf', 'data', 'tipo_frete_display')
    search_fields = ('numero_nf', 'emitente', 'destinatario')
    list_filter = ('data', 'uf_dest', 'mod_frete') # Filtro lateral
    readonly_fields = ('navigation_buttons',)

    # Função que traduz 0/1 para Texto Colorido
    def tipo_frete_display(self, obj):
        if obj.mod_frete == '0':
            return mark_safe('<span style="color:blue; font-weight:bold;">CIF (Remetente)</span>')
        elif obj.mod_frete == '1':
            return mark_safe('<span style="color:green; font-weight:bold;">FOB (Destinatário)</span>')
        elif obj.mod_frete == '2':
            return "Terceiros"
        elif obj.mod_frete == '3':
            return "FOB Retira"
        elif obj.mod_frete == '4':
            return "FOB"
        elif obj.mod_frete == '9':
            return "Sem Frete"
        
        # Caso venha vazio ou outro código
        return f"Outro ({obj.mod_frete})" if obj.mod_frete else "-"
    
    tipo_frete_display.short_description = "Tipo Frete"
    tipo_frete_display.allow_tags = True
    tipo_frete_display.admin_order_field = 'mod_frete'

@admin.register(Cte)
class CteAdmin(NavigationMixin, admin.ModelAdmin):
    list_display = ('numero_cte', 'emitente', 'frete_valor', 'data')
    search_fields = ('numero_cte', 'chave_cte_propria')
    readonly_fields = ('navigation_buttons',)

@admin.register(Item)
class ItemAdmin(NavigationMixin, admin.ModelAdmin):
    list_display = ('produto', 'numero_nf', 'qtd_formatada', 'qtd_display', 'peso_estimado_total', 'vl_total')
    search_fields = ('produto',)
    readonly_fields = ('navigation_buttons',)

@admin.register(Log)
class LogAdmin(NavigationMixin, admin.ModelAdmin):
    list_display = ('data_hora', 'tipo_doc', 'status', 'arquivo')
    list_filter = ('status', 'tipo_doc')
    search_fields = ('arquivo', 'mensagem')
    readonly_fields = ('navigation_buttons',)
    actions = [export_logs_csv]

@admin.register(MemoriaIa)
class MemoriaIaAdmin(NavigationMixin, admin.ModelAdmin):
    list_display = ('cfop', 'fluxo', 'tipo_definido')
    readonly_fields = ('navigation_buttons',)
    

# ==============================================================================
# ADMIN TRANSPORTADORA
# ==============================================================================

def export_transportadoras_csv(modeladmin, request, queryset):
    """Exporta Transportadoras para CSV"""
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="transportadoras.csv"'
    response.write(u'\ufeff'.encode('utf8'))
    writer = csv.writer(response, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    headers = [
        'CNPJ', 'Nome', 'Endereço', 'Cidade', 'UF', 'CEP', 
        'Perfil Tributário', 'Cidades Atendidas', 'Tipos Frete'
    ]
    writer.writerow(headers)
    
    for obj in queryset:
        writer.writerow([
            obj.cnpj, 
            obj.nome, 
            obj.endereco or '', 
            obj.cidade or '', 
            obj.uf or '', 
            obj.cep or '', 
            obj.perfil_tributario,
            obj.cidades_atendidas or '',
            obj.tipos_frete or ''
        ])
    return response
export_transportadoras_csv.short_description = "📥 Baixar Transportadoras (.csv)"

@admin.register(Transportadora)
class TransportadoraAdmin(NavigationMixin, admin.ModelAdmin):
    list_display = ('cnpj', 'nome', 'cidade', 'uf', 'perfil_tributario', 'tipos_frete')
    search_fields = ('nome', 'cnpj', 'cidades_atendidas')
    list_filter = ('uf', 'perfil_tributario')
    readonly_fields = ('navigation_buttons',)
    actions = [export_transportadoras_csv]

    def get_urls(self):
        urls = super().get_urls()
        my_urls = [path('import-csv/', self.admin_site.admin_view(self.import_csv), name="import_transportadoras_csv"),]
        return my_urls + urls

    def import_csv(self, request):
        if request.method == "POST":
            csv_file = request.FILES["csv_file"]
            try:
                file_data = csv_file.read().decode("utf-8-sig")
                io_string = io.StringIO(file_data)
                reader = csv.DictReader(io_string, delimiter=';')
                
                # Validação de coluna mínima
                if 'CNPJ' not in reader.fieldnames:
                    messages.error(request, 'Erro: O CSV precisa ter a coluna "CNPJ".')
                    return redirect("..")
                
                rows = list(reader)
                total_lines = len(rows)
            except Exception as e:
                messages.error(request, f"Erro ao ler arquivo: {str(e)}")
                return redirect("..")

            def item_processor():
                yield render_to_string('core/progress.html', request=request)
                count = 0
                for row in rows:
                    cnpj = row.get('CNPJ', '').strip()
                    if not cnpj: continue

                    defaults = {
                        'nome': row.get('Nome', '').strip(),
                        'endereco': row.get('Endereço', '').strip(),
                        'cidade': row.get('Cidade', '').strip(),
                        'uf': row.get('UF', '').strip(),
                        'cep': row.get('CEP', '').strip(),
                        'perfil_tributario': row.get('Perfil Tributário', 'Padrao').strip(),
                        'cidades_atendidas': row.get('Cidades Atendidas', '').strip(),
                        'tipos_frete': row.get('Tipos Frete', '').strip(),
                    }

                    Transportadora.objects.update_or_create(cnpj=cnpj, defaults=defaults)
                    count += 1
                    
                    if total_lines > 0:
                        percent = int((count / total_lines) * 100)
                        yield f'<script>updateProgress({count}, {total_lines}, {percent});</script>'

                redirect_url = reverse('admin:core_transportadora_changelist')
                yield f"<script>finishProcess('{redirect_url}', 'Transportadoras atualizadas! {count} registros processados.');</script>"

            return StreamingHttpResponse(item_processor())
        
        form = CsvImportForm()
        return render(request, "core/import_csv.html", {"form": form})